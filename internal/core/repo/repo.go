// internal/core/repo/repo.go
package repo

import (
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"strings"
	"sync"
	"time"

	"crysync/internal/backend"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
)

// moduleWriteLocks 模块级共享写锁（键 = meta DB 路径）：v0.5 单一当前状态
// 模型下 Repo 实例按需创建（rsync 每连接一个、webdav 缓存一个），实例内锁
// 无法跨连接/跨前端互斥——此包级锁保证同一模块的写操作全局串行。
var moduleWriteLocks sync.Map // meta DB path -> *sync.Mutex

// moduleUploadGates 模块级上传并发闸门（键 = meta DB 路径）：跨连接/跨前端
// 共享的信号量，限制同一模块同时进行的 backend.Put 网络写总数。Repo 实例
// 各自持有 gate 引用（SetUploadConcurrency 时 LoadOrStore），0 = 不限制。
var moduleUploadGates sync.Map // meta DB path -> chan struct{}

type Repo struct {
	meta      *meta.DB
	backend   backend.Backend
	key       *crypto.Key
	chunkSize int
	// uploadGate 模块上传并发闸门（nil = 不限制）。跨 Repo 实例共享同一
	// 通道——限制后端 API 压力需统计全部连接的上传总数，实例内信号量不够。
	uploadGate chan struct{}
	// inflight：StoreChunk 并发上传去重登记表（hash -> 进行中上传）。
	// 防同内容并发双写后端 blob（夸克 WebDAV 单次 PUT 固定开销 ~3s，双写
	// 浪费整段窗口；SQLite 访问在驱动层串行，in-flight 防的是 backend.Put
	// 网络 IO 的重叠）。
	inflightMu sync.Mutex
	inflight   map[[32]byte]*chunkInflight
}

// chunkInflight 一次进行中的块上传：完成后 close(done) 广播，等待者复用结果。
type chunkInflight struct {
	done chan struct{}
	id   int64
	err  error
}

func New(metaDB *meta.DB, be backend.Backend, key *crypto.Key, chunkSize int) *Repo {
	return &Repo{meta: metaDB, backend: be, key: key, chunkSize: chunkSize,
		inflight: make(map[[32]byte]*chunkInflight)}
}

// lockWrite 获取模块写锁（短持：WebDAV 单请求的行更新段）。
// 返回解锁函数。同模块并发写/写会话在此排队。
func (r *Repo) lockWrite() func() {
	mu, _ := moduleWriteLocks.LoadOrStore(r.meta.DBPath(), &sync.Mutex{})
	m := mu.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

// WriteSessionLock 写会话全程互斥（types.Session 接口）：rsync 备份会话
// 开始时获取、结束释放。同模块并发写会话与 WebDAV 写（lockWrite 短持）在
// 会话期间排队等待——替代 v0.4 快照事务的并发隔离（绿联单客户端场景无感）。
// 契约：会话内直接调用 meta 层写方法（不经 PutFile 等再次加锁），避免死锁。
func (r *Repo) WriteSessionLock() func() { return r.lockWrite() }

// SetUploadConcurrency 设置模块 blob 上传并发上限（0/负值 = 不限制）。
// 闸门按 meta DB 路径跨 Repo 实例共享（首个设置者定容量，daemon 无热重载
// 故不处理容量变更）。OpenModule 对每个新打开的仓库调用。
func (r *Repo) SetUploadConcurrency(limit int) {
	if limit <= 0 {
		r.uploadGate = nil
		return
	}
	v, _ := moduleUploadGates.LoadOrStore(r.meta.DBPath(), make(chan struct{}, limit))
	r.uploadGate = v.(chan struct{})
}

// ChunkSizeBytes 返回分块大小（字节）。receiver 用它确定内容缓冲/分块边界。
func (r *Repo) ChunkSizeBytes() int {
	return r.chunkSize
}

// StoreChunk 将明文块去重存储：哈希命中复用已有 chunk；并发同内容上传时
// 在 in-flight 登记表上等待复用（防双写后端）。可并发调用（PutFile worker
// 池与并发 PUT 请求各自调用；SQLite 驱动层串行，backend.Put 网络 IO 并行）。
func (r *Repo) StoreChunk(data []byte) (chunkID int64, reused bool, err error) {
	h := sha256.Sum256(data)
	if info, exists, err := r.meta.FindChunkByHash(h); err != nil {
		return 0, false, err
	} else if exists {
		return info.ID, true, nil
	}
	// in-flight 登记：同内容上传进行中则等待其完成并复用结果
	r.inflightMu.Lock()
	w, dup := r.inflight[h]
	if !dup {
		w = &chunkInflight{done: make(chan struct{})}
		r.inflight[h] = w
	}
	r.inflightMu.Unlock()
	if dup {
		<-w.done
		return w.id, true, w.err
	}
	// 上传者：收尾先删登记再发信号——删除后新到的同内容调用走 FindChunkByHash
	// 命中已入库行；已在 done 上等待的按信号返回（结果与错误一并携带）
	defer func() {
		r.inflightMu.Lock()
		delete(r.inflight, h)
		r.inflightMu.Unlock()
		w.id, w.err = chunkID, err
		close(w.done)
	}()

	blobName, err := crypto.RandomBlobName()
	if err != nil {
		return 0, false, err
	}
	blob, err := r.key.Encrypt(data, blobName)
	if err != nil {
		return 0, false, err
	}
	if err := r.putBlob(blobName, blob); err != nil {
		return 0, false, fmt.Errorf("写入后端: %w", err)
	}
	id, err := r.meta.InsertChunk(h, blobName, int64(len(data)))
	if err != nil {
		// 跨进程/连接并发写同一内容：对方已插入，删掉自己刚写的重复 blob
		// 后回退为复用（不删则残留一个永远不被引用的孤儿 blob）
		if info, exists, e2 := r.meta.FindChunkByHash(h); e2 == nil && exists {
			_ = r.backend.Delete(blobName)
			return info.ID, true, nil
		}
		return 0, false, err
	}
	return id, false, nil
}

// putBlob 写 blob 到后端：uploadGate 非 nil 时先获取并发槽位（超出上限排队），
// 完成即释放。只闸门网络写段——加密/SQLite 不占槽位。
func (r *Repo) putBlob(name string, blob []byte) error {
	if g := r.uploadGate; g != nil {
		g <- struct{}{}
		defer func() { <-g }()
	}
	return r.backend.Put(name, blob)
}

// UpsertFile 原地落库文件行并附加块关联（替换+refcount 维护+关联全在
// meta.UpsertFile 单事务内原子完成）。调用方必须处于写互斥内
// （WriteSessionLock 持有中，或 lockWrite 短持锁内）——单一状态模型下不存在
// 快照隔离，行更新必须与同模块其他写串行。
func (r *Repo) UpsertFile(f meta.FileRow, chunks []meta.ChunkRef) error {
	_, err := r.meta.UpsertFile(f, chunks)
	return err
}

// DeleteFile 原地删除路径行（refcount 递减）。锁契约同 UpsertFile。
func (r *Repo) DeleteFile(path string) error {
	return r.meta.DeleteFile(path)
}

// FileRows 返回文件清单，按前缀路径过滤：prefix 为空返回全部；否则只含
// path == prefix（单文件/目录自身）或以 prefix+"/" 开头的条目。按 path 排序。
func (r *Repo) FileRows(prefix string) ([]meta.FileRow, error) {
	files, err := r.meta.GetFiles()
	if err != nil {
		return nil, err
	}
	if prefix == "" {
		return files, nil
	}
	out := files[:0]
	for _, f := range files {
		if f.Path == prefix || strings.HasPrefix(f.Path, prefix+"/") {
			out = append(out, f)
		}
	}
	return out, nil
}

// ListDir 返回 path 的直接子项（非递归，不含自身）。path 为空 = 模块根。
// 目录不存在时返回空切片（调用方先用 GetFileRow 判定存在性）。
func (r *Repo) ListDir(path string) ([]meta.FileRow, error) {
	files, err := r.meta.GetFiles()
	if err != nil {
		return nil, err
	}
	prefix := path
	if prefix != "" {
		prefix += "/"
	}
	out := make([]meta.FileRow, 0)
	for _, f := range files {
		if !strings.HasPrefix(f.Path, prefix) {
			continue
		}
		rest := f.Path[len(prefix):]
		if rest == "" || strings.Contains(rest, "/") {
			continue
		}
		out = append(out, f)
	}
	return out, nil
}

// GetFileRow 按路径查文件元数据（quick check 判定用）。
func (r *Repo) GetFileRow(path string) (meta.FileRow, bool, error) {
	return r.meta.GetFileRow(path)
}

// ReadChunkAt 读取文件第 idx 个存储块（file_chunks.idx）的明文。
// 短连接查询：Scan 后即释放连接，供 basisReader 逐块顺序读取（不长期占用
// meta 单写连接——StreamFile+io.Pipe 方案会因连接互锁而死锁，
// meta.DB SetMaxOpenConns(1) 下唯一连接被流式读占用时 StoreChunk 等不到连接）。
func (r *Repo) ReadChunkAt(path string, idx int) ([]byte, error) {
	var blobName string
	var size int64
	err := r.meta.QueryRow(`
		SELECT c.blob_name, c.size FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id
		JOIN chunks c ON c.id = fc.chunk_id
		WHERE f.path = ? AND fc.idx = ?`,
		path, idx).Scan(&blobName, &size)
	if err != nil {
		return nil, err
	}
	raw, err := r.backend.Get(blobName)
	if err != nil {
		return nil, fmt.Errorf("读取块 %s: %w", blobName, err)
	}
	return r.key.Decrypt(raw, blobName)
}

// StreamFile 流式读取文件内容：逐块解密后写入 w，同时计算 rsync 整文件
// 强校验和 MD5(content)（checksum.c sum_end：CSUM_MD5 分支为纯 MD5——sum_init
// 的 seed 参数仅作用于 xxh 系列，对 MD5 无效；带 seed 混入的是 file_checksum，
// --checksum 模式，v1 不涉及）。返回写入字节数与校验和。
func (r *Repo) StreamFile(path string, _ int32, w io.Writer) (int64, [16]byte, error) {
	// 先确认文件存在：空文件没有 file_chunks 行，必须靠 files 行判定
	var fileSize int64
	err := r.meta.QueryRow(`SELECT size FROM files WHERE path = ?`, path).Scan(&fileSize)
	if err == sql.ErrNoRows {
		return 0, [16]byte{}, fmt.Errorf("仓库中不存在文件 %s", path)
	}
	if err != nil {
		return 0, [16]byte{}, err
	}
	rows, err := r.meta.Query(`
		SELECT c.blob_name, c.size FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id
		JOIN chunks c ON c.id = fc.chunk_id
		WHERE f.path = ?
		ORDER BY fc.idx`, path)
	if err != nil {
		return 0, [16]byte{}, err
	}
	defer rows.Close()
	h := md5.New()
	var n int64
	for rows.Next() {
		var blobName string
		var size int64
		if err := rows.Scan(&blobName, &size); err != nil {
			return 0, [16]byte{}, err
		}
		raw, err := r.backend.Get(blobName)
		if err != nil {
			return 0, [16]byte{}, fmt.Errorf("读取块 %s: %w", blobName, err)
		}
		pt, err := r.key.Decrypt(raw, blobName)
		if err != nil {
			return 0, [16]byte{}, err
		}
		if _, err := w.Write(pt); err != nil {
			return 0, [16]byte{}, err
		}
		h.Write(pt)
		n += int64(len(pt))
	}
	if err := rows.Err(); err != nil {
		return 0, [16]byte{}, err
	}
	if n != fileSize {
		return 0, [16]byte{}, fmt.Errorf("文件 %s 内容不完整: %d/%d 字节",
			path, n, fileSize)
	}
	var sum [16]byte
	copy(sum[:], h.Sum(nil))
	return n, sum, nil
}

// ReadFile 按 file_chunks 顺序读出并解密文件内容，写入 w。
func (r *Repo) ReadFile(path string, w io.Writer) error {
	_, _, err := r.StreamFile(path, 0, w)
	return err
}

// GetFilesForTest 临时公开包装，供测试断言。
func (r *Repo) GetFilesForTest() ([]meta.FileRow, error) {
	return r.meta.GetFiles()
}

// fileChunk 文件块索引（OpenFile 时一次查全）。
type fileChunk struct {
	blobName string
	size     int64
}

// FileReader 按块流式重组文件（解密后逐块输出），支持 Seek 定位到
// 任意块重放（HTTP Range 下载用）。块定位 O(块数)，块内读取 O(1)。
type FileReader struct {
	r        *Repo
	row      meta.FileRow
	chunks   []fileChunk
	pos      int64 // 文件内读取偏移
	buf      []byte
	bufChunk int // 当前块下标；-1 = 未加载
	bufOff   int
	closed   bool
}

// OpenFile 按路径打开文件：返回流式块重组 reader（Read/Seek/Close）与
// 文件元数据。仅普通文件可打开；目录/符号链接由调用方先行判定。
func (r *Repo) OpenFile(path string) (io.ReadSeekCloser, meta.FileRow, error) {
	row, ok, err := r.meta.GetFileRow(path)
	if err != nil {
		return nil, meta.FileRow{}, err
	}
	if !ok {
		return nil, meta.FileRow{}, os.ErrNotExist
	}
	if row.IsDir {
		return nil, row, fmt.Errorf("%s 是目录", path)
	}
	rows, err := r.meta.Query(`SELECT c.blob_name, c.size FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id
		JOIN chunks c ON c.id = fc.chunk_id
		WHERE f.path = ? ORDER BY fc.idx`, path)
	if err != nil {
		return nil, row, err
	}
	defer rows.Close()
	chunks := make([]fileChunk, 0, 8)
	for rows.Next() {
		var c fileChunk
		if err := rows.Scan(&c.blobName, &c.size); err != nil {
			return nil, row, err
		}
		chunks = append(chunks, c)
	}
	if err := rows.Err(); err != nil {
		return nil, row, err
	}
	return &FileReader{r: r, row: row, chunks: chunks, bufChunk: -1}, row, nil
}

func (f *FileReader) loadChunk(ci int) error {
	raw, err := f.r.backend.Get(f.chunks[ci].blobName)
	if err != nil {
		return fmt.Errorf("读取块 %s: %w", f.chunks[ci].blobName, err)
	}
	pt, err := f.r.key.Decrypt(raw, f.chunks[ci].blobName)
	if err != nil {
		return err
	}
	f.buf, f.bufChunk, f.bufOff = pt, ci, 0
	return nil
}

func (f *FileReader) Read(p []byte) (int, error) {
	if f.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	for f.bufOff >= len(f.buf) { // 当前块耗尽 → 下一块
		next := f.bufChunk + 1
		if next >= len(f.chunks) {
			return 0, io.EOF
		}
		if err := f.loadChunk(next); err != nil {
			return 0, err
		}
	}
	n := copy(p, f.buf[f.bufOff:])
	f.bufOff += n
	f.pos += int64(n)
	return n, nil
}

func (f *FileReader) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, io.ErrClosedPipe
	}
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = f.pos + offset
	case io.SeekEnd:
		target = f.row.Size + offset
	default:
		return f.pos, fmt.Errorf("非法 whence %d", whence)
	}
	if target < 0 {
		return f.pos, fmt.Errorf("偏移越界: %d", target)
	}
	if target > f.row.Size {
		target = f.row.Size
	}
	// 定位目标块（累计块大小，找到第一个包含 target 的块）
	var acc int64
	ci := 0
	for ci < len(f.chunks) && acc+int64(f.chunks[ci].size) <= target {
		acc += int64(f.chunks[ci].size)
		ci++
	}
	if ci >= len(f.chunks) { // 定位到 EOF（块边界）
		f.buf, f.bufChunk, f.bufOff = nil, len(f.chunks), 0
		f.pos = target
		return f.pos, nil
	}
	if err := f.loadChunk(ci); err != nil {
		return f.pos, err
	}
	f.bufOff = int(target - acc)
	f.pos = target
	return f.pos, nil
}

func (f *FileReader) Close() error {
	f.closed = true
	return nil
}

// GC 回收孤儿 blob。v0.5 单一状态模型下引用判定必须以 file_chunks JOIN files
// 的真引用为唯一真值（不能以 chunks 表为准——失败会话残留的 refcount=0 chunk
// 行会让其 blob 永不回收）：
//  1. chunks 行未被任何文件引用的（StoreChunk 后未 attach 的失败残留）→ 删行+blob；
//  2. 后端存在但无对应 chunk 行的 blob（行删除后的直接残留）→ 删 blob。
//
// 返回删除的 blob 数量。
func (r *Repo) GC() (int, error) {
	blobs, err := r.backend.List()
	if err != nil {
		return 0, fmt.Errorf("列出后端: %w", err)
	}
	if len(blobs) == 0 {
		return 0, nil
	}
	// 真引用集合：当前清单中文件实际引用的 chunk id
	rows, err := r.meta.Query(`SELECT DISTINCT fc.chunk_id FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id`)
	if err != nil {
		return 0, err
	}
	referenced := make(map[int64]bool, len(blobs))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		referenced[id] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	// chunks 表全量：未引用行（残留）连同 blob 一并回收；chunkBlob 供后端
	// blob 判定（后端 blob 名必须对得上 chunks 行才算有主）
	rows, err = r.meta.Query(`SELECT id, blob_name FROM chunks`)
	if err != nil {
		return 0, err
	}
	chunkBlob := make(map[string]bool, len(blobs))
	garbage := make(map[string]bool) // blob_name -> 待删
	var deadChunks []int64
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			rows.Close()
			return 0, err
		}
		chunkBlob[name] = true
		if !referenced[id] {
			garbage[name] = true
			deadChunks = append(deadChunks, id)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, name := range blobs {
		if !chunkBlob[name] {
			garbage[name] = true
		}
	}
	if len(garbage) == 0 {
		return 0, nil
	}

	// 先删 chunk 行再删 blob（行删失败 blob 仍会被下一次 GC 按"无对应行"回收）
	for _, id := range deadChunks {
		if err := r.meta.DeleteChunk(id); err != nil {
			return 0, fmt.Errorf("删除残留 chunk 行 %d: %w", id, err)
		}
	}
	var deleted int
	var failed []string
	for name := range garbage {
		if err := r.backend.Delete(name); err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		deleted++
	}
	if len(failed) > 0 {
		return deleted, fmt.Errorf("删除孤儿 blob 失败（%d 个）: %s", len(failed), strings.Join(failed, "; "))
	}
	return deleted, nil
}

// checkParentDir 校验 path 的父目录在当前清单中存在（WebDAV PUT 语义：
// 父目录缺失 → 409）。仓库为空时除根外任何父目录都不存在。
// 调用方必须处于写互斥内。
func checkParentDir(r *Repo, path string) error {
	parent := pathpkg.Dir(path)
	if parent == "." || parent == "/" {
		return nil
	}
	row, ok, err := r.meta.GetFileRow(parent)
	if err != nil {
		return err
	}
	if !ok || !row.IsDir {
		return os.ErrNotExist
	}
	return nil
}

// pathExistsLocked 在写互斥内判定 path 是否存在于当前清单（幂等
// MKCOL/DELETE 判定用：目标状态已达成即成功，绿联 restic fork 对 405/404
// 敏感）。调用方必须持写锁。
func (r *Repo) pathExistsLocked(path string) (bool, error) {
	_, ok, err := r.meta.GetFileRow(path)
	return ok, err
}

// cleanupNewChunks 写失败收口：删除本次新建但未附引用的 chunk 行
// （blob 随之脱表成孤儿，由 GC 回收；曾因 chunks 表残留 refcount=0 行
// 而 blob 无法回收）。
func (r *Repo) cleanupNewChunks(newChunks []int64) {
	for _, id := range newChunks {
		_ = r.meta.DeleteChunkIfZero(id)
	}
}

// PutFile 写入（或覆盖）path 文件内容。
// v0.5 两阶段语义：阶段 1 无锁并发分块去重上传——4 worker 有界池并行
// StoreChunk（blob 写后端为网络 IO，内容寻址并发安全，StoreChunk 内部
// in-flight 去重防同内容双写）；chunkSize 32MiB 下单 chunk 自然退化为顺序
// 单飞。并发 PUT 请求（绿联 restic fork 4 并发）间阶段 1 互不阻塞。阶段 2
// 短锁内行更新（父目录校验 + 原地 upsert）。失败 → 文件保留旧状态、本次
// 新建 chunk 行清理、blob 成孤儿（GC 回收）。
func (r *Repo) PutFile(path string, mode uint32, mtimeNs int64, src io.Reader) error {
	const uploadWorkers = 4
	type chunkJob struct {
		idx  int
		data []byte
	}
	type chunkRes struct {
		idx    int
		n      int64
		id     int64
		reused bool
		err    error
	}
	jobCh := make(chan chunkJob, uploadWorkers)
	resCh := make(chan chunkRes, uploadWorkers)
	errCh := make(chan error, 1)

	// producer：顺序读流切块送任务（channel 满即背压，源读暂停；每块独立
	// 缓冲——job 发出后 producer 继续读，worker 消费期间缓冲不可复用）
	go func() {
		defer close(jobCh)
		defer close(errCh)
		idx := 0
		for {
			data := make([]byte, r.chunkSize)
			n, err := io.ReadFull(src, data)
			if n > 0 {
				jobCh <- chunkJob{idx: idx, data: data[:n]}
				idx++
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return
			}
			if err != nil {
				errCh <- err // 源断流：停止切块（已发的块由 worker 收完）
				return
			}
		}
	}()
	// 4 worker：各自 StoreChunk（backend.Put 网络 IO 并行）
	var wg sync.WaitGroup
	for w := 0; w < uploadWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobCh {
				id, reused, err := r.StoreChunk(j.data)
				resCh <- chunkRes{idx: j.idx, n: int64(len(j.data)), id: id, reused: reused, err: err}
			}
		}()
	}
	go func() { wg.Wait(); close(resCh) }()

	// collector：归位乱序结果（idx 槽位），累计总量与本次新建 chunk
	refsByIdx := make(map[int]meta.ChunkRef)
	var total int64
	var newChunks []int64 // 本次新建且未复用的 chunk（出错时回收）
	var firstErr error
	for res := range resCh {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		refsByIdx[res.idx] = meta.ChunkRef{ChunkID: res.id, IDX: res.idx}
		total += res.n
		if !res.reused {
			newChunks = append(newChunks, res.id)
		}
	}
	if perr, ok := <-errCh; ok && firstErr == nil {
		firstErr = perr
	}
	if firstErr != nil {
		r.cleanupNewChunks(newChunks)
		return firstErr
	}
	// 槽位归位：idx 连续（producer 顺序编号），按序转有序 refs（块顺序 =
	// 文件内容顺序，重组依赖）
	chunks := make([]meta.ChunkRef, len(refsByIdx))
	for idx, ref := range refsByIdx {
		chunks[idx] = ref
	}
	unlock := r.lockWrite()
	defer unlock()
	if err := checkParentDir(r, path); err != nil {
		r.cleanupNewChunks(newChunks)
		return err
	}
	row := meta.FileRow{Path: path, Mode: mode, Size: total, MTimeNs: mtimeNs}
	if err := r.UpsertFile(row, chunks); err != nil {
		r.cleanupNewChunks(newChunks)
		return err
	}
	return nil
}

// Mkcol 创建目录条目；已存在时幂等返回 nil（目录已存在即目标状态已达
// 成——绿联 NAS 定制 restic fork 把 MKCOL 405 当致命错误；官方 restic
// webdav 后端同样把 405 视为已存在继续。读历史：v0.4.2 幂等化）。
func (r *Repo) Mkcol(path string) error {
	unlock := r.lockWrite()
	defer unlock()
	if err := checkParentDir(r, path); err != nil {
		return err
	}
	if ok, err := r.pathExistsLocked(path); err != nil {
		return err
	} else if ok {
		return nil
	}
	_, err := r.meta.UpsertFile(meta.FileRow{Path: path, IsDir: true, Mode: 0o40755, MTimeNs: time.Now().UnixNano()}, nil)
	return err
}

// DeletePath 删除 path（文件或目录，目录递归删整棵子树）；不存在时幂等
// 返回 nil（绿联 restic fork 对 DELETE 404 敏感，v0.4.2 幂等化）。
func (r *Repo) DeletePath(path string) error {
	unlock := r.lockWrite()
	defer unlock()
	if ok, err := r.pathExistsLocked(path); err != nil {
		return err
	} else if !ok {
		return nil
	}
	rows, err := r.FileRows(path)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil // 防御性兜底：前置检查后正常不会到达
	}
	for _, row := range rows {
		if err := r.meta.DeleteFile(row.Path); err != nil {
			return err
		}
	}
	return nil
}

// MovePath 移动/改名：目标新建条目（chunk 引用直接复制，净 refcount 不变、
// 不产生新 blob），源删除。目录移动递归整棵子树，路径前缀整体替换。
// 目标已存在时覆盖（WebDAV Overwrite 语义）：先删除 dst 原子树（与 src
// 重叠部分除外）再搬入 src，保证 dst 不残留旧内容。
func (r *Repo) MovePath(src, dst string) error {
	unlock := r.lockWrite()
	defer unlock()
	if err := checkParentDir(r, dst); err != nil {
		return err
	}
	rows, err := r.FileRows(src)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return os.ErrNotExist
	}
	dstRows, err := r.FileRows(dst)
	if err != nil {
		return err
	}
	// src 与 dst 可能同子树（原地改名/前缀替换），删除 dst 时必须排除与
	// src 重叠的行，否则会把待移动的 src 行也删掉。
	srcSet := make(map[string]bool, len(rows))
	for _, row := range rows {
		srcSet[row.Path] = true
	}
	for _, drow := range dstRows {
		if srcSet[drow.Path] {
			continue // 与 src 重叠的行（src 自身或其子树内容已由 upsert 处理）
		}
		if err := r.meta.DeleteFile(drow.Path); err != nil {
			return err
		}
	}
	for _, row := range rows {
		var newPath string
		if row.Path == src {
			newPath = dst
		} else {
			newPath = dst + row.Path[len(src):]
		}
		chunks, err := r.meta.GetFileChunks(row.Path)
		if err != nil {
			return err
		}
		newRow := row
		newRow.Path = newPath
		if err := r.UpsertFile(newRow, chunks); err != nil {
			return err
		}
		if err := r.meta.DeleteFile(row.Path); err != nil {
			return err
		}
	}
	return nil
}
