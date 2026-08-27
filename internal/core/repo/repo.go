// internal/core/repo/repo.go
package repo

import (
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"crysync/internal/backend"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
	"crysync/internal/core/prune"
	"crysync/internal/core/types"
)

type Repo struct {
	meta      *meta.DB
	backend   backend.Backend
	key       *crypto.Key
	chunkSize int
}

func New(metaDB *meta.DB, be backend.Backend, key *crypto.Key, chunkSize int) *Repo {
	return &Repo{meta: metaDB, backend: be, key: key, chunkSize: chunkSize}
}

// ChunkSizeBytes 返回分块大小（字节）。receiver 用它确定内容缓冲/分块边界。
func (r *Repo) ChunkSizeBytes() int {
	return r.chunkSize
}

// StoreChunk 将明文块去重存储：哈希命中则复用已有 chunk，否则加密写入后端。
func (r *Repo) StoreChunk(data []byte) (chunkID int64, reused bool, err error) {
	h := sha256.Sum256(data)
	if info, exists, err := r.meta.FindChunkByHash(h); err != nil {
		return 0, false, err
	} else if exists {
		return info.ID, true, nil
	}
	blobName, err := crypto.RandomBlobName()
	if err != nil {
		return 0, false, err
	}
	blob, err := r.key.Encrypt(data, blobName)
	if err != nil {
		return 0, false, err
	}
	if err := r.backend.Put(blobName, blob); err != nil {
		return 0, false, fmt.Errorf("写入后端: %w", err)
	}
	id, err := r.meta.InsertChunk(h, blobName, int64(len(data)))
	if err != nil {
		// 并发写同一内容：另一个连接先插入，回退为复用
		if info, exists, e2 := r.meta.FindChunkByHash(h); e2 == nil && exists {
			return info.ID, true, nil
		}
		return 0, false, err
	}
	return id, false, nil
}

type SnapshotTxn struct {
	repo       *Repo
	snapshotID int64
	finalized  bool
}

// SnapshotID 返回本次事务创建的快照 ID（复制清单后即可用）。
func (t *SnapshotTxn) SnapshotID() int64 { return t.snapshotID }

// BeginSnapshot 创建新快照并复制上一快照的完整文件清单。
// 返回面向前端的 SessionTxn 接口（rsyncproto 等调用方只依赖接口方法）。
func (r *Repo) BeginSnapshot(now time.Time) (types.SessionTxn, error) {
	prev, err := r.LatestSnapshotID()
	if err != nil {
		return nil, err
	}
	id, err := r.meta.CreateSnapshot(now)
	if err != nil {
		return nil, err
	}
	if prev != 0 {
		if err := r.meta.CopyFiles(prev, id); err != nil {
			return nil, err
		}
	}
	return &SnapshotTxn{repo: r, snapshotID: id}, nil
}

func (t *SnapshotTxn) UpsertFile(f meta.FileRow, chunks []meta.ChunkRef) error {
	if t.finalized {
		return errors.New("快照事务已结束")
	}
	fid, err := t.repo.meta.UpsertFile(t.snapshotID, f)
	if err != nil {
		return err
	}
	if len(chunks) > 0 {
		if err := t.repo.meta.AttachChunks(fid, chunks); err != nil {
			return err
		}
		// refcount 递增由引用方负责（AttachChunks 只建关联，见接口契约）
		for _, c := range chunks {
			if err := t.repo.meta.IncrRefcount(c.ChunkID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *SnapshotTxn) DeleteFile(path string) error {
	if t.finalized {
		return errors.New("快照事务已结束")
	}
	return t.repo.meta.DeleteFile(t.snapshotID, path)
}

// ApplyDelete 执行 rsync --delete 语义：删除事务快照中 prefix 子树内、不在
// covered 路径集合中的行（文件与目录，refcount 级联由 DeleteFile 处理），
// 返回删除行数。covered 键为落库形态路径（目录无尾斜杠）；范围按 prefix
// 子树界定（SnapshotFileRows 语义），不殃及模块内其他子路径。调用方须先
// 确认发送侧无 io_error（rsync 语义：io_error≠0 时禁删，防源清单不完整误删）。
func (t *SnapshotTxn) ApplyDelete(prefix string, covered map[string]bool) (int, error) {
	if t.finalized {
		return 0, errors.New("快照事务已结束")
	}
	rows, err := t.repo.SnapshotFileRows(t.snapshotID, prefix)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, row := range rows {
		if covered[row.Path] {
			continue
		}
		if err := t.DeleteFile(row.Path); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func (t *SnapshotTxn) Commit() (int64, error) {
	if t.finalized {
		return 0, errors.New("快照事务已结束")
	}
	t.finalized = true
	return t.snapshotID, nil
}

func (t *SnapshotTxn) Rollback() error {
	if t.finalized {
		return errors.New("快照事务已结束")
	}
	t.finalized = true
	_, err := t.repo.meta.DeleteSnapshot(t.snapshotID)
	return err
}

func (r *Repo) LatestSnapshotID() (int64, error) {
	var id int64
	err := r.meta.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM snapshots`).Scan(&id)
	return id, err
}

// ActiveSnapshotID 返回活跃快照：meta 表 active_snapshot 键（CLI 切换时间点）
// 存在且有效时优先，否则默认最新快照。
func (r *Repo) ActiveSnapshotID() (int64, error) {
	v, ok, err := r.meta.GetMeta("active_snapshot")
	if err != nil {
		return 0, err
	}
	if ok {
		var id int64
		if _, err := fmt.Sscanf(v, "%d", &id); err == nil && id > 0 {
			var n int
			if err := r.meta.QueryRow(`SELECT COUNT(*) FROM snapshots WHERE id = ?`, id).Scan(&n); err != nil {
				return 0, err
			}
			if n > 0 {
				return id, nil
			}
		}
	}
	return r.LatestSnapshotID()
}

// SetActiveSnapshot 手动切换活跃快照（快照恢复时间点，存 meta 表）。
func (r *Repo) SetActiveSnapshot(id int64) error {
	return r.meta.SetMeta("active_snapshot", fmt.Sprintf("%d", id))
}

// SnapshotFileRows 返回快照文件清单，按子路径过滤：prefix 为空（拉取模块根）返回全部；
// 否则只含 path == prefix（单文件/目录自身）或以 prefix+"/" 开头的条目。
// 结果按 path 排序（发送侧再按 f_name_cmp 重排）。
func (r *Repo) SnapshotFileRows(snapshotID int64, prefix string) ([]meta.FileRow, error) {
	files, err := r.meta.GetFiles(snapshotID)
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

// GetFileRow 按快照路径查文件元数据（quick check 判定用）：旧快照存在同路径
// 普通文件且 (mtime, size) 一致 → 未变化，无需传输。
func (r *Repo) GetFileRow(snapshotID int64, path string) (meta.FileRow, bool, error) {
	return r.meta.GetFileRow(snapshotID, path)
}

// ReadChunkAt 读取快照文件第 idx 个存储块（file_chunks.idx）的明文。
// 短连接查询：Scan 后即释放连接，供 basisReader 逐块顺序读取（不长期占用
// meta 单写连接——StreamFile+io.Pipe 方案会因连接互锁而死锁，
// meta.DB SetMaxOpenConns(1) 下唯一连接被流式读占用时 StoreChunk 等不到连接）。
func (r *Repo) ReadChunkAt(snapshotID int64, path string, idx int) ([]byte, error) {
	var blobName string
	var size int64
	err := r.meta.QueryRow(`
		SELECT c.blob_name, c.size FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id
		JOIN chunks c ON c.id = fc.chunk_id
		WHERE f.snapshot_id = ? AND f.path = ? AND fc.idx = ?`,
		snapshotID, path, idx).Scan(&blobName, &size)
	if err != nil {
		return nil, err
	}
	raw, err := r.backend.Get(blobName)
	if err != nil {
		return nil, fmt.Errorf("读取块 %s: %w", blobName, err)
	}
	return r.key.Decrypt(raw, blobName)
}

// StreamFile 流式读取快照中文件内容：逐块解密后写入 w，同时计算 rsync 整文件
// 强校验和 MD5(content)（checksum.c sum_end：CSUM_MD5 分支为纯 MD5——sum_init
// 的 seed 参数仅作用于 xxh 系列，对 MD5 无效；带 seed 混入的是 file_checksum，
// --checksum 模式，v1 不涉及）。返回写入字节数与校验和。
func (r *Repo) StreamFile(snapshotID int64, path string, _ int32, w io.Writer) (int64, [16]byte, error) {
	// 先确认文件存在：空文件没有 file_chunks 行，必须靠 files 行判定
	var fileSize int64
	err := r.meta.QueryRow(`SELECT size FROM files WHERE snapshot_id = ? AND path = ?`,
		snapshotID, path).Scan(&fileSize)
	if err == sql.ErrNoRows {
		return 0, [16]byte{}, fmt.Errorf("快照 %d 中不存在文件 %s", snapshotID, path)
	}
	if err != nil {
		return 0, [16]byte{}, err
	}
	rows, err := r.meta.Query(`
		SELECT c.blob_name, c.size FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id
		JOIN chunks c ON c.id = fc.chunk_id
		WHERE f.snapshot_id = ? AND f.path = ?
		ORDER BY fc.idx`, snapshotID, path)
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
		return 0, [16]byte{}, fmt.Errorf("快照 %d 中文件 %s 内容不完整: %d/%d 字节",
			snapshotID, path, n, fileSize)
	}
	var sum [16]byte
	copy(sum[:], h.Sum(nil))
	return n, sum, nil
}

// ReadFile 按 file_chunks 顺序读出并解密文件内容，写入 w。
func (r *Repo) ReadFile(snapshotID int64, path string, w io.Writer) error {
	_, _, err := r.StreamFile(snapshotID, path, 0, w)
	return err
}

// SnapshotList 返回快照列表（id + 创建时间，供 CLI 展示与 prune）。
func (r *Repo) SnapshotList() ([]meta.SnapshotInfo, error) {
	return r.meta.SnapshotList()
}

// SnapshotFileCount 返回快照文件条目数。
func (r *Repo) SnapshotFileCount(snapshotID int64) (int, error) {
	return r.meta.SnapshotFileCount(snapshotID)
}

// GetFilesForTest 临时公开包装，供测试断言。
func (r *Repo) GetFilesForTest(snapshotID int64) ([]meta.FileRow, error) {
	return r.meta.GetFiles(snapshotID)
}

// SnapshotCountForTest 临时公开包装，供测试断言（单份模式恒 1）。
func (r *Repo) SnapshotCountForTest() (int, error) {
	infos, err := r.meta.SnapshotList()
	return len(infos), err
}

// Prune 按保留策略执行清理：删除被裁掉的快照（DeleteSnapshot 递减引用，
// 归零的 chunk 行删除）-> 孤儿 blob 回收（GC）。返回删除的快照数与回收的 blob 数。
func (r *Repo) Prune(p prune.Policy) (int, int, error) {
	infos, err := r.meta.SnapshotList()
	if err != nil {
		return 0, 0, err
	}
	snaps := make([]prune.Snapshot, 0, len(infos))
	for _, s := range infos {
		snaps = append(snaps, prune.Snapshot{ID: s.ID, CreatedAt: s.CreatedAt})
	}
	_, remove := p.Apply(snaps)
	for _, s := range remove {
		if _, err := r.meta.DeleteSnapshot(s.ID); err != nil {
			return 0, 0, fmt.Errorf("删除快照 %d: %w", s.ID, err)
		}
	}
	blobs, err := r.GC()
	if err != nil {
		return len(remove), 0, err
	}
	return len(remove), blobs, nil
}

// KeepOnlySnapshot 单份模式（模块配置 snapshot: false）会话收尾：删除
// keepID 之前的全部快照，使仓库收敛为"只保留一份"。返回删除的快照数与
// 回收的 blob 数。
//
// 语义要点：
//   - 只删 ID 更小的快照（更早创建）。并发会话已提交的更新快照不殃及——
//     同模块并发单份备份收敛为最后提交者；被裁掉在途快照的会话会在后续
//     落库时报错（客户端重试即可）。要彻底规避可用 max_connections: 1。
//   - 本次删除未产生孤儿 chunk（deletedChunks=0，如纯 quick check 的无变化
//     会话）时跳过 GC，避免每次会话全量扫描后端；失败/中断会话的陈年孤儿
//     由下一次确有删除的会话或 prune 调度兜底回收。
func (r *Repo) KeepOnlySnapshot(keepID int64) (int, int, error) {
	infos, err := r.meta.SnapshotList()
	if err != nil {
		return 0, 0, err
	}
	var removed, deletedChunks int
	for _, s := range infos {
		if s.ID >= keepID {
			continue
		}
		n, err := r.meta.DeleteSnapshot(s.ID)
		if err != nil {
			return removed, 0, fmt.Errorf("删除快照 %d: %w", s.ID, err)
		}
		removed++
		deletedChunks += n
	}
	if deletedChunks == 0 {
		return removed, 0, nil
	}
	blobs, err := r.GC()
	if err != nil {
		return removed, 0, err
	}
	return removed, blobs, nil
}

// GC 回收孤儿 blob：后端存在但未被任何 chunk 引用的 blob（失败/中断会话的
// 残留，设计文档 §4.2：blob 先写后端、快照提交失败即孤儿）。引用判定以
// chunks 表为准——chunk 一旦入库即保留（refcount 归零与否由 prune 策略决定，
// 本函数只清"从未入库"的）。返回删除数量。
func (r *Repo) GC() (int, error) {
	blobs, err := r.backend.List()
	if err != nil {
		return 0, fmt.Errorf("列出后端: %w", err)
	}
	if len(blobs) == 0 {
		return 0, nil
	}
	rows, err := r.meta.Query(`SELECT blob_name FROM chunks`)
	if err != nil {
		return 0, err
	}
	referenced := make(map[string]bool, len(blobs))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return 0, err
		}
		referenced[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	var deleted int
	var failed []string
	for _, name := range blobs {
		if referenced[name] {
			continue
		}
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
