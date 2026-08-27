// internal/core/repo/repo_test.go
package repo

import (
	"bytes"
	"crypto/md5"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crysync/internal/backend"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
	"crysync/internal/core/prune"
)

func newTestRepo(t *testing.T) (*Repo, *backend.InMemory) {
	t.Helper()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	key, _ := crypto.GenerateKey()
	be := backend.NewInMemory()
	return New(db, be, key, 64), be
}

// TestListDir：快照中 path 的直接子项（非递归，不含自身）；path 为空 = 模块根。
func TestListDir(t *testing.T) {
	r, _ := newTestRepo(t)
	txn, _ := r.BeginSnapshot(time.Now())
	txn.UpsertFile(meta.FileRow{Path: "sub", IsDir: true, Mode: 0o40755}, nil)
	txn.UpsertFile(meta.FileRow{Path: "sub/a.txt", Mode: 0o644, Size: 1}, nil)
	txn.UpsertFile(meta.FileRow{Path: "sub/deep/b.txt", Mode: 0o644, Size: 2}, nil)
	txn.UpsertFile(meta.FileRow{Path: "sub/deep", IsDir: true, Mode: 0o40755}, nil)
	txn.UpsertFile(meta.FileRow{Path: "root.txt", Mode: 0o644, Size: 3}, nil)
	sid, _ := txn.Commit()

	// 模块根：直接子项（不含子孙）
	rows, err := r.ListDir(sid, "")
	if err != nil || len(rows) != 2 {
		t.Fatalf("根子项应为 sub/root.txt, got %+v, %v", rows, err)
	}
	// 目录 sub：直接子项
	rows, err = r.ListDir(sid, "sub")
	if err != nil || len(rows) != 2 {
		t.Fatalf("sub 子项应为 a.txt/deep, got %+v, %v", rows, err)
	}
	// 文件路径：返回空
	rows, err = r.ListDir(sid, "root.txt")
	if err != nil || len(rows) != 0 {
		t.Fatalf("文件路径子项应为空, got %+v, %v", rows, err)
	}
}

// TestOpenFileSeek：块流式读取 + Seek 定位块重放（HTTP Range 下载语义）。
func TestOpenFileSeek(t *testing.T) {
	r, _ := newTestRepo(t)
	// 3.5 块内容
	cs := r.ChunkSizeBytes()
	big := make([]byte, cs*3+cs/2)
	for i := range big {
		big[i] = byte(i % 251)
	}
	txn, _ := r.BeginSnapshot(time.Now())
	var refs []meta.ChunkRef
	for off := 0; off < len(big); off += cs {
		end := off + cs
		if end > len(big) {
			end = len(big)
		}
		id, _, err := r.StoreChunk(big[off:end])
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, meta.ChunkRef{ChunkID: id, IDX: len(refs)})
	}
	txn.UpsertFile(meta.FileRow{Path: "big.bin", Mode: 0o644, Size: int64(len(big))}, refs)
	sid, _ := txn.Commit()

	fr, row, err := r.OpenFile(sid, "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer fr.Close()
	if row.Size != int64(len(big)) {
		t.Fatalf("size 不符: %d", row.Size)
	}
	// Seek 到第 2 块开头，读回比对
	pos, err := fr.Seek(int64(2*cs), io.SeekStart)
	if err != nil || pos != int64(2*cs) {
		t.Fatalf("seek 失败: %d, %v", pos, err)
	}
	got := make([]byte, cs/2)
	if _, err := io.ReadFull(fr, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, big[2*cs:2*cs+cs/2]) {
		t.Fatal("seek 后内容不符")
	}
	// SeekEnd 负偏移
	if _, err := fr.Seek(-10, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	last := make([]byte, 10)
	if _, err := io.ReadFull(fr, last); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(last, big[len(big)-10:]) {
		t.Fatal("SeekEnd 内容不符")
	}
	// 从头顺序读与原始逐字节一致
	if _, err := fr.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(fr)
	if err != nil || !bytes.Equal(all, big) {
		t.Fatal("全量读不符")
	}
	// 不存在文件报错
	if _, _, err := r.OpenFile(sid, "nonexistent"); err == nil {
		t.Fatal("不存在文件应报错")
	}
}

func TestStoreChunkNewAndReused(t *testing.T) {
	r, be := newTestRepo(t)
	data := bytes.Repeat([]byte{0x77}, 100)
	c1, reused, err := r.StoreChunk(data)
	if err != nil || reused {
		t.Fatalf("首次存储应新建: %v %v", c1, reused)
	}
	blobs, _ := be.List()
	if len(blobs) != 1 {
		t.Fatalf("后端应有 1 个 blob: %v", blobs)
	}
	// 相同内容 → 去重复用
	c2, reused, err := r.StoreChunk(data)
	if err != nil || !reused || c2 != c1 {
		t.Fatalf("重复存储应复用: %d %v", c2, reused)
	}
	if blobs, _ = be.List(); len(blobs) != 1 {
		t.Fatalf("去重后不应新增 blob: %v", blobs)
	}
}

func TestStoreChunkEncryptsData(t *testing.T) {
	r, be := newTestRepo(t)
	data := []byte("sensitive backup content")
	if _, _, err := r.StoreChunk(data); err != nil {
		t.Fatal(err)
	}
	blobs, _ := be.List()
	raw, _ := be.Get(blobs[0])
	if bytes.Contains(raw, data) {
		t.Fatal("后端不应出现明文")
	}
}

func TestStoreChunkPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db1, _ := meta.Open(filepath.Join(dir, "meta.db"))
	key, _ := crypto.GenerateKey()
	be := backend.NewInMemory()
	r1 := New(db1, be, key, 64)
	data := []byte("persist me")
	c1, _, err := r1.StoreChunk(data)
	if err != nil {
		t.Fatal(err)
	}
	db1.Close()

	db2, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	r2 := New(db2, be, key, 64)
	c2, reused, err := r2.StoreChunk(data)
	if err != nil || !reused || c2 != c1 {
		t.Fatalf("重开后去重失效: %d %v %v", c2, reused, err)
	}
	// 后端 blob 可用密钥解密
	blobs, _ := be.List()
	raw, _ := be.Get(blobs[0])
	got, err := key.Decrypt(raw, blobs[0])
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("blob 解密失败: %v", err)
	}
}

var _ = os.Getenv // 避免误删依赖告警

func TestSnapshotTxnCommit(t *testing.T) {
	r, be := newTestRepo(t)
	// 第一次快照：写入 2 个文件
	txn, err := r.BeginSnapshot(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("file-one-content")
	c1, _, _ := r.StoreChunk(data)
	if err := txn.UpsertFile(meta.FileRow{Path: "one.txt", Mode: 0o644, Size: int64(len(data))}, []meta.ChunkRef{{ChunkID: c1, IDX: 0}}); err != nil {
		t.Fatal(err)
	}
	c2, _, _ := r.StoreChunk([]byte("file-two-content"))
	if err := txn.UpsertFile(meta.FileRow{Path: "two.txt", Mode: 0o644, Size: 16}, []meta.ChunkRef{{ChunkID: c2, IDX: 0}}); err != nil {
		t.Fatal(err)
	}
	s1, err := txn.Commit()
	if err != nil || s1 == 0 {
		t.Fatalf("Commit: %v %d", err, s1)
	}

	// 第二次快照：复制 + 删 one.txt + 改 two.txt
	txn2, err := r.BeginSnapshot(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := txn2.DeleteFile("one.txt"); err != nil {
		t.Fatal(err)
	}
	c3, _, _ := r.StoreChunk([]byte("file-two-updated"))
	if err := txn2.UpsertFile(meta.FileRow{Path: "two.txt", Mode: 0o600, Size: 16}, []meta.ChunkRef{{ChunkID: c3, IDX: 0}}); err != nil {
		t.Fatal(err)
	}
	s2, err := txn2.Commit()
	if err != nil || s2 == s1 {
		t.Fatalf("第二次 Commit: %v %d", err, s2)
	}

	// 验证快照 1：两个文件都在且内容可读
	var buf bytes.Buffer
	if err := r.ReadFile(s1, "one.txt", &buf); err != nil || buf.String() != "file-one-content" {
		t.Fatalf("快照1 读取 one.txt: %v %q", err, buf.String())
	}
	// 验证快照 2：one.txt 删除、two.txt 更新
	files, err := r.meta.GetFiles(s2)
	if err != nil || len(files) != 1 || files[0].Path != "two.txt" {
		t.Fatalf("快照2 文件清单: %v %v", files, err)
	}
	buf.Reset()
	if err := r.ReadFile(s2, "two.txt", &buf); err != nil || buf.String() != "file-two-updated" {
		t.Fatalf("快照2 读取 two.txt: %v %q", err, buf.String())
	}
	// 去重验证：两快照 one.txt 引用同一 chunk
	var refcount int
	// 通过公开接口间接验证：快照1 和快照2 的 two.txt 大小不同 → chunk 不同；one.txt 只在快照1
	_ = refcount
	// 后端 blob 数量 = 3 个唯一内容
	blobs, _ := be.List()
	if len(blobs) != 3 {
		t.Fatalf("后端应有 3 个唯一 blob: %v", blobs)
	}
}

func TestSnapshotTxnRollback(t *testing.T) {
	r, _ := newTestRepo(t)
	txn, err := r.BeginSnapshot(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.UpsertFile(meta.FileRow{Path: "x.txt", Mode: 0o644}, nil); err != nil {
		t.Fatal(err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	// 回滚后应无快照
	id, err := r.LatestSnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	if id != 0 {
		t.Fatalf("回滚后不应有快照: %d", id)
	}
}

func TestReadFileMultiChunk(t *testing.T) {
	r, _ := newTestRepo(t)
	txn, _ := r.BeginSnapshot(time.Now())
	// 超过 chunkSize(64) 的内容 → 2 块
	big := bytes.Repeat([]byte{0x5A}, 100)
	var refs []meta.ChunkRef
	var idx int
	if err := Split(bytes.NewReader(big), 64, func(c []byte) error {
		id, _, err := r.StoreChunk(c)
		if err != nil {
			return err
		}
		refs = append(refs, meta.ChunkRef{ChunkID: id, IDX: idx})
		idx++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	txn.UpsertFile(meta.FileRow{Path: "big.bin", Mode: 0o644, Size: int64(len(big))}, refs)
	sid, _ := txn.Commit()

	var buf bytes.Buffer
	if err := r.ReadFile(sid, "big.bin", &buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), big) {
		t.Fatalf("多块读取不一致: got %d bytes, want %d", buf.Len(), len(big))
	}
}

func TestReadFileMissing(t *testing.T) {
	r, _ := newTestRepo(t)
	if err := r.ReadFile(1, "nope.txt", &bytes.Buffer{}); err == nil {
		t.Fatal("读取不存在的文件应报错")
	}
}

// 造一个含 3 个文件 1 个子目录的快照（供过滤/校验和测试复用）。
func seedSnapshot(t *testing.T, r *Repo) int64 {
	t.Helper()
	txn, err := r.BeginSnapshot(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	add := func(path string, data []byte, mode uint32) {
		t.Helper()
		c, _, err := r.StoreChunk(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := txn.UpsertFile(meta.FileRow{Path: path, Mode: mode, Size: int64(len(data))},
			[]meta.ChunkRef{{ChunkID: c, IDX: 0}}); err != nil {
			t.Fatal(err)
		}
	}
	add("top.txt", []byte("top content"), 0o644)
	add("sub/inner.txt", []byte("inner content"), 0o644)
	add("sub/deep/deep.txt", []byte("deep content"), 0o600)
	if err := txn.UpsertFile(meta.FileRow{Path: "sub", IsDir: true, Mode: 0o40755}, nil); err != nil {
		t.Fatal(err)
	}
	if err := txn.UpsertFile(meta.FileRow{Path: "sub/deep", IsDir: true, Mode: 0o40755}, nil); err != nil {
		t.Fatal(err)
	}
	sid, err := txn.Commit()
	if err != nil {
		t.Fatal(err)
	}
	return sid
}

func TestSnapshotFileRowsFilter(t *testing.T) {
	r, _ := newTestRepo(t)
	sid := seedSnapshot(t, r)

	all, err := r.SnapshotFileRows(sid, "")
	if err != nil || len(all) != 5 {
		t.Fatalf("空前缀应返回全部: %d %v", len(all), err)
	}
	// 子目录前缀：自身 + 子树
	sub, err := r.SnapshotFileRows(sid, "sub")
	if err != nil || len(sub) != 4 {
		t.Fatalf("子目录前缀应返回 4 条: %d %v", len(sub), err)
	}
	for _, f := range sub {
		if f.Path != "sub" && f.Path != "sub/inner.txt" && f.Path != "sub/deep" && f.Path != "sub/deep/deep.txt" {
			t.Fatalf("过滤结果超出子目录: %s", f.Path)
		}
	}
	// 单文件
	one, err := r.SnapshotFileRows(sid, "top.txt")
	if err != nil || len(one) != 1 || one[0].Path != "top.txt" {
		t.Fatalf("单文件过滤: %v %v", one, err)
	}
}

// 前缀歧义断言独立于 seedSnapshot 的数据，直接在过滤结果上验证。
func TestSnapshotFileRowsAmbiguousPrefix(t *testing.T) {
	r, _ := newTestRepo(t)
	txn, _ := r.BeginSnapshot(time.Now())
	add := func(path string, data []byte) {
		c, _, _ := r.StoreChunk(data)
		txn.UpsertFile(meta.FileRow{Path: path, Mode: 0o644, Size: int64(len(data))},
			[]meta.ChunkRef{{ChunkID: c, IDX: 0}})
	}
	add("x", []byte("x"))
	add("x/y", []byte("y"))
	add("x2", []byte("x2"))
	sid, _ := txn.Commit()

	got, err := r.SnapshotFileRows(sid, "x")
	if err != nil || len(got) != 2 {
		t.Fatalf("前缀 x 应匹配 x 与 x/y 而非 x2: %d %v", len(got), err)
	}
	for _, f := range got {
		if f.Path == "x2" {
			t.Fatalf("前缀过滤误匹配 x2: %v", got)
		}
	}
}

func TestStreamFileChecksum(t *testing.T) {
	r, _ := newTestRepo(t)
	sid := seedSnapshot(t, r)

	content := []byte("top content")
	// 校验和 = 纯 MD5(content)（sum_end 的 CSUM_MD5 分支；seed 参数对 MD5 无效）
	var buf bytes.Buffer
	n, sum, err := r.StreamFile(sid, "top.txt", 0, &buf)
	if err != nil || n != int64(len(content)) {
		t.Fatalf("StreamFile: %v %d", err, n)
	}
	want := md5.Sum(content)
	if sum != want {
		t.Fatalf("校验和: got %x want %x", sum, want)
	}
	buf.Reset()
	_, sum, err = r.StreamFile(sid, "top.txt", 42, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if sum != want {
		t.Fatalf("seed 参数不应影响 MD5: got %x want %x", sum, want)
	}
	// 大文件跨块 + 空文件
	big := bytes.Repeat([]byte{0xAB}, 150)
	addFile := func(path string, data []byte) {
		var refs []meta.ChunkRef
		idx := 0
		Split(bytes.NewReader(data), 64, func(c []byte) error {
			id, _, err := r.StoreChunk(c)
			if err != nil {
				return err
			}
			refs = append(refs, meta.ChunkRef{ChunkID: id, IDX: idx})
			idx++
			return nil
		})
		txn, _ := r.BeginSnapshot(time.Now())
		txn.UpsertFile(meta.FileRow{Path: path, Mode: 0o644, Size: int64(len(data))}, refs)
		txn.Commit()
	}
	addFile("big.bin", big)
	addFile("empty.txt", nil)
	sid2, _ := r.LatestSnapshotID()
	for _, tc := range []struct {
		path string
		want []byte
	}{
		{"big.bin", big},
		{"empty.txt", nil},
	} {
		buf.Reset()
		n, _, err := r.StreamFile(sid2, tc.path, 7, &buf)
		if err != nil {
			t.Fatalf("StreamFile(%s): %v", tc.path, err)
		}
		if !bytes.Equal(buf.Bytes(), tc.want) || n != int64(len(tc.want)) {
			t.Fatalf("StreamFile(%s) 内容不符: %d bytes", tc.path, n)
		}
	}
}

func TestActiveSnapshotID(t *testing.T) {
	r, _ := newTestRepo(t)
	s1 := seedSnapshot(t, r)
	// 未设置 → 默认最新
	id, err := r.ActiveSnapshotID()
	if err != nil || id != s1 {
		t.Fatalf("默认活跃快照应是最新: %d %v", id, err)
	}
	// 手动设置（CLI 语义）
	if err := r.SetActiveSnapshot(s1); err != nil {
		t.Fatal(err)
	}
	seedSnapshot(t, r) // 新建快照，活跃快照应保持
	id, err = r.ActiveSnapshotID()
	if err != nil || id != s1 {
		t.Fatalf("活跃快照应保持手动设置值: %d %v", id, err)
	}
}

func TestGC(t *testing.T) {
	r, be := newTestRepo(t)
	// 正常快照 + 孤儿模拟：
	// 1) 引用中的 blob（正常备份）
	// 2) StoreChunk 后 Rollback 的 blob（中断会话残留 = 孤儿）
	// 3) 直接写入后端的 blob（无任何元数据 = 孤儿）
	sid := seedSnapshot(t, r)
	blobs1, _ := be.List()
	if len(blobs1) != 3 {
		t.Fatalf("seedSnapshot 应有 3 个 blob: %v", blobs1)
	}

	txn, _ := r.BeginSnapshot(time.Now())
	c, _, _ := r.StoreChunk([]byte("orphan from rollback"))
	txn.UpsertFile(meta.FileRow{Path: "x.txt", Mode: 0o644, Size: 19}, []meta.ChunkRef{{ChunkID: c, IDX: 0}})
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
	_ = c
	orphanName := "ff" + strings.Repeat("0", 62) // 64 hex 伪 blob 名
	if err := be.Put(orphanName, []byte("direct orphan")); err != nil {
		t.Fatal(err)
	}

	deleted, err := r.GC()
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("应删除 2 个孤儿 blob，实际 %d", deleted)
	}
	blobs, _ := be.List()
	if len(blobs) != 3 {
		t.Fatalf("GC 后应剩 3 个被引用 blob: %v", blobs)
	}
	// 快照仍可读（被引用 blob 未误删）
	var buf bytes.Buffer
	if err := r.ReadFile(sid, "top.txt", &buf); err != nil || buf.String() != "top content" {
		t.Fatalf("GC 后读取失败: %v %q", err, buf.String())
	}
	// 再次 GC 无操作
	if n, err := r.GC(); err != nil || n != 0 {
		t.Fatalf("二次 GC 应为 0: %d %v", n, err)
	}
}

// TestUpsertRefcount：覆盖写入同一路径时旧 chunk 引用递减、新 chunk 引用递增。
func TestUpsertRefcount(t *testing.T) {
	r, be := newTestRepo(t)
	txn, _ := r.BeginSnapshot(time.Now())
	c1, _, _ := r.StoreChunk([]byte("version one"))
	txn.UpsertFile(meta.FileRow{Path: "v.txt", Mode: 0o644, Size: 11}, []meta.ChunkRef{{ChunkID: c1, IDX: 0}})
	c2, _, _ := r.StoreChunk([]byte("version two longer"))
	txn.UpsertFile(meta.FileRow{Path: "v.txt", Mode: 0o644, Size: 18}, []meta.ChunkRef{{ChunkID: c2, IDX: 0}})
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	// c1 的引用被覆盖移除 -> refcount 归零 -> chunk 行删除 -> blob 成孤儿
	n, err := r.GC()
	if err != nil || n != 1 {
		t.Fatalf("GC 应回收被覆盖的旧 blob: %d %v", n, err)
	}
	blobs, _ := be.List()
	if len(blobs) != 1 {
		t.Fatalf("后端应只剩新 blob: %v", blobs)
	}
	var buf bytes.Buffer
	if err := r.ReadFile(mustLatest(t, r), "v.txt", &buf); err != nil || buf.String() != "version two longer" {
		t.Fatalf("覆盖后读取失败: %v %q", err, buf.String())
	}
}

func mustLatest(t *testing.T, r *Repo) int64 {
	t.Helper()
	id, err := r.LatestSnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestPrune：多快照按策略删除 + blob 回收 + 保留快照可读。
func TestPrune(t *testing.T) {
	r, be := newTestRepo(t)
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.Local)
	contents := []string{"snap one", "snap two", "snap three"}
	for i, data := range contents {
		txn, err := r.BeginSnapshot(base.Add(time.Duration(i) * time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		c, _, _ := r.StoreChunk([]byte(data))
		if err := txn.UpsertFile(meta.FileRow{Path: "f.txt", Mode: 0o644, Size: int64(len(data))},
			[]meta.ChunkRef{{ChunkID: c, IDX: 0}}); err != nil {
			t.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if blobs, _ := be.List(); len(blobs) != 3 {
		t.Fatalf("应有 3 个 blob: %v", blobs)
	}

	// keep_last=1：删除前 2 个快照，回收其 blob
	removed, blobs, err := r.Prune(prune.Policy{KeepLast: 1})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if removed != 2 || blobs != 2 {
		t.Fatalf("应删 2 快照 2 blob: %d %d", removed, blobs)
	}
	sid, _ := r.LatestSnapshotID()
	var buf bytes.Buffer
	if err := r.ReadFile(sid, "f.txt", &buf); err != nil || buf.String() != "snap three" {
		t.Fatalf("保留快照读取失败: %v %q", err, buf.String())
	}
	// 旧快照不可读
	if err := r.ReadFile(sid-2, "f.txt", &buf); err == nil {
		t.Fatal("被删除的快照不应可读")
	}
	if blobs, _ := be.List(); len(blobs) != 1 {
		t.Fatalf("后端应只剩 1 个 blob: %v", blobs)
	}
	// 无策略 prune：不删除
	if removed, _, err := r.Prune(prune.Policy{}); err != nil || removed != 0 {
		t.Fatalf("无策略不应删除: %d %v", removed, err)
	}
}

// TestTrimAfterCommit：快照提交后的收尾裁剪（receiver 与 WebDAV 写共用）。
func TestTrimAfterCommit(t *testing.T) {
	r, _ := newTestRepo(t)
	// 三次提交产生 3 个快照
	for i := 0; i < 3; i++ {
		txn, err := r.BeginSnapshot(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	latest, err := r.LatestSnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	// keepHistory=true：不裁剪
	if _, _, err := r.TrimAfterCommit(latest, true); err != nil {
		t.Fatal(err)
	}
	if n, err := r.SnapshotCountForTest(); err != nil || n != 3 {
		t.Fatalf("多版本模式应保留 3 个快照, got %d, %v", n, err)
	}
	// keepHistory=false：收敛到 1
	if _, _, err := r.TrimAfterCommit(latest, false); err != nil {
		t.Fatal(err)
	}
	if n, err := r.SnapshotCountForTest(); err != nil || n != 1 {
		t.Fatalf("单份模式应保留 1 个快照, got %d, %v", n, err)
	}
}

// TestKeepOnlySnapshot：单份模式收尾。三个内容各异的快照 + 一个纯复制
// （无变化）快照：裁剪删旧快照、回收独占 blob、共享 chunk 保留；随后再来
// 一个无变化快照时裁剪不产生孤儿 chunk，跳过 GC（后端零访问）。
func TestKeepOnlySnapshot(t *testing.T) {
	r, be := newTestRepo(t)
	base := time.Date(2026, 8, 1, 0, 0, 0, 0, time.Local)
	contents := []string{"snap one", "snap two", "snap three"}
	for i, data := range contents {
		txn, err := r.BeginSnapshot(base.Add(time.Duration(i) * time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		c, _, _ := r.StoreChunk([]byte(data))
		if err := txn.UpsertFile(meta.FileRow{Path: "f.txt", Mode: 0o644, Size: int64(len(data))},
			[]meta.ChunkRef{{ChunkID: c, IDX: 0}}); err != nil {
			t.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// 快照 4：纯无变化会话（复制快照 3 清单，共享其 chunk）
	txn4, err := r.BeginSnapshot(base.Add(3 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	s4, err := txn4.Commit()
	if err != nil {
		t.Fatal(err)
	}

	removed, blobs, err := r.KeepOnlySnapshot(s4)
	if err != nil {
		t.Fatalf("KeepOnlySnapshot: %v", err)
	}
	// 删除 3 个旧快照；snap one/two 的 chunk 归零回收（2 blob），
	// snap three 的 chunk 被快照 4 共享保留（GC 不动其 blob）
	if removed != 3 || blobs != 2 {
		t.Fatalf("应删 3 快照 2 blob: %d %d", removed, blobs)
	}
	var buf bytes.Buffer
	if err := r.ReadFile(s4, "f.txt", &buf); err != nil || buf.String() != "snap three" {
		t.Fatalf("保留快照读取失败: %v %q", err, buf.String())
	}
	if got, _ := be.List(); len(got) != 1 {
		t.Fatalf("后端应只剩 1 个 blob: %v", got)
	}
	infos, _ := r.meta.SnapshotList()
	if len(infos) != 1 || infos[0].ID != s4 {
		t.Fatalf("应只剩快照 %d: %+v", s4, infos)
	}

	// 再来一个无变化快照：裁剪不删 chunk 行（共享），不触发 GC、后端无变化
	txn5, err := r.BeginSnapshot(base.Add(4 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	s5, err := txn5.Commit()
	if err != nil {
		t.Fatal(err)
	}
	removed, blobs, err = r.KeepOnlySnapshot(s5)
	if err != nil {
		t.Fatalf("KeepOnlySnapshot 无变化: %v", err)
	}
	if removed != 1 || blobs != 0 {
		t.Fatalf("无变化会话应删 1 快照 0 blob（跳过 GC）: %d %d", removed, blobs)
	}
	if got, _ := be.List(); len(got) != 1 {
		t.Fatalf("后端 blob 不应变化: %v", got)
	}
}

// TestGetFileRow：repo 层包装的 quick check 查询（UpsertFile 后可查回）。
func TestGetFileRow(t *testing.T) {
	r, _ := newTestRepo(t)
	now := time.Unix(1700000000, 0).UTC()
	txn, err := r.BeginSnapshot(now)
	if err != nil {
		t.Fatal(err)
	}
	want := meta.FileRow{Path: "a.txt", Mode: 0o644, Size: 5, MTimeNs: now.UnixNano()}
	if err := txn.UpsertFile(want, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	sid, err := r.LatestSnapshotID()
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := r.GetFileRow(sid, "a.txt")
	if err != nil || !ok {
		t.Fatalf("GetFileRow: ok=%v err=%v", ok, err)
	}
	if got.Size != 5 || got.MTimeNs != want.MTimeNs || got.Path != "a.txt" {
		t.Fatalf("字段不一致: %+v", got)
	}
	if _, ok, err := r.GetFileRow(sid, "nope.txt"); err != nil || ok {
		t.Fatalf("不存在路径: ok=%v err=%v", ok, err)
	}
}

// TestPutFileAndFriends：写即快照方法族（PutFile/Mkcol/DeletePath/MovePath）。
func TestPutFileAndFriends(t *testing.T) {
	r, _ := newTestRepo(t)
	now := time.Now().UnixNano()

	// 根文件 PUT
	if err := r.PutFile("a.txt", 0o644, now, strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	sid, _ := r.LatestSnapshotID()
	row, ok, err := r.GetFileRow(sid, "a.txt")
	if err != nil || !ok {
		t.Fatalf("a.txt 应存在: %v, %v", ok, err)
	}
	if row.Size != 5 {
		t.Fatalf("size 不符: %d", row.Size)
	}

	// 父目录缺失 → os.ErrNotExist
	if err := r.PutFile("sub/b.txt", 0o644, now, strings.NewReader("x")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("父目录缺失应 ErrNotExist, got %v", err)
	}

	// Mkcol + 目录内 PUT
	if err := r.Mkcol("sub"); err != nil {
		t.Fatal(err)
	}
	if err := r.Mkcol("sub"); !errors.Is(err, os.ErrExist) {
		t.Fatalf("重复 Mkcol 应 ErrExist, got %v", err)
	}
	if err := r.PutFile("sub/b.txt", 0o644, now, strings.NewReader("xyz")); err != nil {
		t.Fatal(err)
	}

	// MovePath 文件（内容 chunk 引用保持）
	if err := r.MovePath("a.txt", "a2.txt"); err != nil {
		t.Fatal(err)
	}
	sid, _ = r.LatestSnapshotID()
	if _, ok, _ := r.GetFileRow(sid, "a.txt"); ok {
		t.Fatal("移动后源应不存在")
	}
	fr, _, err := r.OpenFile(sid, "a2.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(fr)
	fr.Close()
	if string(got) != "hello" {
		t.Fatalf("移动后内容不符: %q", got)
	}

	// DeletePath 文件
	if err := r.DeletePath("a2.txt"); err != nil {
		t.Fatal(err)
	}
	sid, _ = r.LatestSnapshotID()
	if _, ok, _ := r.GetFileRow(sid, "a2.txt"); ok {
		t.Fatal("删除后应不存在")
	}
	// DeletePath 目录（递归含 b.txt）
	if err := r.DeletePath("sub"); err != nil {
		t.Fatal(err)
	}
	sid, _ = r.LatestSnapshotID()
	if _, ok, _ := r.GetFileRow(sid, "sub"); ok {
		t.Fatal("删除后 sub 应不存在")
	}
	if _, ok, _ := r.GetFileRow(sid, "sub/b.txt"); ok {
		t.Fatal("删除后 sub/b.txt 应不存在")
	}
	// DeletePath 不存在 → os.ErrNotExist
	if err := r.DeletePath("nope"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("删除不存在应 ErrNotExist, got %v", err)
	}
}

// TestPutFileChunkDedup：相同内容两次 PUT 复用 chunk（后端 blob 数不变）。
func TestPutFileChunkDedup(t *testing.T) {
	r, be := newTestRepo(t)
	now := time.Now().UnixNano()
	if err := r.PutFile("one.txt", 0o644, now, strings.NewReader("same")); err != nil {
		t.Fatal(err)
	}
	if err := r.PutFile("two.txt", 0o644, now, strings.NewReader("same")); err != nil {
		t.Fatal(err)
	}
	blobs, err := be.List()
	if err != nil || len(blobs) != 1 {
		t.Fatalf("去重后应只有 1 个 blob, got %d, %v", len(blobs), err)
	}
	for _, p := range []string{"one.txt", "two.txt"} {
		fr, _, err := r.OpenFile(mustLatest(t, r), p)
		if err != nil {
			t.Fatalf("打开 %s: %v", p, err)
		}
		got, _ := io.ReadAll(fr)
		fr.Close()
		if string(got) != "same" {
			t.Fatalf("%s 内容不符: %q", p, got)
		}
	}
}
