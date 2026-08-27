// internal/core/meta/db_test.go
package meta

import (
	"path/filepath"
	"testing"
	"time"
)

func openTemp(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestSnapshotLifecycle(t *testing.T) {
	db := openTemp(t)
	now := time.Unix(1700000000, 0).UTC()
	s1, err := db.CreateSnapshot(now)
	if err != nil {
		t.Fatal(err)
	}
	f := FileRow{Path: "a/b.txt", Mode: 0o644, Size: 10, MTimeNs: now.UnixNano()}
	fid, err := db.UpsertFile(s1, f)
	if err != nil || fid == 0 {
		t.Fatalf("UpsertFile: %v %d", err, fid)
	}
	if err := db.DeleteFile(s1, "a/b.txt"); err != nil {
		t.Fatal(err)
	}
	files, err := db.GetFiles(s1)
	if err != nil || len(files) != 0 {
		t.Fatalf("删除后应为空: %v %v", files, err)
	}
}

func TestCopyFilesCreatesNewSnapshot(t *testing.T) {
	db := openTemp(t)
	now := time.Unix(1700000000, 0).UTC()
	s1, _ := db.CreateSnapshot(now)
	db.UpsertFile(s1, FileRow{Path: "keep.txt", Mode: 0o644, Size: 1})
	db.UpsertFile(s1, FileRow{Path: "drop.txt", Mode: 0o644, Size: 1})
	s2, _ := db.CreateSnapshot(now.Add(time.Minute))
	if err := db.CopyFiles(s1, s2); err != nil {
		t.Fatal(err)
	}
	db.DeleteFile(s2, "drop.txt")
	files, _ := db.GetFiles(s2)
	if len(files) != 1 || files[0].Path != "keep.txt" {
		t.Fatalf("快照 2 应为 1 个文件: %v", files)
	}
	files1, _ := db.GetFiles(s1)
	if len(files1) != 2 {
		t.Fatalf("快照 1 应保持 2 个文件: %v", files1)
	}
}

// 快照自包含的关键：CopyFiles 必须同时复制 chunk 关联（继承文件可读）并递增 refcount。
func TestCopyFilesCopiesChunkRefs(t *testing.T) {
	db := openTemp(t)
	var h [32]byte
	h[0] = 9
	cid, _ := db.InsertChunk(h, "blob9", 3)
	s1, _ := db.CreateSnapshot(time.Now())
	fid, _ := db.UpsertFile(s1, FileRow{Path: "keep.txt", Mode: 0o644, Size: 3})
	db.AttachChunks(fid, []ChunkRef{{ChunkID: cid, IDX: 0}})
	db.IncrRefcount(cid)

	s2, _ := db.CreateSnapshot(time.Now())
	if err := db.CopyFiles(s1, s2); err != nil {
		t.Fatal(err)
	}
	// 继承文件在快照2 中应有关联（可通过 file_chunks 行数验证）
	var n int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM file_chunks fc JOIN files f ON f.id = fc.file_id WHERE f.snapshot_id = ?`, s2).Scan(&n); err != nil || n != 1 {
		t.Fatalf("快照2 应继承 1 条 chunk 关联: %d %v", n, err)
	}
	// refcount 应因继承引用递增为 2
	var rc int
	if err := db.db.QueryRow(`SELECT refcount FROM chunks WHERE id = ?`, cid).Scan(&rc); err != nil || rc != 2 {
		t.Fatalf("refcount 应递增为 2: %d %v", rc, err)
	}
}

func TestFileRowRoundtrip(t *testing.T) {
	db := openTemp(t)
	s1, _ := db.CreateSnapshot(time.Now())
	want := FileRow{
		Path: "sub/链接文件.txt", IsSymlink: true, Mode: 0o777,
		UID: 1000, GID: 1000, Size: 0, MTimeNs: 1700000000123456789,
		Xattrs: []byte{0x01, 0x02}, LinkTarget: "/etc/passwd",
	}
	db.UpsertFile(s1, want)
	got, _ := db.GetFiles(s1)
	if len(got) != 1 {
		t.Fatal("应读回 1 个文件")
	}
	g := got[0]
	if g.Path != want.Path || g.IsSymlink != want.IsSymlink || g.Mode != want.Mode ||
		g.UID != want.UID || g.GID != want.GID || g.MTimeNs != want.MTimeNs ||
		len(g.Xattrs) != 2 || g.LinkTarget != want.LinkTarget {
		t.Fatalf("字段不一致: %+v vs %+v", g, want)
	}
}

func TestMetaKeyValue(t *testing.T) {
	db := openTemp(t)
	if err := db.SetMeta("active_snapshot", "3"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := db.GetMeta("active_snapshot")
	if err != nil || !ok || v != "3" {
		t.Fatalf("meta 读写失败: %q %v %v", v, ok, err)
	}
}

func TestChunkDedup(t *testing.T) {
	db := openTemp(t)
	var h1, h2 [32]byte
	h1[0] = 1
	h2[0] = 2

	c1, err := db.InsertChunk(h1, "blob1", 100)
	if err != nil || c1 == 0 {
		t.Fatalf("InsertChunk: %v %d", err, c1)
	}
	// 相同哈希插入应失败（UNIQUE）
	if _, err := db.InsertChunk(h1, "blob2", 100); err == nil {
		t.Fatal("重复哈希应报错")
	}
	info, exists, err := db.FindChunkByHash(h1)
	if err != nil || !exists || info.BlobName != "blob1" || info.Size != 100 {
		t.Fatalf("FindChunkByHash: %+v %v %v", info, exists, err)
	}
	if _, exists, _ := db.FindChunkByHash(h2); exists {
		t.Fatal("不存在的哈希不应命中")
	}
}

func TestDeleteSnapshotCascades(t *testing.T) {
	db := openTemp(t)
	s1, err := db.CreateSnapshot(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpsertFile(s1, FileRow{Path: "a.txt", Mode: 0o644, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DeleteSnapshot(s1); err != nil {
		t.Fatal(err)
	}
	var files int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM files WHERE snapshot_id = ?`, s1).Scan(&files); err != nil || files != 0 {
		t.Fatalf("级联删除后 files 应为 0: %d %v", files, err)
	}
	var snaps int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM snapshots WHERE id = ?`, s1).Scan(&snaps); err != nil || snaps != 0 {
		t.Fatalf("级联删除后 snapshots 应无 s1: %d %v", snaps, err)
	}
}

// TestDeleteSnapshotReturnsChunkCount：返回值 = 本次删除快照导致 refcount 归零
// 而删除的 chunk 行数（被其他快照继续引用的 chunk 不计入、不删除）。
func TestDeleteSnapshotReturnsChunkCount(t *testing.T) {
	db := openTemp(t)
	var shared, excl [32]byte
	shared[0], excl[0] = 1, 2
	cShared, _ := db.InsertChunk(shared, "blob-shared", 10)
	cExcl, _ := db.InsertChunk(excl, "blob-excl", 10)

	s1, _ := db.CreateSnapshot(time.Now())
	fid, _ := db.UpsertFile(s1, FileRow{Path: "a.txt", Mode: 0o644, Size: 10})
	db.AttachChunks(fid, []ChunkRef{{ChunkID: cShared, IDX: 0}})
	db.IncrRefcount(cShared)
	fid2, _ := db.UpsertFile(s1, FileRow{Path: "b.txt", Mode: 0o644, Size: 10})
	db.AttachChunks(fid2, []ChunkRef{{ChunkID: cExcl, IDX: 0}})
	db.IncrRefcount(cExcl)

	// s2 继续引用 shared：删 s1 后 shared refcount 2->1 保留，excl 归零删除
	s2, _ := db.CreateSnapshot(time.Now())
	fid3, _ := db.UpsertFile(s2, FileRow{Path: "a.txt", Mode: 0o644, Size: 10})
	db.AttachChunks(fid3, []ChunkRef{{ChunkID: cShared, IDX: 0}})
	db.IncrRefcount(cShared)

	deleted, err := db.DeleteSnapshot(s1)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("应只删 1 个 chunk 行（独占者），得到 %d", deleted)
	}
	var n int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM chunks WHERE id = ?`, cShared).Scan(&n); err != nil || n != 1 {
		t.Fatalf("共享 chunk 应保留: %d %v", n, err)
	}
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM chunks WHERE id = ?`, cExcl).Scan(&n); err != nil || n != 0 {
		t.Fatalf("独占 chunk 行应被删除: %d %v", n, err)
	}
}

func TestAttachChunksAndRefcount(t *testing.T) {
	db := openTemp(t)
	var h [32]byte
	h[0] = 7
	cid, _ := db.InsertChunk(h, "blob", 50)
	s1, _ := db.CreateSnapshot(time.Now())
	fid, _ := db.UpsertFile(s1, FileRow{Path: "f.txt", Mode: 0o644, Size: 150})
	if err := db.AttachChunks(fid, []ChunkRef{{ChunkID: cid, IDX: 0}, {ChunkID: cid, IDX: 1}, {ChunkID: cid, IDX: 2}}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM file_chunks WHERE file_id = ?`, fid).Scan(&count); err != nil || count != 3 {
		t.Fatalf("file_chunks 应为 3 行: %d %v", count, err)
	}
	if err := db.IncrRefcount(cid); err != nil {
		t.Fatal(err)
	}
	var rc int
	if err := db.db.QueryRow(`SELECT refcount FROM chunks WHERE id = ?`, cid).Scan(&rc); err != nil || rc != 1 {
		t.Fatalf("refcount 应为 1: %d %v", rc, err)
	}
}

// TestDeleteFileDecrementsRefcount：删除文件后其 chunk 引用递减，归零时 chunk
// 行删除（blob 随之成为孤儿，由 GC 回收——refcount 闭环）。
func TestDeleteFileDecrementsRefcount(t *testing.T) {
	db := openTemp(t)
	var h [32]byte
	h[0] = 42
	cid, _ := db.InsertChunk(h, "blob42", 13)
	s1, _ := db.CreateSnapshot(time.Now())
	fid, _ := db.UpsertFile(s1, FileRow{Path: "a.txt", Mode: 0o644, Size: 13})
	db.AttachChunks(fid, []ChunkRef{{ChunkID: cid, IDX: 0}})
	db.IncrRefcount(cid)
	fid2, _ := db.UpsertFile(s1, FileRow{Path: "b.txt", Mode: 0o644, Size: 13})
	db.AttachChunks(fid2, []ChunkRef{{ChunkID: cid, IDX: 0}})
	db.IncrRefcount(cid)

	// 删一个引用：refcount 2->1，chunk 保留
	if err := db.DeleteFile(s1, "a.txt"); err != nil {
		t.Fatal(err)
	}
	var rc int
	if err := db.db.QueryRow(`SELECT refcount FROM chunks WHERE id = ?`, cid).Scan(&rc); err != nil || rc != 1 {
		t.Fatalf("refcount 应为 1: %d %v", rc, err)
	}
	// 删第二个引用：refcount 归零 -> chunk 行删除
	if err := db.DeleteFile(s1, "b.txt"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM chunks WHERE id = ?`, cid).Scan(&n); err != nil || n != 0 {
		t.Fatalf("chunk 行应被删除: %d %v", n, err)
	}
}

// TestUpsertDecrementsOldRefs：覆盖写入同一路径时旧引用递减（归零删行）。
func TestUpsertDecrementsOldRefs(t *testing.T) {
	db := openTemp(t)
	var h1, h2 [32]byte
	h1[0], h2[0] = 1, 2
	c1, _ := db.InsertChunk(h1, "blob1", 5)
	c2, _ := db.InsertChunk(h2, "blob2", 5)
	s1, _ := db.CreateSnapshot(time.Now())
	fid, _ := db.UpsertFile(s1, FileRow{Path: "v.txt", Mode: 0o644, Size: 5})
	db.AttachChunks(fid, []ChunkRef{{ChunkID: c1, IDX: 0}})
	db.IncrRefcount(c1)

	// 覆盖：旧引用 c1 递减归零 -> 删除；c2 保持
	fid2, _ := db.UpsertFile(s1, FileRow{Path: "v.txt", Mode: 0o644, Size: 5})
	db.AttachChunks(fid2, []ChunkRef{{ChunkID: c2, IDX: 0}})
	db.IncrRefcount(c2)
	var n int
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM chunks WHERE id = ?`, c1).Scan(&n); err != nil || n != 0 {
		t.Fatalf("被覆盖的 chunk 行应删除: %d %v", n, err)
	}
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM chunks WHERE id = ?`, c2).Scan(&n); err != nil || n != 1 {
		t.Fatalf("新 chunk 应保留: %d %v", n, err)
	}
}

// TestGetFileRow：按 (snapshot_id, path) 单行查询（quick check 用）。
func TestGetFileRow(t *testing.T) {
	db := openTemp(t)
	now := time.Unix(1700000000, 0).UTC()
	s1, err := db.CreateSnapshot(now)
	if err != nil {
		t.Fatal(err)
	}
	want := FileRow{Path: "a/b.txt", Mode: 0o644, UID: 1000, GID: 1000, Size: 10, MTimeNs: now.UnixNano()}
	if _, err := db.UpsertFile(s1, want); err != nil {
		t.Fatal(err)
	}
	// 存在：字段完整一致
	got, ok, err := db.GetFileRow(s1, "a/b.txt")
	if err != nil || !ok {
		t.Fatalf("GetFileRow: ok=%v err=%v", ok, err)
	}
	if got.Path != want.Path || got.Mode != want.Mode || got.UID != want.UID ||
		got.GID != want.GID || got.Size != want.Size || got.MTimeNs != want.MTimeNs ||
		got.IsDir || got.IsSymlink {
		t.Fatalf("GetFileRow 字段不一致: %+v vs %+v", got, want)
	}
	// 目录行（is_dir=1）与符号链接行也按原样还原
	if _, err := db.UpsertFile(s1, FileRow{Path: "sub", IsDir: true, Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpsertFile(s1, FileRow{Path: "ln", IsSymlink: true, Mode: 0o777, LinkTarget: "a/b.txt"}); err != nil {
		t.Fatal(err)
	}
	d, ok, err := db.GetFileRow(s1, "sub")
	if err != nil || !ok || !d.IsDir || d.IsSymlink {
		t.Fatalf("目录行: ok=%v d=%+v err=%v", ok, d, err)
	}
	l, ok, err := db.GetFileRow(s1, "ln")
	if err != nil || !ok || !l.IsSymlink || l.LinkTarget != "a/b.txt" {
		t.Fatalf("链接行: ok=%v l=%+v err=%v", ok, l, err)
	}
	// 跨快照隔离：另一快照中不存在
	s2, _ := db.CreateSnapshot(now.Add(time.Minute))
	if _, ok, err := db.GetFileRow(s2, "a/b.txt"); err != nil || ok {
		t.Fatalf("快照 2 不应有 a/b.txt: ok=%v err=%v", ok, err)
	}
	// 不存在路径
	if _, ok, err := db.GetFileRow(s1, "nope.txt"); err != nil || ok {
		t.Fatalf("不存在路径: ok=%v err=%v", ok, err)
	}
}
