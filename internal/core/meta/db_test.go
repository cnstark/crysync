// internal/core/meta/db_test.go
package meta

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
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

// TestOpenRejectsOldSchema：v0.4.x 旧版库（含 snapshots 表）必须被 Open 明确
// 拒绝并提示删除重建——v0.5 删除快照且不迁移，静默建新表会造成两套清单并存。
func TestOpenRejectsOldSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	// 旧 schema 骨架：snapshots 表存在即判定为旧库
	if _, err := raw.Exec(`CREATE TABLE snapshots (id INTEGER PRIMARY KEY, created_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	raw.Close()

	_, err = Open(path)
	if err == nil {
		t.Fatal("旧版库应被拒绝")
	}
	if !strings.Contains(err.Error(), "v0.4.x") || !strings.Contains(err.Error(), "快照表") {
		t.Fatalf("错误应说明旧版库与原因: %v", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("错误应提示删除的路径: %v", err)
	}
}

func TestUpsertAndDeleteFile(t *testing.T) {
	db := openTemp(t)
	now := time.Unix(1700000000, 0).UTC()
	f := FileRow{Path: "a/b.txt", Mode: 0o644, Size: 10, MTimeNs: now.UnixNano()}
	fid, err := db.UpsertFile(f, nil)
	if err != nil || fid == 0 {
		t.Fatalf("UpsertFile: %v %d", err, fid)
	}
	// 同路径再次 upsert（原地覆盖）：仍单行
	if _, err := db.UpsertFile(f, nil); err != nil {
		t.Fatal(err)
	}
	files, err := db.GetFiles()
	if err != nil || len(files) != 1 {
		t.Fatalf("覆盖写入后应恰 1 行: %v %v", files, err)
	}
	if err := db.DeleteFile("a/b.txt"); err != nil {
		t.Fatal(err)
	}
	files, err = db.GetFiles()
	if err != nil || len(files) != 0 {
		t.Fatalf("删除后应为空: %v %v", files, err)
	}
	// 删除不存在路径：幂等无错（单一状态模型下重复删除无副作用）
	if err := db.DeleteFile("a/b.txt"); err != nil {
		t.Fatalf("重复删除应幂等: %v", err)
	}
}

func TestFileRowRoundtrip(t *testing.T) {
	db := openTemp(t)
	want := FileRow{
		Path: "sub/链接文件.txt", IsSymlink: true, Mode: 0o777,
		UID: 1000, GID: 1000, Size: 0, MTimeNs: 1700000000123456789,
		Xattrs: []byte{0x01, 0x02}, LinkTarget: "/etc/passwd",
	}
	db.UpsertFile(want, nil)
	got, _ := db.GetFiles()
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
	if err := db.SetMeta("schema_version", "5"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := db.GetMeta("schema_version")
	if err != nil || !ok || v != "5" {
		t.Fatalf("meta 读写失败: %q %v %v", v, ok, err)
	}
	// 覆盖写
	if err := db.SetMeta("schema_version", "5.1"); err != nil {
		t.Fatal(err)
	}
	v, _, _ = db.GetMeta("schema_version")
	if v != "5.1" {
		t.Fatalf("meta 覆盖写失败: %q", v)
	}
	// 缺失键
	if _, ok, err := db.GetMeta("nope"); err != nil || ok {
		t.Fatalf("缺失键: ok=%v err=%v", ok, err)
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

func TestAttachChunksAndRefcount(t *testing.T) {
	db := openTemp(t)
	var h [32]byte
	h[0] = 7
	cid, _ := db.InsertChunk(h, "blob", 50)
	fid, _ := db.UpsertFile(FileRow{Path: "f.txt", Mode: 0o644, Size: 150}, nil)
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
	fid, _ := db.UpsertFile(FileRow{Path: "a.txt", Mode: 0o644, Size: 13}, nil)
	db.AttachChunks(fid, []ChunkRef{{ChunkID: cid, IDX: 0}})
	db.IncrRefcount(cid)
	fid2, _ := db.UpsertFile(FileRow{Path: "b.txt", Mode: 0o644, Size: 13}, nil)
	db.AttachChunks(fid2, []ChunkRef{{ChunkID: cid, IDX: 0}})
	db.IncrRefcount(cid)

	// 删一个引用：refcount 2->1，chunk 保留
	if err := db.DeleteFile("a.txt"); err != nil {
		t.Fatal(err)
	}
	var rc int
	if err := db.db.QueryRow(`SELECT refcount FROM chunks WHERE id = ?`, cid).Scan(&rc); err != nil || rc != 1 {
		t.Fatalf("refcount 应为 1: %d %v", rc, err)
	}
	// 删第二个引用：refcount 归零 -> chunk 行删除
	if err := db.DeleteFile("b.txt"); err != nil {
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
	fid, _ := db.UpsertFile(FileRow{Path: "v.txt", Mode: 0o644, Size: 5}, nil)
	db.AttachChunks(fid, []ChunkRef{{ChunkID: c1, IDX: 0}})
	db.IncrRefcount(c1)

	// 覆盖：旧引用 c1 递减归零 -> 删除；c2 保持
	fid2, _ := db.UpsertFile(FileRow{Path: "v.txt", Mode: 0o644, Size: 5}, nil)
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

// TestGetFileChunks：按 path 查文件块引用（MovePath 复制引用用）。
func TestGetFileChunks(t *testing.T) {
	db := openTemp(t)
	fid, err := db.UpsertFile(FileRow{Path: "a.txt", Mode: 0o644, Size: 10}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 先建 chunk 行（AttachChunks 的 file_chunks.chunk_id 外键引用 chunks.id）
	var h1, h2 [32]byte
	h1[0], h2[0] = 1, 2
	c1, err := db.InsertChunk(h1, "blob1", 5)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := db.InsertChunk(h2, "blob2", 5)
	if err != nil {
		t.Fatal(err)
	}
	refs := []ChunkRef{{ChunkID: c1, IDX: 0}, {ChunkID: c2, IDX: 1}}
	if err := db.AttachChunks(fid, refs); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetFileChunks("a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ChunkID != c1 || got[0].IDX != 0 || got[1].ChunkID != c2 || got[1].IDX != 1 {
		t.Fatalf("chunks 不符: %+v", got)
	}
	// 不存在文件返回空
	got, err = db.GetFileChunks("nope.txt")
	if err != nil || len(got) != 0 {
		t.Fatalf("不存在文件应返回空: %+v, %v", got, err)
	}
}

func TestGetFileRow(t *testing.T) {
	db := openTemp(t)
	now := time.Unix(1700000000, 0).UTC()
	want := FileRow{Path: "a/b.txt", Mode: 0o644, UID: 1000, GID: 1000, Size: 10, MTimeNs: now.UnixNano()}
	if _, err := db.UpsertFile(want, nil); err != nil {
		t.Fatal(err)
	}
	// 存在：字段完整一致
	got, ok, err := db.GetFileRow("a/b.txt")
	if err != nil || !ok {
		t.Fatalf("GetFileRow: ok=%v err=%v", ok, err)
	}
	if got.Path != want.Path || got.Mode != want.Mode || got.UID != want.UID ||
		got.GID != want.GID || got.Size != want.Size || got.MTimeNs != want.MTimeNs ||
		got.IsDir || got.IsSymlink {
		t.Fatalf("GetFileRow 字段不一致: %+v vs %+v", got, want)
	}
	// 目录行（is_dir=1）与符号链接行也按原样还原
	if _, err := db.UpsertFile(FileRow{Path: "sub", IsDir: true, Mode: 0o755}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpsertFile(FileRow{Path: "ln", IsSymlink: true, Mode: 0o777, LinkTarget: "a/b.txt"}, nil); err != nil {
		t.Fatal(err)
	}
	d, ok, err := db.GetFileRow("sub")
	if err != nil || !ok || !d.IsDir || d.IsSymlink {
		t.Fatalf("目录行: ok=%v d=%+v err=%v", ok, d, err)
	}
	l, ok, err := db.GetFileRow("ln")
	if err != nil || !ok || !l.IsSymlink || l.LinkTarget != "a/b.txt" {
		t.Fatalf("链接行: ok=%v l=%+v err=%v", ok, l, err)
	}
	// 不存在路径
	if _, ok, err := db.GetFileRow("nope.txt"); err != nil || ok {
		t.Fatalf("不存在路径: ok=%v err=%v", ok, err)
	}
}
