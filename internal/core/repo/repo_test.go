// internal/core/repo/repo_test.go
package repo

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crysync/internal/backend"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
)

type gcBlockingBackend struct {
	*backend.InMemory
	uploadStarted chan struct{}
	releaseUpload chan struct{}
	listCalled    chan struct{}
}

func (b *gcBlockingBackend) PutContext(ctx context.Context, name string, data []byte) error {
	select {
	case <-b.uploadStarted:
	default:
		close(b.uploadStarted)
	}
	select {
	case <-b.releaseUpload:
		return b.InMemory.PutContext(ctx, name, data)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *gcBlockingBackend) List() ([]string, error) {
	select {
	case b.listCalled <- struct{}{}:
	default:
	}
	return b.InMemory.List()
}

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

func TestGCWaitsForWebDAVFileWrite(t *testing.T) {
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key, _ := crypto.GenerateKey()
	be := &gcBlockingBackend{
		InMemory:      backend.NewInMemory(),
		uploadStarted: make(chan struct{}),
		releaseUpload: make(chan struct{}),
		listCalled:    make(chan struct{}, 1),
	}
	r := New(db, be, key, 64)

	putDone := make(chan error, 1)
	go func() { putDone <- r.PutFile("inflight.bin", 0o644, 1, bytes.NewReader([]byte("data"))) }()
	select {
	case <-be.uploadStarted:
	case <-time.After(time.Second):
		t.Fatal("上传未进入后端写入阶段")
	}

	gcDone := make(chan error, 1)
	go func() { _, err := r.GC(); gcDone <- err }()
	select {
	case <-be.listCalled:
		t.Fatal("GC 不应在文件写入提交前扫描后端")
	case <-time.After(100 * time.Millisecond):
	}
	close(be.releaseUpload)
	if err := <-putDone; err != nil {
		t.Fatalf("文件写入失败: %v", err)
	}
	select {
	case <-be.listCalled:
	case <-time.After(time.Second):
		t.Fatal("上传完成后 GC 未开始扫描")
	}
	if err := <-gcDone; err != nil {
		t.Fatalf("GC 失败: %v", err)
	}
}

// TestListDir：当前清单中 path 的直接子项（非递归，不含自身）；path 为空 = 模块根。
func TestListDir(t *testing.T) {
	r, _ := newTestRepo(t)
	unlock := r.WriteSessionLock()
	r.UpsertFile(meta.FileRow{Path: "sub", IsDir: true, Mode: 0o40755}, nil)
	r.UpsertFile(meta.FileRow{Path: "sub/a.txt", Mode: 0o644, Size: 1}, nil)
	r.UpsertFile(meta.FileRow{Path: "sub/deep/b.txt", Mode: 0o644, Size: 2}, nil)
	r.UpsertFile(meta.FileRow{Path: "sub/deep", IsDir: true, Mode: 0o40755}, nil)
	r.UpsertFile(meta.FileRow{Path: "root.txt", Mode: 0o644, Size: 3}, nil)
	unlock()

	// 模块根：直接子项（不含子孙）
	rows, err := r.ListDir("")
	if err != nil || len(rows) != 2 {
		t.Fatalf("根子项应为 sub/root.txt, got %+v, %v", rows, err)
	}
	// 目录 sub：直接子项
	rows, err = r.ListDir("sub")
	if err != nil || len(rows) != 2 {
		t.Fatalf("sub 子项应为 a.txt/deep, got %+v, %v", rows, err)
	}
	// 文件路径：返回空
	rows, err = r.ListDir("root.txt")
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
	unlock := r.WriteSessionLock()
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
	r.UpsertFile(meta.FileRow{Path: "big.bin", Mode: 0o644, Size: int64(len(big))}, refs)
	unlock()

	fr, row, err := r.OpenFile("big.bin")
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
	if _, _, err := r.OpenFile("nonexistent"); err == nil {
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

// TestUpsertInPlace：同路径连续原地 upsert + 删除，单一状态收敛、refcount 闭环。
func TestUpsertInPlace(t *testing.T) {
	r, be := newTestRepo(t)
	unlock := r.WriteSessionLock()
	defer unlock()
	// 写入 2 个文件
	data := []byte("file-one-content")
	c1, _, _ := r.StoreChunk(data)
	if err := r.UpsertFile(meta.FileRow{Path: "one.txt", Mode: 0o644, Size: int64(len(data))}, []meta.ChunkRef{{ChunkID: c1, IDX: 0}}); err != nil {
		t.Fatal(err)
	}
	c2, _, _ := r.StoreChunk([]byte("file-two-content"))
	if err := r.UpsertFile(meta.FileRow{Path: "two.txt", Mode: 0o644, Size: 16}, []meta.ChunkRef{{ChunkID: c2, IDX: 0}}); err != nil {
		t.Fatal(err)
	}
	// 删除 one.txt + 覆盖 two.txt
	if err := r.DeleteFile("one.txt"); err != nil {
		t.Fatal(err)
	}
	c3, _, _ := r.StoreChunk([]byte("file-two-updated"))
	if err := r.UpsertFile(meta.FileRow{Path: "two.txt", Mode: 0o600, Size: 16}, []meta.ChunkRef{{ChunkID: c3, IDX: 0}}); err != nil {
		t.Fatal(err)
	}

	files, err := r.meta.GetFiles()
	if err != nil || len(files) != 1 || files[0].Path != "two.txt" {
		t.Fatalf("当前清单应只剩 two.txt: %v %v", files, err)
	}
	// one.txt 内容仍可经删除前快照读？——单一状态无历史：验证当前文件内容
	var buf bytes.Buffer
	if err := r.ReadFile("two.txt", &buf); err != nil || buf.String() != "file-two-updated" {
		t.Fatalf("读取 two.txt: %v %q", err, buf.String())
	}
	// 后端 blob = 3 个唯一内容；被删/被覆盖的 c1/c2 blob 成孤儿可回收
	blobs, _ := be.List()
	if len(blobs) != 3 {
		t.Fatalf("后端应有 3 个 blob: %v", blobs)
	}
	if n, err := r.GC(); err != nil || n != 2 {
		t.Fatalf("GC 应回收被覆盖/删除的 2 个 blob: %d %v", n, err)
	}
}

func TestReadFileMultiChunk(t *testing.T) {
	r, _ := newTestRepo(t)
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
	unlock := r.WriteSessionLock()
	r.UpsertFile(meta.FileRow{Path: "big.bin", Mode: 0o644, Size: int64(len(big))}, refs)
	unlock()

	var buf bytes.Buffer
	if err := r.ReadFile("big.bin", &buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), big) {
		t.Fatalf("多块读取不一致: got %d bytes, want %d", buf.Len(), len(big))
	}
}

func TestReadFileMissing(t *testing.T) {
	r, _ := newTestRepo(t)
	if err := r.ReadFile("nope.txt", &bytes.Buffer{}); err == nil {
		t.Fatal("读取不存在的文件应报错")
	}
}

// seedState 造一个含 3 个文件 2 层子目录的当前清单（供过滤/校验和测试复用）。
func seedState(t *testing.T, r *Repo) {
	t.Helper()
	unlock := r.WriteSessionLock()
	defer unlock()
	add := func(path string, data []byte, mode uint32) {
		t.Helper()
		c, _, err := r.StoreChunk(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.UpsertFile(meta.FileRow{Path: path, Mode: mode, Size: int64(len(data))},
			[]meta.ChunkRef{{ChunkID: c, IDX: 0}}); err != nil {
			t.Fatal(err)
		}
	}
	add("top.txt", []byte("top content"), 0o644)
	add("sub/inner.txt", []byte("inner content"), 0o644)
	add("sub/deep/deep.txt", []byte("deep content"), 0o600)
	if err := r.UpsertFile(meta.FileRow{Path: "sub", IsDir: true, Mode: 0o40755}, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.UpsertFile(meta.FileRow{Path: "sub/deep", IsDir: true, Mode: 0o40755}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestFileRowsFilter(t *testing.T) {
	r, _ := newTestRepo(t)
	seedState(t, r)

	all, err := r.FileRows("")
	if err != nil || len(all) != 5 {
		t.Fatalf("空前缀应返回全部: %d %v", len(all), err)
	}
	// 子目录前缀：自身 + 子树
	sub, err := r.FileRows("sub")
	if err != nil || len(sub) != 4 {
		t.Fatalf("子目录前缀应返回 4 条: %d %v", len(sub), err)
	}
	for _, f := range sub {
		if f.Path != "sub" && f.Path != "sub/inner.txt" && f.Path != "sub/deep" && f.Path != "sub/deep/deep.txt" {
			t.Fatalf("过滤结果超出子目录: %s", f.Path)
		}
	}
	// 单文件
	one, err := r.FileRows("top.txt")
	if err != nil || len(one) != 1 || one[0].Path != "top.txt" {
		t.Fatalf("单文件过滤: %v %v", one, err)
	}
}

// 前缀歧义断言独立于 seedState 的数据，直接在过滤结果上验证。
func TestFileRowsAmbiguousPrefix(t *testing.T) {
	r, _ := newTestRepo(t)
	unlock := r.WriteSessionLock()
	add := func(path string, data []byte) {
		c, _, _ := r.StoreChunk(data)
		r.UpsertFile(meta.FileRow{Path: path, Mode: 0o644, Size: int64(len(data))},
			[]meta.ChunkRef{{ChunkID: c, IDX: 0}})
	}
	add("x", []byte("x"))
	add("x/y", []byte("y"))
	add("x2", []byte("x2"))
	unlock()

	got, err := r.FileRows("x")
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
	seedState(t, r)

	content := []byte("top content")
	// 校验和 = 纯 MD5(content)（sum_end 的 CSUM_MD5 分支；seed 参数对 MD5 无效）
	var buf bytes.Buffer
	n, sum, err := r.StreamFile("top.txt", 0, &buf)
	if err != nil || n != int64(len(content)) {
		t.Fatalf("StreamFile: %v %d", err, n)
	}
	want := md5.Sum(content)
	if sum != want {
		t.Fatalf("校验和: got %x want %x", sum, want)
	}
	buf.Reset()
	_, sum, err = r.StreamFile("top.txt", 42, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if sum != want {
		t.Fatalf("seed 参数不应影响 MD5: got %x want %x", sum, want)
	}
	// 大文件跨块 + 空文件
	big := bytes.Repeat([]byte{0xAB}, 150)
	unlock := r.WriteSessionLock()
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
		r.UpsertFile(meta.FileRow{Path: path, Mode: 0o644, Size: int64(len(data))}, refs)
	}
	addFile("big.bin", big)
	addFile("empty.txt", nil)
	unlock()
	for _, tc := range []struct {
		path string
		want []byte
	}{
		{"big.bin", big},
		{"empty.txt", nil},
	} {
		buf.Reset()
		n, _, err := r.StreamFile(tc.path, 7, &buf)
		if err != nil {
			t.Fatalf("StreamFile(%s): %v", tc.path, err)
		}
		if !bytes.Equal(buf.Bytes(), tc.want) || n != int64(len(tc.want)) {
			t.Fatalf("StreamFile(%s) 内容不符: %d bytes", tc.path, n)
		}
	}
}

func TestGC(t *testing.T) {
	r, be := newTestRepo(t)
	// 正常状态 + 孤儿模拟：
	// 1) 引用中的 blob（正常落库）
	// 2) StoreChunk 后未 attach 的 chunk 行（失败会话残留：行+blob 都应回收）
	// 3) 直接写入后端的 blob（无任何元数据 = 孤儿）
	seedState(t, r)
	blobs1, _ := be.List()
	if len(blobs1) != 3 {
		t.Fatalf("seedState 应有 3 个 blob: %v", blobs1)
	}

	// 失败残留：chunk 行已建（refcount=0）但从未 attach 到文件——v0.5 GC
	// 必须把这类行连同 blob 一起回收（若以 chunks 表为引用判定会永久泄漏）
	c, _, _ := r.StoreChunk([]byte("orphan from failed session"))
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
	// 残留 chunk 行也应被清掉
	var n int
	if err := r.meta.QueryRow(`SELECT COUNT(*) FROM chunks`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("残留 chunk 行应被清除，剩 3 个: %d %v", n, err)
	}
	blobs, _ := be.List()
	if len(blobs) != 3 {
		t.Fatalf("GC 后应剩 3 个被引用 blob: %v", blobs)
	}
	// 清单仍可读（被引用 blob 未误删）
	var buf bytes.Buffer
	if err := r.ReadFile("top.txt", &buf); err != nil || buf.String() != "top content" {
		t.Fatalf("GC 后读取失败: %v %q", err, buf.String())
	}
	// 再次 GC 无操作
	if n, err := r.GC(); err != nil || n != 0 {
		t.Fatalf("二次 GC 应为 0: %d %v", n, err)
	}
}

func TestDirBucketBackendStoreReadAndGC(t *testing.T) {
	root := t.TempDir()
	db, err := meta.Open(filepath.Join(root, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	be, err := backend.NewDir(filepath.Join(root, "data"), 2)
	if err != nil {
		t.Fatal(err)
	}
	r := New(db, be, key, 64)

	content := []byte("content stored through bucketed dir backend")
	chunkID, _, err := r.StoreChunk(content)
	if err != nil {
		t.Fatal(err)
	}
	unlock := r.WriteSessionLock()
	err = r.UpsertFile(meta.FileRow{Path: "file.txt", Mode: 0o644, Size: int64(len(content))}, []meta.ChunkRef{{ChunkID: chunkID, IDX: 0}})
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := be.Put("direct-orphan", []byte("orphan")); err != nil {
		t.Fatal(err)
	}

	deleted, err := r.GC()
	if err != nil || deleted != 1 {
		t.Fatalf("GC 应删除一个孤儿 blob: deleted=%d err=%v", deleted, err)
	}
	var got bytes.Buffer
	if err := r.ReadFile("file.txt", &got); err != nil || !bytes.Equal(got.Bytes(), content) {
		t.Fatalf("分桶后端读取失败: got=%q err=%v", got.Bytes(), err)
	}
	if names, err := be.List(); err != nil || len(names) != 1 {
		t.Fatalf("GC 后应只保留一个被引用 blob: names=%v err=%v", names, err)
	}
}

// TestUpsertRefcount：覆盖写入同一路径时旧 chunk 引用递减、新 chunk 引用递增
// （chunk 行在 upsert 事务内随引用归零删除，blob 成孤儿）。
func TestUpsertRefcount(t *testing.T) {
	r, be := newTestRepo(t)
	unlock := r.WriteSessionLock()
	c1, _, _ := r.StoreChunk([]byte("version one"))
	r.UpsertFile(meta.FileRow{Path: "v.txt", Mode: 0o644, Size: 11}, []meta.ChunkRef{{ChunkID: c1, IDX: 0}})
	c2, _, _ := r.StoreChunk([]byte("version two longer"))
	r.UpsertFile(meta.FileRow{Path: "v.txt", Mode: 0o644, Size: 18}, []meta.ChunkRef{{ChunkID: c2, IDX: 0}})
	unlock()
	// c1 的引用被覆盖移除 -> chunk 行已删 -> blob 成孤儿
	n, err := r.GC()
	if err != nil || n != 1 {
		t.Fatalf("GC 应回收被覆盖的旧 blob: %d %v", n, err)
	}
	blobs, _ := be.List()
	if len(blobs) != 1 {
		t.Fatalf("后端应只剩新 blob: %v", blobs)
	}
	var buf bytes.Buffer
	if err := r.ReadFile("v.txt", &buf); err != nil || buf.String() != "version two longer" {
		t.Fatalf("覆盖后读取失败: %v %q", err, buf.String())
	}
}

// TestGetFileRow：repo 层包装的 quick check 查询（UpsertFile 后可查回）。
func TestGetFileRow(t *testing.T) {
	r, _ := newTestRepo(t)
	now := time.Unix(1700000000, 0).UTC()
	unlock := r.WriteSessionLock()
	want := meta.FileRow{Path: "a.txt", Mode: 0o644, Size: 5, MTimeNs: now.UnixNano()}
	if err := r.UpsertFile(want, nil); err != nil {
		t.Fatal(err)
	}
	unlock()
	got, ok, err := r.GetFileRow("a.txt")
	if err != nil || !ok {
		t.Fatalf("GetFileRow: ok=%v err=%v", ok, err)
	}
	if got.Size != 5 || got.MTimeNs != want.MTimeNs || got.Path != "a.txt" {
		t.Fatalf("字段不一致: %+v", got)
	}
	if _, ok, err := r.GetFileRow("nope.txt"); err != nil || ok {
		t.Fatalf("不存在路径: ok=%v err=%v", ok, err)
	}
}

// TestPutFileAndFriends：写方法族（PutFile/Mkcol/DeletePath/MovePath），
// v0.5 原地语义——每次写直接更新当前清单。
func TestPutFileAndFriends(t *testing.T) {
	r, _ := newTestRepo(t)
	now := time.Now().UnixNano()

	// 根文件 PUT
	if err := r.PutFile("a.txt", 0o644, now, strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	row, ok, err := r.GetFileRow("a.txt")
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
	// 重复 Mkcol 幂等成功（已存在即目标达成；绿联 restic fork 对 405 敏感，
	// 服务端不再返回 ErrExist——见 TestMkcolDeleteIdempotent 的快照数断言）
	if err := r.Mkcol("sub"); err != nil {
		t.Fatalf("重复 Mkcol 应幂等成功, got %v", err)
	}
	if err := r.PutFile("sub/b.txt", 0o644, now, strings.NewReader("xyz")); err != nil {
		t.Fatal(err)
	}

	// MovePath 文件（内容 chunk 引用保持）
	if err := r.MovePath("a.txt", "a2.txt"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := r.GetFileRow("a.txt"); ok {
		t.Fatal("移动后源应不存在")
	}
	fr, _, err := r.OpenFile("a2.txt")
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
	if _, ok, _ := r.GetFileRow("a2.txt"); ok {
		t.Fatal("删除后应不存在")
	}
	// DeletePath 目录（递归含 b.txt）
	if err := r.DeletePath("sub"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := r.GetFileRow("sub"); ok {
		t.Fatal("删除后 sub 应不存在")
	}
	if _, ok, _ := r.GetFileRow("sub/b.txt"); ok {
		t.Fatal("删除后 sub/b.txt 应不存在")
	}
	// DeletePath 不存在幂等成功（同 Mkcol 理由，绿联 restic fork 对 DELETE
	// 404 同样敏感；服务端不再返回 ErrNotExist）
	if err := r.DeletePath("nope"); err != nil {
		t.Fatalf("删除不存在应幂等成功, got %v", err)
	}
}

// TestMkcolDeleteIdempotent：幂等请求（重复 MKCOL 已存在目录、DELETE 不存在
// 路径）不产生任何写——清单行数与内容不变（绿联 restic fork 对 405/404 敏感，
// 幂等返回 nil 且不触碰后端）。
func TestMkcolDeleteIdempotent(t *testing.T) {
	r, be := newTestRepo(t)
	if err := r.Mkcol("data"); err != nil {
		t.Fatal(err)
	}
	if err := r.PutFile("data/a.txt", 0o644, time.Now().UnixNano(), strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	before, _ := be.List()

	// 幂等 MKCOL（目录已存在）与幂等 DELETE（不存在）都不应写
	if err := r.Mkcol("data"); err != nil {
		t.Fatalf("重复 Mkcol 应幂等成功, got %v", err)
	}
	if err := r.DeletePath("nope"); err != nil {
		t.Fatalf("删除不存在应幂等成功, got %v", err)
	}
	if got, _ := be.List(); len(got) != len(before) {
		t.Fatalf("幂等请求不应产生 blob: before=%d after=%d", len(before), len(got))
	}
	// 清单未受幂等请求影响
	if _, ok, _ := r.GetFileRow("data"); !ok {
		t.Fatal("幂等请求后 data 应仍在")
	}
	if _, ok, _ := r.GetFileRow("data/a.txt"); !ok {
		t.Fatal("幂等请求后 data/a.txt 应仍在")
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
		fr, _, err := r.OpenFile(p)
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

// failReader 输出部分数据后 mid-stream 返回错误（模拟请求体/源断流）。
type failReader struct {
	total int // 输出字节数上限
	err   error
	pos   int
}

func (r *failReader) Read(p []byte) (int, error) {
	if r.pos >= r.total {
		return 0, r.err
	}
	n := r.total - r.pos
	if n > len(p) {
		n = len(p)
	}
	r.pos += n
	return n, nil
}

// TestPutFileFailureCleanup：PutFile 中途失败（src mid-stream 报错）时，
// 已 StoreChunk 的新建 chunk 行（refcount=0）必须被清理——否则残留行让 GC
// 按 chunks 表判定其 blob 非孤儿而无法回收，形成泄漏。
func TestPutFileFailureCleanup(t *testing.T) {
	r, be := newTestRepo(t)
	now := time.Now().UnixNano()
	// 构造 2.5 块的源，读到中途报错
	errPut := errors.New("mid-stream failure")
	src := &failReader{total: r.ChunkSizeBytes()*2 + r.ChunkSizeBytes()/2, err: errPut}
	if err := r.PutFile("half.bin", 0o644, now, src); err == nil {
		t.Fatal("PutFile 应返回错误")
	}
	// 1) 失败不落库：files 表无行
	if files, err := r.meta.GetFiles(); err != nil || len(files) != 0 {
		t.Fatalf("失败后 files 表应为空: %v %v", files, err)
	}
	// 2) 新建的 chunk 行（refcount=0）必须清掉，不残留于 chunks 表
	var n int
	if err := r.meta.QueryRow(`SELECT COUNT(*) FROM chunks`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("chunks 表应无残留行, got %d, %v", n, err)
	}
	// 3) blob 已写后端但因 chunk 行已删而成为孤儿 → GC 可回收
	blobs, _ := be.List()
	if len(blobs) == 0 {
		t.Fatal("失败前应已写后端 blob（GC 待回收）")
	}
	deleted, err := r.GC()
	if err != nil || deleted != len(blobs) {
		t.Fatalf("GC 应回收全部失败残留 blob: deleted=%d want=%d err=%v", deleted, len(blobs), err)
	}
	if blobs, _ = be.List(); len(blobs) != 0 {
		t.Fatalf("GC 后后端应为空: %v", blobs)
	}
}

// TestMovePathOverwriteDir：目录移动覆盖已有目标子树时，dst 旧子树必须
// 整体删除（否则新旧混存），src 内容完整就位。
func TestMovePathOverwriteDir(t *testing.T) {
	r, _ := newTestRepo(t)
	now := time.Now().UnixNano()
	// src 目录 sub（含文件 sub/a.txt）
	if err := r.Mkcol("sub"); err != nil {
		t.Fatal(err)
	}
	if err := r.PutFile("sub/a.txt", 0o644, now, strings.NewReader("from-src")); err != nil {
		t.Fatal(err)
	}
	if err := r.PutFile("sub/deep.txt", 0o644, now, strings.NewReader("deep")); err != nil {
		t.Fatal(err)
	}
	// dst 目标已有目录 sub2（含旧文件 sub2/x.txt）
	if err := r.Mkcol("sub2"); err != nil {
		t.Fatal(err)
	}
	if err := r.PutFile("sub2/x.txt", 0o644, now, strings.NewReader("OLD-DST")); err != nil {
		t.Fatal(err)
	}
	// 把 sub 移动到 sub2（覆盖已有子树）
	if err := r.MovePath("sub", "sub2"); err != nil {
		t.Fatal(err)
	}
	// dst 原子文件（sub2/x.txt）应不存在——被覆盖删除
	if _, ok, _ := r.GetFileRow("sub2/x.txt"); ok {
		t.Fatal("覆盖后 dst 旧文件 sub2/x.txt 不应残留")
	}
	// src 内容就位：sub2/a.txt 与 sub2/deep.txt 迁移到位
	for path, want := range map[string]string{"sub2/a.txt": "from-src", "sub2/deep.txt": "deep"} {
		fr, _, err := r.OpenFile(path)
		if err != nil {
			t.Fatalf("打开 %s: %v", path, err)
		}
		got, _ := io.ReadAll(fr)
		fr.Close()
		if string(got) != want {
			t.Fatalf("%s 内容不符: got %q want %q", path, got, want)
		}
	}
	// src 子树在移动后消失
	if _, ok, _ := r.GetFileRow("sub"); ok {
		t.Fatal("src 源目录 sub 应已消失")
	}
	if _, ok, _ := r.GetFileRow("sub/a.txt"); ok {
		t.Fatal("src 源文件 sub/a.txt 应已消失")
	}
}

func TestConcurrentPutFileNoLostWrite(t *testing.T) {
	r, _ := newTestRepo(t)
	// 并发 8 个不同路径的 PUT：行更新段模块级短锁串行化后，8 个全在
	//（blob 上传在锁外并发，行更新互斥——见 PutFile 两阶段设计）。
	const n = 8
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := r.PutFile(fmt.Sprintf("f%d.txt", i), 0o644, time.Now().UnixNano(),
				strings.NewReader(fmt.Sprintf("content-%d", i))); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发 PUT 失败: %v", err)
	}
	for i := 0; i < n; i++ {
		row, ok, err := r.GetFileRow(fmt.Sprintf("f%d.txt", i))
		if err != nil || !ok {
			t.Fatalf("并发写丢失: f%d.txt 不在最终清单 (ok=%v err=%v)", i, ok, err)
		}
		if row.Size != int64(len(fmt.Sprintf("content-%d", i))) {
			t.Fatalf("f%d.txt size 不符: %d", i, row.Size)
		}
	}
}

func TestConcurrentMkcolPut(t *testing.T) {
	r, _ := newTestRepo(t)
	// MKCOL 与子文件 PUT 并发（restic init 的典型形态）：串行化后 PUT 若
	// 先于 MKCOL 拿锁，允许返回可重试的 os.ErrNotExist；不允许其他错误；
	// 客户端重试一次必须成功（父目录已在）。
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := r.Mkcol("data"); err != nil && !errors.Is(err, os.ErrExist) {
			errCh <- err
		}
	}()
	var putErr error
	go func() {
		defer wg.Done()
		putErr = r.PutFile("data/blob", 0o644, time.Now().UnixNano(), strings.NewReader("blob"))
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发 MKCOL 失败: %v", err)
	}
	if putErr != nil && !errors.Is(putErr, os.ErrNotExist) {
		t.Fatalf("PUT 失败须为可重试的 ErrNotExist, got %v", putErr)
	}
	// 模拟客户端重试：父目录已落库，重放必须成功
	if err := r.PutFile("data/blob", 0o644, time.Now().UnixNano(), strings.NewReader("blob")); err != nil {
		t.Fatalf("重试 PUT 失败: %v", err)
	}
	if _, ok, _ := r.GetFileRow("data"); !ok {
		t.Fatal("并发后 data 目录丢失")
	}
	if _, ok, _ := r.GetFileRow("data/blob"); !ok {
		t.Fatal("重试后 data/blob 仍不在清单")
	}
}

// --- Task 5：blob 并发上传专项 ---

// TestStoreChunkConcurrentDedup：8 goroutine 并发上传同一内容 → 后端只 1 个
// blob、返回同一 chunk id（in-flight 去重防双写，其余 7 个复用）。
func TestStoreChunkConcurrentDedup(t *testing.T) {
	r, be := newTestRepo(t)
	data := bytes.Repeat([]byte{0xCD}, 1000)
	const n = 8
	ids := make([]int64, n)
	reuseds := make([]bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, reused, err := r.StoreChunk(data)
			if err != nil {
				t.Errorf("StoreChunk: %v", err)
				return
			}
			ids[i], reuseds[i] = id, reused
		}(i)
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("并发同内容应返回同一 chunk id: %v", ids)
		}
	}
	blobs, _ := be.List()
	if len(blobs) != 1 {
		t.Fatalf("并发同内容后端应只 1 个 blob: %v", blobs)
	}
	// 恰好一个 !reused（首个上传者），其余 7 个复用
	first := 0
	for _, r2 := range reuseds {
		if !r2 {
			first++
		}
	}
	if first != 1 {
		t.Fatalf("应恰 1 个新建其余复用: %v", reuseds)
	}
}

// TestStoreChunkConcurrentDistinct：8 goroutine 并发上传不同内容 → 8 个 blob、
// 8 个不同 chunk id，全部可解密读回。
func TestStoreChunkConcurrentDistinct(t *testing.T) {
	r, be := newTestRepo(t)
	const n = 8
	datas := make([][]byte, n)
	ids := make([]int64, n)
	for i := range datas {
		datas[i] = bytes.Repeat([]byte{byte(i)}, 100+i*10)
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, reused, err := r.StoreChunk(datas[i])
			if err != nil || reused {
				t.Errorf("StoreChunk(%d): %v reused=%v", i, err, reused)
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()
	seen := map[int64]bool{}
	for _, id := range ids {
		if id == 0 || seen[id] {
			t.Fatalf("不同内容应各得不同 id: %v", ids)
		}
		seen[id] = true
	}
	blobs, _ := be.List()
	if len(blobs) != n {
		t.Fatalf("应 %d 个 blob, got %d", n, len(blobs))
	}
	// 全部 blob 可解密（内容寻址无错配）
	for _, name := range blobs {
		raw, err := be.Get(name)
		if err != nil {
			t.Fatal(err)
		}
		pt, err := r.key.Decrypt(raw, name)
		if err != nil || len(pt) == 0 {
			t.Fatalf("blob %s 解密失败: %v", name, err)
		}
	}
}

// TestPutFileConcurrentChunks：多块大文件（>4×chunkSize 触发 worker 池并行）
// 并发写入不同路径 → 读回与源逐字节一致（块顺序重组正确——并发结果按 idx
// 归位是正确性关键）。
func TestPutFileConcurrentChunks(t *testing.T) {
	r, _ := newTestRepo(t)
	now := time.Now().UnixNano()
	unlock := r.WriteSessionLock()
	for _, d := range []string{"d1", "d2", "d3"} {
		if err := r.UpsertFile(meta.FileRow{Path: d, IsDir: true, Mode: 0o40755, MTimeNs: now}, nil); err != nil {
			t.Fatal(err)
		}
	}
	unlock()

	big := make([]byte, r.ChunkSizeBytes()*6+r.ChunkSizeBytes()/3) // 6.33 块
	for i := range big {
		big[i] = byte(i * 7)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	for _, p := range []string{"d1/big.bin", "d2/big.bin", "d3/big.bin"} {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			if err := r.PutFile(p, 0o644, now, bytes.NewReader(big)); err != nil {
				errCh <- err
			}
		}(p)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发多块 PUT 失败: %v", err)
	}
	for _, p := range []string{"d1/big.bin", "d2/big.bin", "d3/big.bin"} {
		fr, row, err := r.OpenFile(p)
		if err != nil {
			t.Fatalf("打开 %s: %v", p, err)
		}
		if row.Size != int64(len(big)) {
			t.Fatalf("%s size 不符: %d", p, row.Size)
		}
		got, err := io.ReadAll(fr)
		fr.Close()
		if err != nil {
			t.Fatalf("读取 %s: %v", p, err)
		}
		if !bytes.Equal(got, big) {
			t.Fatalf("%s 内容与源不一致（块顺序重组错误）: %d bytes", p, len(got))
		}
	}
}

// --- 模块级上传并发上限 ---

// countingBackend 包装 InMemory，观测 backend.Put 的重叠并发（上限测试用）。
type countingBackend struct {
	backend.Backend
	mu    sync.Mutex
	cur   int
	max   int
	delay time.Duration // 每 Put 模拟网络耗时，放大重叠窗口
}

func (c *countingBackend) Put(name string, data []byte) error {
	c.mu.Lock()
	c.cur++
	if c.cur > c.max {
		c.max = c.cur
	}
	c.mu.Unlock()
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	err := c.Backend.Put(name, data)
	c.mu.Lock()
	c.cur--
	c.mu.Unlock()
	return err
}

func newCountingRepo(t *testing.T, delay time.Duration) (*Repo, *countingBackend) {
	t.Helper()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	key, _ := crypto.GenerateKey()
	cb := &countingBackend{Backend: backend.NewInMemory(), delay: delay}
	return New(db, cb, key, 64), cb
}

// TestUploadGateLimitsConcurrency：上限 1 时并发 StoreChunk 的后端 Put 段
// 完全串行（max 重叠 = 1）。
func TestUploadGateLimitsConcurrency(t *testing.T) {
	r, cb := newCountingRepo(t, 20*time.Millisecond)
	r.SetUploadConcurrency(1)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, _, err := r.StoreChunk(bytes.Repeat([]byte{byte(i + 1)}, 300)); err != nil {
				t.Errorf("StoreChunk: %v", err)
			}
		}(i)
	}
	wg.Wait()
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.max != 1 {
		t.Fatalf("上限 1 时后端 Put 并发应为 1, got %d", cb.max)
	}
	if blobs, _ := cb.List(); len(blobs) != 4 {
		t.Fatalf("4 个不同内容应全部上传: %d", len(blobs))
	}
}

// TestUploadGateUnlimited：不设上限时并发 StoreChunk 的后端 Put 重叠。
func TestUploadGateUnlimited(t *testing.T) {
	r, cb := newCountingRepo(t, 30*time.Millisecond)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, _, err := r.StoreChunk(bytes.Repeat([]byte{byte(i + 9)}, 300)); err != nil {
				t.Errorf("StoreChunk: %v", err)
			}
		}(i)
	}
	wg.Wait()
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.max < 2 {
		t.Fatalf("无上限时 Put 应可重叠, max=%d", cb.max)
	}
}

// TestUploadGateSharedAcrossRepos：同 meta DB 路径的两个 Repo 实例共享同一
// 闸门（跨连接限制——rsync 每连接一个实例、webdav 缓存一个）。
func TestUploadGateSharedAcrossRepos(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "meta.db")
	db1, err := meta.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	db2, err := meta.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	key, _ := crypto.GenerateKey()
	cb := &countingBackend{Backend: backend.NewInMemory(), delay: 20 * time.Millisecond}
	r1 := New(db1, cb, key, 64)
	r2 := New(db2, cb, key, 64)
	r1.SetUploadConcurrency(2)
	r2.SetUploadConcurrency(2) // 同 path 复用同容量闸门

	var wg sync.WaitGroup
	// 各 goroutine 不同内容（绕过 in-flight 去重），6 路上传争 2 槽位
	for i := 0; i < 6; i++ {
		wg.Add(1)
		r := r1
		if i >= 3 {
			r = r2
		}
		go func(i int) {
			defer wg.Done()
			if _, _, err := r.StoreChunk([]byte{byte(i), byte(i + 1), byte(i + 2)}); err != nil {
				t.Errorf("StoreChunk: %v", err)
			}
		}(i)
	}
	wg.Wait()
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.max > 2 {
		t.Fatalf("两实例共享上限 2: 实际重叠 %d", cb.max)
	}
}
