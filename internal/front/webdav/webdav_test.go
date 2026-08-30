package webdav

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/net/webdav"

	"crysync/internal/core/meta"
)

// fakeStore 最小 FileStore 实现（只实现测试用方法）。
type fakeStore struct {
	rows map[string]meta.FileRow // path -> row（模拟活跃快照清单）
	data map[string]string       // path -> 内容
	sid  int64
}

func (f *fakeStore) ActiveSnapshotID() (int64, error) { return f.sid, nil }
func (f *fakeStore) GetFileRow(_ int64, path string) (meta.FileRow, bool, error) {
	row, ok := f.rows[path]
	return row, ok, nil
}
func (f *fakeStore) ListDir(_ int64, path string) ([]meta.FileRow, error) {
	var out []meta.FileRow
	prefix := path
	if prefix != "" {
		prefix += "/"
	}
	for p, row := range f.rows {
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		rest := p[len(prefix):]
		if rest == "" || strings.Contains(rest, "/") {
			continue
		}
		out = append(out, row)
	}
	return out, nil
}
func (f *fakeStore) OpenFile(_ int64, path string) (io.ReadSeekCloser, meta.FileRow, error) {
	row, ok := f.rows[path]
	if !ok {
		return nil, meta.FileRow{}, errors.New("not found")
	}
	return nopReadSeekCloser{strings.NewReader(f.data[path])}, row, nil
}

// 以下方法满足 core.FileStore 接口（测试未用到，提供空实现）。
func (f *fakeStore) SnapshotFileRows(int64, string) ([]meta.FileRow, error) {
	return nil, nil
}
func (f *fakeStore) StreamFile(int64, string, int32, io.Writer) (int64, [16]byte, error) {
	return 0, [16]byte{}, nil
}
func (f *fakeStore) ChunkSizeBytes() int { return 0 }

type nopReadSeekCloser struct{ *strings.Reader }

func (nopReadSeekCloser) Close() error { return nil }

// fakeWriter 记录写调用。
type fakeWriter struct {
	puts   map[string]string
	mkcols []string
	dels   []string
	moves  [][2]string
}

func (w *fakeWriter) PutFile(path string, mode uint32, mtimeNs int64, src io.Reader) error {
	b, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	w.puts[path] = string(b)
	return nil
}
func (w *fakeWriter) Mkcol(path string) error      { w.mkcols = append(w.mkcols, path); return nil }
func (w *fakeWriter) DeletePath(path string) error { w.dels = append(w.dels, path); return nil }
func (w *fakeWriter) MovePath(src, dst string) error {
	w.moves = append(w.moves, [2]string{src, dst})
	return nil
}

func newTestFS(store *fakeStore, writer *fakeWriter, readOnly bool) *moduleFS {
	return &moduleFS{store: store, writer: writer, readOnly: readOnly}
}

func TestPropfindGet(t *testing.T) {
	store := &fakeStore{
		rows: map[string]meta.FileRow{
			"sub":       {Path: "sub", IsDir: true, Mode: 0o40755},
			"sub/a.txt": {Path: "sub/a.txt", Mode: 0o644, Size: 5},
			"root.txt":  {Path: "root.txt", Mode: 0o644, Size: 3},
		},
		data: map[string]string{"sub/a.txt": "hello", "root.txt": "abc"},
	}
	h := &webdav.Handler{
		FileSystem: newTestFS(store, &fakeWriter{puts: map[string]string{}}, false),
		LockSystem: noopLockSystem{},
	}

	// GET 文件
	req := httptest.NewRequest(http.MethodGet, "/root.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "abc" {
		t.Fatalf("GET 不符: %d %q", rec.Code, rec.Body.String())
	}
	// GET 不存在 → 404
	req = httptest.NewRequest(http.MethodGet, "/nope", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET 不存在应 404, got %d", rec.Code)
	}
	// PROPFIND 根（depth 1）含 root.txt 与 sub
	body := strings.NewReader(`<?xml version="1.0"?><propfind xmlns="DAV:"><prop><displayname/></prop></propfind>`)
	req = httptest.NewRequest("PROPFIND", "/", body)
	req.Header.Set("Depth", "1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND 应 207, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "root.txt") || !strings.Contains(rec.Body.String(), "sub") {
		t.Fatalf("PROPFIND 响应缺条目: %s", rec.Body.String())
	}
	// HEAD 目录（根）：x/net/webdav 对集合（目录）的 GET/HEAD 返回 405（RFC 4918 §9.3 允许）
	req = httptest.NewRequest(http.MethodHead, "/", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK && rec.Code != http.StatusNoContent && rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("HEAD 根应成功, got %d", rec.Code)
	}
}

// TestGetSymlink 覆盖 review 缺陷修复：symlinkFile.Seek 完整语义，x/net/webdav 的
// http.ServeContent 依赖 Seek(0, io.SeekEnd) 取大小——旧实现返回 os.ErrInvalid 导致 500。
func TestGetSymlink(t *testing.T) {
	store := &fakeStore{rows: map[string]meta.FileRow{}, data: map[string]string{}}
	h := &webdav.Handler{
		FileSystem: newTestFS(store, &fakeWriter{puts: map[string]string{}}, false),
		LockSystem: noopLockSystem{},
	}
	store.rows["link.txt"] = meta.FileRow{
		Path: "link.txt", IsSymlink: true, Mode: 0o777, LinkTarget: "/tmp/real.txt",
	}
	req := httptest.NewRequest(http.MethodGet, "/link.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "/tmp/real.txt" {
		t.Fatalf("GET symlink 应 200 + target 文本, got %d %q", rec.Code, rec.Body.String())
	}
}

func TestWriteOperations(t *testing.T) {
	store := &fakeStore{rows: map[string]meta.FileRow{}, data: map[string]string{}}
	writer := &fakeWriter{puts: map[string]string{}}
	h := &webdav.Handler{
		FileSystem: newTestFS(store, writer, false),
		LockSystem: noopLockSystem{},
	}

	// MKCOL（根下创建，无父目录问题）
	req := httptest.NewRequest("MKCOL", "/sub", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("MKCOL 应 201, got %d", rec.Code)
	}
	// PUT 新文件（父目录 sub 需存在——openWrite 前置检查；把 sub 加进 store）
	store.rows["sub"] = meta.FileRow{Path: "sub", IsDir: true}
	req = httptest.NewRequest(http.MethodPut, "/sub/b.txt", strings.NewReader("content"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("PUT 应 201, got %d", rec.Code)
	}
	if writer.puts["sub/b.txt"] != "content" {
		t.Fatalf("PUT 内容不符: %q", writer.puts["sub/b.txt"])
	}
	// PUT 父目录缺失 → 409（os.ErrNotExist → x/net/webdav 映射）
	req = httptest.NewRequest(http.MethodPut, "/noparent/x.txt", strings.NewReader("x"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("PUT 父目录缺失应 409, got %d", rec.Code)
	}
	// DELETE
	store.rows["sub/b.txt"] = meta.FileRow{Path: "sub/b.txt", Mode: 0o644}
	req = httptest.NewRequest(http.MethodDelete, "/sub/b.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE 应 204, got %d", rec.Code)
	}
	if len(writer.dels) != 1 || writer.dels[0] != "sub/b.txt" {
		t.Fatalf("DELETE 记录不符: %+v", writer.dels)
	}
	// MOVE（Destination 必填）
	req = httptest.NewRequest("MOVE", "/sub/b.txt", nil)
	req.Header.Set("Destination", "/moved.txt")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated && rec.Code != http.StatusNoContent {
		t.Fatalf("MOVE 应 201/204, got %d", rec.Code)
	}
	if len(writer.moves) != 1 || writer.moves[0] != [2]string{"sub/b.txt", "moved.txt"} {
		t.Fatalf("MOVE 记录不符: %+v", writer.moves)
	}
}

func TestReadOnlyFS(t *testing.T) {
	store := &fakeStore{rows: map[string]meta.FileRow{}, data: map[string]string{}}
	h := &webdav.Handler{
		FileSystem: newTestFS(store, &fakeWriter{}, true),
		LockSystem: noopLockSystem{},
	}
	req := httptest.NewRequest(http.MethodPut, "/x.txt", strings.NewReader("x"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	// 适配层返回 os.ErrPermission；x/net/webdav 对 PUT 的非 NotExist 错误映射 404
	// ——read_only 的 403 由 readOnlyGuard 中间件层保障（本测试仅验证适配层拒绝写）
	if rec.Code == http.StatusCreated || rec.Code == http.StatusOK {
		t.Fatalf("只读适配层不应允许写, got %d", rec.Code)
	}
}

// TestPropfindEmptyDir 空目录 PROPFIND 必须 207：x/net/webdav walkFS 用
// Readdir(0) 且把任何 err 当错误；count<=0 时耗尽应返回 (nil, nil)
//（对照库内 memFile 语义），返回 io.EOF 会让空目录 PROPFIND 变 500
//（restic 等客户端反复探测空目录会卡死重试循环）。
func TestPropfindEmptyDir(t *testing.T) {
	store := &fakeStore{
		rows: map[string]meta.FileRow{
			"locks":  {Path: "locks", IsDir: true, Mode: 0o40755},
			"keys":   {Path: "keys", IsDir: true, Mode: 0o40755},
			"a.txt":  {Path: "a.txt", Mode: 0o644, Size: 3},
		},
		data: map[string]string{"a.txt": "abc"},
	}
	h := &webdav.Handler{
		FileSystem: newTestFS(store, &fakeWriter{puts: map[string]string{}}, false),
		LockSystem: noopLockSystem{},
	}
	body := strings.NewReader(`<?xml version="1.0"?><propfind xmlns="DAV:"><prop><displayname/></prop></propfind>`)
	// 空目录 locks（root 下两个空目录 + 一个文件，walkFS 会枚举空目录）
	req := httptest.NewRequest("PROPFIND", "/locks", body)
	req.Header.Set("Depth", "1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("空目录 PROPFIND 应 207, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<D:href>/locks/</D:href>") {
		t.Fatalf("空目录响应应含自身集合 href: %s", rec.Body.String())
	}
	// 根 Depth 1：两个空目录 + 文件都在
	req = httptest.NewRequest("PROPFIND", "/", body)
	req.Header.Set("Depth", "1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("根 PROPFIND 应 207, got %d: %s", rec.Code, rec.Body.String())
	}
	for _, want := range []string{"locks", "keys", "a.txt"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("根枚举缺 %q: %s", want, rec.Body.String())
		}
	}
}
