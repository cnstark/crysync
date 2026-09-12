package webdav

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/webdav"

	"crysync/internal/backend"
	"crysync/internal/core"
	"crysync/internal/core/meta"
)

func TestRequestTraceLogsLifecycle(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h := requestTrace(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s", r.Method)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("ok"))
	}), logger)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/home/file.txt", nil))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d", rec.Code)
	}
	got := logs.String()
	for _, want := range []string{"msg=webdav_request_start", "msg=webdav_request_end", "op=", "status=201", "response_bytes=2"} {
		if !strings.Contains(got, want) {
			t.Fatalf("日志缺少 %q: %s", want, got)
		}
	}
}

type contextTestWriter struct {
	fakeWriter
	started chan struct{}
	release chan struct{}
	err     error
}

func TestWritePutErrorReportsBackendFailure(t *testing.T) {
	rec := httptest.NewRecorder()
	err := fmt.Errorf("upload chunk: %w", &backend.WebDAVError{
		Method:     "MKCOL",
		StatusCode: http.StatusNotFound,
		RetryAfter: "12",
		Err:        errors.New("stale parent id"),
	})
	writePutError(rec, err)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("后端 404 应映射为 503，得到 %d", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "12" {
		t.Fatalf("Retry-After = %q，期望 12", got)
	}
	if got := rec.Header().Get("X-CrySync-Backend-Status"); got != "404" {
		t.Fatalf("后端状态头 = %q，期望 404", got)
	}
	if got := rec.Header().Get("X-CrySync-Backend-Operation"); got != "MKCOL" {
		t.Fatalf("后端操作头 = %q，期望 MKCOL", got)
	}
	if strings.Contains(rec.Body.String(), "stale parent id") {
		t.Fatalf("不应向客户端暴露上游错误细节: %q", rec.Body.String())
	}
}

func (w *contextTestWriter) PutFileContext(ctx context.Context, path string, mode uint32, mtimeNs int64, src io.Reader) (core.PutResult, error) {
	if w.err != nil {
		return core.PutResult{}, w.err
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(src, b); err != nil {
		return core.PutResult{}, err
	}
	close(w.started)
	select {
	case <-w.release:
	case <-ctx.Done():
		return core.PutResult{}, ctx.Err()
	}
	return core.PutResult{Size: 1, MTimeNs: mtimeNs}, nil
}

// fakeStore 最小 FileStore 实现（只实现测试用方法）。
type fakeStore struct {
	rows map[string]meta.FileRow // path -> row（模拟当前清单）
	data map[string]string       // path -> 内容
}

func (f *fakeStore) GetFileRow(path string) (meta.FileRow, bool, error) {
	row, ok := f.rows[path]
	return row, ok, nil
}
func (f *fakeStore) ListDir(path string) ([]meta.FileRow, error) {
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
func (f *fakeStore) OpenFile(path string) (io.ReadSeekCloser, meta.FileRow, error) {
	row, ok := f.rows[path]
	if !ok {
		return nil, meta.FileRow{}, errors.New("not found")
	}
	return nopReadSeekCloser{strings.NewReader(f.data[path])}, row, nil
}

// 以下方法满足 core.FileStore 接口（测试未用到，提供空实现）。
func (f *fakeStore) FileRows(string) ([]meta.FileRow, error) {
	return nil, nil
}
func (f *fakeStore) StreamFile(string, int32, io.Writer) (int64, [16]byte, error) {
	return 0, [16]byte{}, nil
}
func (f *fakeStore) ChunkSizeBytes() int { return 0 }

type nopReadSeekCloser struct{ *strings.Reader }

func (nopReadSeekCloser) Close() error { return nil }

// fakeWriter 记录写调用。
type fakeWriter struct {
	mu     sync.Mutex // 并发用例（TestPutNoStaleSnapshotRace）保护 map/切片写
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
	w.mu.Lock()
	defer w.mu.Unlock()
	w.puts[path] = string(b)
	return nil
}
func (w *fakeWriter) Mkcol(path string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.mkcols = append(w.mkcols, path)
	return nil
}
func (w *fakeWriter) DeletePath(path string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dels = append(w.dels, path)
	return nil
}
func (w *fakeWriter) MovePath(src, dst string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.moves = append(w.moves, [2]string{src, dst})
	return nil
}

func newTestFS(store *fakeStore, writer *fakeWriter, readOnly bool) *moduleFS {
	return &moduleFS{
		store:       store,
		writer:      writer,
		readOnly:    readOnly,
		rootName:    "test",
		rootModTime: time.Date(2026, time.September, 7, 0, 0, 0, 0, time.UTC),
	}
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
	// PROPFIND 文件系统根（depth 1）含根目录自身、root.txt 与 sub。x/net/webdav
	// 会刻意隐藏处理器根的 displayname，模块根的行为由下方专项测试覆盖。
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

func TestModuleRootPropfindMetadata(t *testing.T) {
	store := &fakeStore{rows: map[string]meta.FileRow{}, data: map[string]string{}}
	fs := newTestFS(store, &fakeWriter{puts: map[string]string{}}, false)
	fs.urlRoot = "/home"
	h := &webdav.Handler{FileSystem: fs, LockSystem: noopLockSystem{}}
	body := strings.NewReader(`<?xml version="1.0"?><propfind xmlns="DAV:"><prop><displayname/><getlastmodified/><resourcetype/><supportedlock/></prop></propfind>`)
	req := httptest.NewRequest("PROPFIND", "/home/", body)
	req.Header.Set("Depth", "0")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("模块根 PROPFIND 应 207, got %d", rec.Code)
	}
	for _, want := range []string{
		"<D:href>/home/</D:href>",
		"<D:displayname>test</D:displayname>",
		"<D:getlastmodified>Mon, 07 Sep 2026 00:00:00 GMT</D:getlastmodified>",
		"<D:collection xmlns:D=\"DAV:\"/>",
		"<D:supportedlock>",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("模块根 PROPFIND 响应缺 %q: %s", want, rec.Body.String())
		}
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

// TestIdempotentDelete 幂等 DELETE 中间件：不存在/存在的路径都 204（锁内
// 幂等判定，无 404——x/net/webdav 库层 Stat 前置检查固定 404，绿联 restic
// fork 对 404 敏感）；模块根删除拒绝；非 DELETE 透传。
func TestIdempotentDelete(t *testing.T) {
	writer := &fakeWriter{puts: map[string]string{}}
	var nextCalled bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})
	h := idempotentDelete(next, writer, "/home")

	// 存在/不存在路径都 204（fakeWriter.DeletePath 恒 nil）
	for _, p := range []string{"/home/a.txt", "/home/nope"} {
		req := httptest.NewRequest(http.MethodDelete, p, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("DELETE %s 应 204, got %d", p, rec.Code)
		}
	}
	if len(writer.dels) != 2 {
		t.Fatalf("DELETE 记录不符: %+v", writer.dels)
	}
	// 模块根拒绝删除
	req := httptest.NewRequest(http.MethodDelete, "/home/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE 模块根应 405, got %d", rec.Code)
	}
	// 非 DELETE 透传
	nextCalled = false
	req = httptest.NewRequest(http.MethodPut, "/home/a.txt", strings.NewReader("x"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !nextCalled {
		t.Fatal("PUT 应透传 next")
	}
}

// TestWriteOperations 写操作全流程（MKCOL/PUT/DELETE/MOVE）。
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
	// 409 判定下沉到 repo.PutFile 锁内 checkParentDir（适配层不做锁外预检）；
	// fakeWriter 不做
	// 父目录检查，真实行为由 repo 层测试与集成测试覆盖。
	if rec.Code != http.StatusCreated {
		t.Fatalf("PUT（fake 层无父目录检查）应 201, got %d", rec.Code)
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

func TestStreamingPutStatusAndETag(t *testing.T) {
	writer := &fakeWriter{puts: map[string]string{}}
	h := streamingPut(http.NotFoundHandler(), writer, "/home")
	req := httptest.NewRequest(http.MethodPut, "/home/a.txt", strings.NewReader("abc"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated || rec.Header().Get("ETag") == "" {
		t.Fatalf("旧 FileWriter PUT 应返回 201 和 ETag，得到 code=%d etag=%q", rec.Code, rec.Header().Get("ETag"))
	}

	bad := &fakeWriter{puts: map[string]string{}}
	badHandler := streamingPut(http.NotFoundHandler(), badWriter{fakeWriter: bad, err: os.ErrNotExist}, "/home")
	req = httptest.NewRequest(http.MethodPut, "/home/a.txt", strings.NewReader("abc"))
	rec = httptest.NewRecorder()
	badHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("父目录缺失应 409，得到 %d", rec.Code)
	}

	readOnly := readOnlyGuard(true, streamingPut(http.NotFoundHandler(), writer, "/home"))
	req = httptest.NewRequest(http.MethodPut, "/home/a.txt", strings.NewReader("abc"))
	rec = httptest.NewRecorder()
	readOnly.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("只读模块 PUT 应 403，得到 %d", rec.Code)
	}
}

type badWriter struct {
	*fakeWriter
	err error
}

func (w badWriter) PutFile(path string, mode uint32, mtimeNs int64, src io.Reader) error {
	return w.err
}

func TestStreamingPutStartsBeforeBodyEOF(t *testing.T) {
	w := &contextTestWriter{fakeWriter: fakeWriter{puts: map[string]string{}}, started: make(chan struct{}), release: make(chan struct{})}
	h := streamingPut(http.NotFoundHandler(), w, "/home")
	req := httptest.NewRequest(http.MethodPut, "/home/a.txt", strings.NewReader("first-and-more"))
	done := make(chan struct{})
	go func() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Errorf("PUT 应 201，得到 %d", rec.Code)
		}
		close(done)
	}()
	select {
	case <-w.started:
	case <-time.After(time.Second):
		t.Fatal("首块未在请求体 EOF 前交给写入器")
	}
	close(w.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("流式 PUT 未收口")
	}
}

// TestPropfindEmptyDir 空目录 PROPFIND 必须 207：x/net/webdav walkFS 用
// Readdir(0) 且把任何 err 当错误；count<=0 时耗尽应返回 (nil, nil)
// （对照库内 memFile 语义），返回 io.EOF 会让空目录 PROPFIND 变 500
// （restic 等客户端反复探测空目录会卡死重试循环）。
func TestPropfindEmptyDir(t *testing.T) {
	store := &fakeStore{
		rows: map[string]meta.FileRow{
			"locks": {Path: "locks", IsDir: true, Mode: 0o40755},
			"keys":  {Path: "keys", IsDir: true, Mode: 0o40755},
			"a.txt": {Path: "a.txt", Mode: 0o644, Size: 3},
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

// TestDirBrowse 目录 HTML 浏览中间件：GET/HEAD 目录渲染 HTML 文件列表，
// 文件/不存在/POST 透传给 webdav.Handler（对标 rclone/wsgidav dir_browser）。
func TestDirBrowse(t *testing.T) {
	store := &fakeStore{
		rows: map[string]meta.FileRow{
			"sub":       {Path: "sub", IsDir: true, Mode: 0o40755},
			"sub/a.txt": {Path: "sub/a.txt", Mode: 0o644, Size: 5},
			"中文.txt":    {Path: "中文.txt", Mode: 0o644, Size: 9},
			"root.txt":  {Path: "root.txt", Mode: 0o644, Size: 3},
		},
		data: map[string]string{"sub/a.txt": "hello", "root.txt": "abc", "中文.txt": "中文内容"},
	}
	var nextCalled bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		http.Error(w, "next-handler", http.StatusNotFound)
	})
	h := dirBrowse(next, store, "/test")

	// GET 模块根 -> HTML 列表（目录项带斜杠、文件项、中文文件名）
	req := httptest.NewRequest(http.MethodGet, "/test/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || nextCalled {
		t.Fatalf("目录 GET 应渲染 HTML, got %d next=%v", rec.Code, nextCalled)
	}
	body := rec.Body.String()
	for _, want := range []string{"sub/", "root.txt", "中文.txt"} {
		if !strings.Contains(body, want) {
			t.Fatalf("根列表缺 %q: %s", want, body)
		}
	}
	// 子目录 GET -> 含子项与父目录链接
	req = httptest.NewRequest(http.MethodGet, "/test/sub/", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "a.txt") {
		t.Fatalf("子目录列表不符: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "/test/") {
		t.Fatalf("子目录应有父目录/根链接: %s", rec.Body.String())
	}

	// 文件 GET -> 透传（下载交给 webdav.Handler）
	nextCalled = false
	req = httptest.NewRequest(http.MethodGet, "/test/root.txt", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !nextCalled {
		t.Fatal("文件 GET 应透传 next")
	}
	// 不存在路径 -> 透传（webdav.Handler 返回 404）
	nextCalled = false
	req = httptest.NewRequest(http.MethodGet, "/test/nope", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !nextCalled {
		t.Fatal("不存在路径应透传 next")
	}
	// 非 GET/HEAD（PROPFIND/PUT 等）-> 透传
	nextCalled = false
	req = httptest.NewRequest("PROPFIND", "/test/", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !nextCalled {
		t.Fatal("PROPFIND 应透传 next")
	}
}

// TestPutNoStaleSnapshotRace 并发写路径不再因状态预检而 409 风暴：
// v0.5 单一状态模型下无快照裁剪竞态，写入只需父目录存在（适配层不做
// 锁外清单预检，父目录校验在 repo 写锁内）——并发 PUT 全部收敛成功。
func TestPutNoStaleSnapshotRace(t *testing.T) {
	store := &fakeStore{rows: map[string]meta.FileRow{}, data: map[string]string{}}
	writer := &fakeWriter{puts: map[string]string{}}
	h := &webdav.Handler{
		FileSystem: newTestFS(store, writer, false),
		LockSystem: noopLockSystem{},
	}
	// 根下已有 data 目录（模拟 MKCOL 已提交）
	store.rows["data"] = meta.FileRow{Path: "data", IsDir: true, Mode: 0o40755}

	// 并发 8 个 PUT 到 data/ 下（fake 层路径：Handler 会做 Stat，目录须在）
	var wg sync.WaitGroup
	ok := int64(0)
	var mu sync.Mutex
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/data/f%d.txt", i), strings.NewReader(fmt.Sprintf("content-%d", i)))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			// fakeWriter 层的 PutFile 不做父目录检查（模拟修复后无锁外预检），
			// 此处只断言请求不因适配层预检而 4xx（fake 层 PUT 走 write 路径）
			if rec.Code != http.StatusCreated && rec.Code != http.StatusNoContent {
				mu.Lock()
				ok++
				mu.Unlock()
				t.Errorf("PUT f%d 失败: %d %s", i, rec.Code, rec.Body.String())
			}
		}(i)
	}
	wg.Wait()
	_ = ok
}
