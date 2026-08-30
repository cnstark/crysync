// internal/front/webdav/root_test.go
// 虚拟根处理器测试：挂载服务器根的 WebDAV 客户端应看到模块列表。
package webdav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestRoot 构造带认证的虚拟根处理器（模拟 buildMux 的装配形态）。
func newTestRoot(names ...string) http.Handler {
	return basicAuth(map[string]string{"backup": "secret"})(&rootHandler{moduleNames: names})
}

func TestRootPropfind(t *testing.T) {
	h := newTestRoot("backup", "test")

	// 未认证 -> 401 质询（客户端凭此弹认证框）
	req := httptest.NewRequest("PROPFIND", "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("未认证应 401 + 质询, got %d", rec.Code)
	}

	// 认证后 Depth 1 -> 207：根集合 + 各模块集合
	req = httptest.NewRequest("PROPFIND", "/", nil)
	req.SetBasicAuth("backup", "secret")
	req.Header.Set("Depth", "1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND 根应 207, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{`<D:href>/</D:href>`, `<D:href>/backup/</D:href>`, `<D:href>/test/</D:href>`, "<D:collection/>"} {
		if !strings.Contains(body, want) {
			t.Fatalf("PROPFIND 根响应缺 %q: %s", want, body)
		}
	}

	// Depth 0 -> 只有根集合，不含模块
	req = httptest.NewRequest("PROPFIND", "/", nil)
	req.SetBasicAuth("backup", "secret")
	req.Header.Set("Depth", "0")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND Depth 0 应 207, got %d", rec.Code)
	}
	body = rec.Body.String()
	if !strings.Contains(body, `<D:href>/</D:href>`) || strings.Contains(body, "test") {
		t.Fatalf("Depth 0 应只有根: %s", body)
	}
}

func TestRootOtherMethods(t *testing.T) {
	h := newTestRoot("test")

	// OPTIONS -> 200 + DAV 头（与模块 handler 的 1,2 级一致）
	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.SetBasicAuth("backup", "secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("DAV") != "1, 2" {
		t.Fatalf("OPTIONS 根应 200 + DAV:1,2, got %d %q", rec.Code, rec.Header().Get("DAV"))
	}

	// GET 根 -> 200，HTML 索引含模块链接
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("backup", "secret")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `href="/test/"`) {
		t.Fatalf("GET 根应 200 且含模块链接, got %d %q", rec.Code, rec.Body.String())
	}

	// 根是虚拟集合，写方法 -> 405
	for _, m := range []string{http.MethodPut, http.MethodDelete, "MKCOL", "COPY", "MOVE"} {
		req = httptest.NewRequest(m, "/", nil)
		req.SetBasicAuth("backup", "secret")
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("根上 %s 应 405, got %d", m, rec.Code)
		}
	}

	// 根下未知路径 -> 404
	req = httptest.NewRequest("PROPFIND", "/nope", nil)
	req.SetBasicAuth("backup", "secret")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("未知路径应 404, got %d", rec.Code)
	}
}
