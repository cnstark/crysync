package webdav

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBasicAuth(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := basicAuth(map[string]string{"backup": "secret"})(next)

	// 无凭据 → 401
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("未认证应 401 + WWW-Authenticate, got %d", rec.Code)
	}
	// 错误密码 → 401
	req.SetBasicAuth("backup", "wrong")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("错误密码应 401, got %d", rec.Code)
	}
	// 正确 → 放行
	req.SetBasicAuth("backup", "secret")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("正确凭据应放行, got %d", rec.Code)
	}
	// 空用户集 → 匿名放行
	h = basicAuth(nil)(next)
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("空用户集应匿名放行, got %d", rec.Code)
	}
}

func TestReadOnlyGuard(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := readOnlyGuard(true, next)
	// 写方法 → 403
	for _, m := range []string{http.MethodPut, http.MethodDelete, "MKCOL", "COPY", "MOVE", "PROPPATCH", "LOCK", "UNLOCK"} {
		req := httptest.NewRequest(m, "/home/a.txt", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s 应 403, got %d", m, rec.Code)
		}
	}
	// 读方法放行
	for _, m := range []string{http.MethodGet, http.MethodHead, "PROPFIND", "OPTIONS"} {
		req := httptest.NewRequest(m, "/home/a.txt", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s 应放行, got %d", m, rec.Code)
		}
	}
	// 可写模块不拦截
	h = readOnlyGuard(false, next)
	req := httptest.NewRequest(http.MethodPut, "/home/a.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("可写模块 PUT 应放行, got %d", rec.Code)
	}
}
