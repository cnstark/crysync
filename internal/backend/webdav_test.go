// internal/backend/webdav_test.go
package backend

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/webdav"
)

// startWebDAVServer 起一个本地 WebDAV 测试服务器（x/net/webdav handler），
// 返回根 URL。authenticate 非空时要求 Basic 认证。
func startWebDAVServer(t *testing.T, authenticate string) string {
	t.Helper()
	_ = filepath.Join // dir 模式暂不用（MemFS 已覆盖接口行为）
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	h := &webdav.Handler{
		FileSystem: webdav.NewMemFS(),
		LockSystem: webdav.NewMemLS(),
	}
	if authenticate != "" {
		user, pass, _ := splitAuth(authenticate)
		h.Prefix = "/"
		// 包一层 Basic 认证
		authHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if u, p, ok := r.BasicAuth(); !ok || u != user || p != pass {
				w.Header().Set("WWW-Authenticate", `Basic realm="crysync-test"`)
				http.Error(w, "auth required", http.StatusUnauthorized)
				return
			}
			h.ServeHTTP(w, r)
		})
		srv := &http.Server{Handler: authHandler}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
	} else {
		srv := &http.Server{Handler: h}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
	}
	time.Sleep(50 * time.Millisecond)
	return fmt.Sprintf("http://%s", ln.Addr().String())
}

func splitAuth(s string) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

func TestWebDAVRoundTrip(t *testing.T) {
	base := startWebDAVServer(t, "")
	be, err := NewWebDAV(base, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := be.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	data := []byte("encrypted blob content")
	if err := be.Put("aa11bb22cc33dd44", data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	names, err := be.List()
	if err != nil || len(names) != 1 || names[0] != "aa11bb22cc33dd44" {
		t.Fatalf("List: %v %v", names, err)
	}
	got, err := be.Get("aa11bb22cc33dd44")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get: %q %v", got, err)
	}
	if err := be.Delete("aa11bb22cc33dd44"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if names, _ := be.List(); len(names) != 0 {
		t.Fatalf("删除后应无 blob: %v", names)
	}
	// 404 删除幂等
	if err := be.Delete("aa11bb22cc33dd44"); err != nil {
		t.Fatalf("重复 Delete 应幂等: %v", err)
	}
	// 不存在的 Get 报错
	if _, err := be.Get("nope"); err == nil {
		t.Fatal("Get 不存在应报错")
	}
}

func TestWebDAVAuth(t *testing.T) {
	base := startWebDAVServer(t, "backup:secret")
	// 无认证 → 失败
	be, _ := NewWebDAV(base, "", "")
	if err := be.Put("abc", []byte("x")); err == nil {
		t.Fatal("无认证应失败")
	}
	// 错误密码 → 失败
	be2, _ := NewWebDAV(base, "backup", "wrong")
	if err := be2.Put("abc", []byte("x")); err == nil {
		t.Fatal("错误密码应失败")
	}
	// 正确认证 → 成功
	be3, _ := NewWebDAV(base, "backup", "secret")
	if err := be3.Put("abc", []byte("x")); err != nil {
		t.Fatalf("正确认证应成功: %v", err)
	}
	got, err := be3.Get("abc")
	if err != nil || !bytes.Equal(got, []byte("x")) {
		t.Fatalf("认证 Get: %q %v", got, err)
	}
}

func TestWebDAVInvalidURL(t *testing.T) {
	if _, err := NewWebDAV("ftp://host/x", "", ""); err == nil {
		t.Fatal("非 http(s) URL 应报错")
	}
	if _, err := NewWebDAV("://bad", "", ""); err == nil {
		t.Fatal("非法 URL 应报错")
	}
}
