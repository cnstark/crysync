// internal/backend/webdav_test.go
package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"sync/atomic"
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
	be, err := NewWebDAV(base, "", "", 2)
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
	be, _ := NewWebDAV(base, "", "", 2)
	if err := be.Put("abc", []byte("x")); err == nil {
		t.Fatal("无认证应失败")
	}
	// 错误密码 → 失败
	be2, _ := NewWebDAV(base, "backup", "wrong", 2)
	if err := be2.Put("abc", []byte("x")); err == nil {
		t.Fatal("错误密码应失败")
	}
	// 正确认证 → 成功
	be3, _ := NewWebDAV(base, "backup", "secret", 2)
	if err := be3.Put("abc", []byte("x")); err != nil {
		t.Fatalf("正确认证应成功: %v", err)
	}
	got, err := be3.Get("abc")
	if err != nil || !bytes.Equal(got, []byte("x")) {
		t.Fatalf("认证 Get: %q %v", got, err)
	}
}

func TestWebDAVBucketDepthsAndEscaping(t *testing.T) {
	base := startWebDAVServer(t, "")
	for _, depth := range []int{1, 2, 4} {
		be, err := NewWebDAV(base, "", "", depth)
		if err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("blob-%d name", depth)
		if err := be.Put(name, []byte(name)); err != nil {
			t.Fatalf("深度 %d Put: %v", depth, err)
		}
		got, err := be.Get(name)
		if err != nil || !bytes.Equal(got, []byte(name)) {
			t.Fatalf("深度 %d Get: %q %v", depth, got, err)
		}
		names, err := be.List()
		if err != nil {
			t.Fatalf("深度 %d List: %v", depth, err)
		}
		found := false
		for _, gotName := range names {
			if gotName == name {
				found = true
			}
		}
		if !found {
			t.Fatalf("深度 %d List 未返回逻辑名 %q: %v", depth, name, names)
		}
	}
}

func TestWebDAVInvalidURL(t *testing.T) {
	if _, err := NewWebDAV("ftp://host/x", "", "", 2); err == nil {
		t.Fatal("非 http(s) URL 应报错")
	}
	if _, err := NewWebDAV("://bad", "", "", 2); err == nil {
		t.Fatal("非法 URL 应报错")
	}
}

func TestWebDAVCollectionCache(t *testing.T) {
	var mkcols atomic.Int32
	dav := &webdav.Handler{FileSystem: webdav.NewMemFS(), LockSystem: webdav.NewMemLS()}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "MKCOL" {
			mkcols.Add(1)
		}
		dav.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	be, err := NewWebDAV(srv.URL, "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := be.Put("same-blob", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if got := mkcols.Load(); got != 2 {
		t.Fatalf("首次 Put 的 MKCOL 次数 = %d，期望 2", got)
	}
	if err := be.Put("same-blob", []byte("second")); err != nil {
		t.Fatal(err)
	}
	if got := mkcols.Load(); got != 2 {
		t.Fatalf("缓存命中后仍发送 MKCOL，累计次数 = %d", got)
	}
}

func TestWebDAVConcurrentFirstPut(t *testing.T) {
	base := startWebDAVServer(t, "")
	be, err := NewWebDAV(base, "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	const count = 8
	errCh := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := be.Put("same-new-blob", []byte("same-data")); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	got, err := be.Get("same-new-blob")
	if err != nil || !bytes.Equal(got, []byte("same-data")) {
		t.Fatalf("并发首次建桶后读取失败: got=%q err=%v", got, err)
	}
}

func TestWebDAVListIgnoresMisplacedBlob(t *testing.T) {
	base := startWebDAVServer(t, "")
	be, err := NewWebDAV(base, "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := be.Put("valid", []byte("valid")); err != nil {
		t.Fatal(err)
	}
	parts, _ := bucketParts("valid", 2)
	misplaced := "misplaced"
	for blobBelongsToBucket(misplaced, parts, 2) {
		misplaced += "x"
	}
	req, err := http.NewRequest(http.MethodPut, be.collectionURL(parts)+"/"+url.PathEscape(misplaced), bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("准备错误分桶对象失败: HTTP %d", resp.StatusCode)
	}

	names, err := be.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "valid" {
		t.Fatalf("List 应只返回严格布局中的 blob，得到 %v", names)
	}
}

func TestWebDAVMetaNamespace(t *testing.T) {
	base := startWebDAVServer(t, "")
	be, err := NewWebDAV(base, "", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	src := bytes.NewReader([]byte("encrypted meta"))
	if err := be.PutMetaContext(context.Background(), "one.cmeta", src, int64(src.Len())); err != nil {
		t.Fatal(err)
	}
	names, err := be.ListMetaContext(context.Background())
	if err != nil || len(names) != 1 || names[0] != "one.cmeta" {
		t.Fatalf("Meta List: %v %v", names, err)
	}
	r, err := be.GetMetaContext(context.Background(), "one.cmeta")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != "encrypted meta" {
		t.Fatalf("Meta 内容错误: %q", got)
	}
	dataNames, err := be.List()
	if err != nil || len(dataNames) != 0 {
		t.Fatalf("Meta 不应进入数据 blob List: %v %v", dataNames, err)
	}
}
