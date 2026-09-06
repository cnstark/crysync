// internal/front/webdav/lazy_open_test.go
// 懒打开自愈集成：daemon 先起、后端后起——启动时模块打开失败，
// 请求路径 503（Retry-After）→ 后端就绪 → 冷却过后自动恢复 200，
// key/meta 同步创建，根 PROPFIND 动态出现该模块。
package webdav

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/webdav"

	"crysync/internal/config"
)

// davDo 返回状态码、响应体与响应头（webdavDo 不回传 header，Retry-After 断言需要）。
func davDo(t *testing.T, method, url string, body []byte) (int, []byte, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, b, resp.Header
}

func TestWebDAVLazyOpenRecovery(t *testing.T) {
	dir := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bePort := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	modCfg := &config.ModuleConfig{
		Name: "home", Path: "/",
		Backend: config.BackendConfig{
			Type: "webdav",
			URL:  fmt.Sprintf("http://127.0.0.1:%d", bePort),
		},
		Keyfile: filepath.Join(dir, "home.key"),
		Meta:    filepath.Join(dir, "home.db"),
	}
	davLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	davPort := davLn.Addr().(*net.TCPAddr).Port
	davLn.Close()
	cfg := &config.Config{
		Front: config.FrontConfig{
			WebDAV: &config.WebDAVFrontConfig{Listen: fmt.Sprintf("127.0.0.1:%d", davPort)},
		},
		Modules: []config.ModuleConfig{*modCfg},
	}
	srv := New(cfg, nil)
	srv.openCooldown = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				t.Errorf("前端退出异常: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("前端未在取消后退出")
		}
	}()
	davBase := fmt.Sprintf("http://127.0.0.1:%d", davPort)
	deadline := time.Now().Add(10 * time.Second)
	for !dialOK(davPort) {
		if time.Now().After(deadline) {
			t.Fatal("webdav 前端 10s 内未就绪")
		}
		time.Sleep(50 * time.Millisecond)
	}

	code, _, hdr := davDo(t, http.MethodGet, davBase+"/home/", nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("后端未就绪应 503, got %d", code)
	}
	if hdr.Get("Retry-After") != "30" {
		t.Fatalf("503 应带 Retry-After: 30, got %q", hdr.Get("Retry-After"))
	}
	if _, err := os.Stat(modCfg.Keyfile); !os.IsNotExist(err) {
		t.Fatalf("后端未就绪时不应创建 key: %v", err)
	}
	code, body, _ := davDo(t, "PROPFIND", davBase+"/", nil)
	if code != http.StatusMultiStatus || bytes.Contains(body, []byte(`<D:href>/home/</D:href>`)) {
		t.Fatalf("未就绪模块不应出现在根列表: %d %s", code, body)
	}

	beRoot := filepath.Join(dir, "backend")
	if err := os.MkdirAll(beRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	beLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", bePort))
	if err != nil {
		t.Fatalf("重新绑定后端端口失败: %v", err)
	}
	beSrv := &http.Server{Handler: &webdav.Handler{
		FileSystem: webdav.Dir(beRoot),
		LockSystem: webdav.NewMemLS(),
	}}
	go beSrv.Serve(beLn) //nolint:errcheck // 测试服务器错误经断言可见
	defer beSrv.Close()

	deadline = time.Now().Add(5 * time.Second)
	for {
		code, body, _ = davDo(t, http.MethodGet, davBase+"/home/", nil)
		if code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("懒打开未在 5s 内恢复, 最后状态=%d body=%s", code, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := os.Stat(modCfg.Keyfile); err != nil {
		t.Fatalf("恢复后应自动创建 key: %v", err)
	}
	if _, err := os.Stat(modCfg.Meta); err != nil {
		t.Fatalf("恢复后应自动创建 meta: %v", err)
	}
	code, body, _ = davDo(t, "PROPFIND", davBase+"/", nil)
	if code != http.StatusMultiStatus || !bytes.Contains(body, []byte(`<D:href>/home/</D:href>`)) {
		t.Fatalf("恢复后模块应出现在根列表: %d %s", code, body)
	}
}
