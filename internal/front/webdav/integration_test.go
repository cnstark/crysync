// internal/front/webdav/integration_test.go
// 真实 daemon 双前端（rsync + webdav，Dir 后端）端到端跨前端一致性验收：
// rsync 备份 → WebDAV 下载比对；WebDAV 上传 → rsync 恢复比对；单份模式收敛。
package webdav

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"crysync/internal/config"
	"crysync/internal/core"
	"crysync/internal/core/crypto"
	"crysync/internal/front/rsync"
)

// startDualFront 起一个 daemon：rsync + webdav 双前端、Dir 后端、无认证。
// 返回 rsync 端口、webdav 基址、模块配置与关闭函数。
func startDualFront(t *testing.T) (rsyncPort int, davBase string, modCfg *config.ModuleConfig, stop func()) {
	t.Helper()
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "home.key")
	k, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := crypto.SaveKeyFile(keyPath, k); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rsyncPort = ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	ln, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	davPort := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	modCfg = &config.ModuleConfig{
		Name: "home", Path: "/",
		Backend: config.BackendConfig{Type: "dir", Path: filepath.Join(dir, "data")},
		Keyfile: keyPath,
		Meta:    filepath.Join(dir, "home.db"),
	}
	cfg := &config.Config{
		Front: config.FrontConfig{
			Rsync:  &config.RsyncFrontConfig{Listen: fmt.Sprintf("127.0.0.1:%d", rsyncPort)},
			WebDAV: &config.WebDAVFrontConfig{Listen: fmt.Sprintf("127.0.0.1:%d", davPort)},
		},
		Modules: []config.ModuleConfig{*modCfg},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 2)
	for _, f := range []interface{ Serve(context.Context) error }{
		rsync.New(cfg, nil), New(cfg, nil),
	} {
		go func(srv interface{ Serve(context.Context) error }) {
			errCh <- srv.Serve(ctx)
		}(f)
	}
	stop = func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				t.Errorf("前端退出异常: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("前端未在取消后退出")
		}
	}
	// 轮询探测双端口就绪（替代固定 sleep）：并发套件负载下 300ms 固定等待
	// 不可靠——webdav Serve 已改为先开模块后监听，连通即代表 mux 就绪。
	deadline := time.Now().Add(10 * time.Second)
	for !dialOK(rsyncPort) || !dialOK(davPort) {
		if time.Now().After(deadline) {
			t.Fatal("双前端 10s 内未就绪（探测 rsync/webdav 端口失败）")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return rsyncPort, fmt.Sprintf("http://127.0.0.1:%d", davPort), modCfg, stop
}

// dialOK 探测 127.0.0.1:port 的 TCP 监听是否可连通（200ms 单次超时）。
func dialOK(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// webdavDo 发送 WebDAV 请求并返回状态码与响应体（headers 为可选附加头）。
func webdavDo(t *testing.T, method, url string, body []byte, headers map[string]string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
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
	return resp.StatusCode, b
}

func TestWebDAVRsyncCrossFront(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("PATH 中无 rsync，跳过集成测试")
	}
	rsyncPort, davBase, modCfg, stop := startDualFront(t)
	defer stop()

	// 1) rsync 备份源目录
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "hello.txt"), []byte("hello webdav"), 0o644)
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "sub/deep.txt"), []byte("deep content"), 0o644)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600) // 无认证模块，密码文件仅满足 rsync 参数要求
	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(rsyncPort),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsync 备份失败: %v\n%s", err, out)
	}

	// 2) WebDAV GET 下载比对（跨前端一致性：rsync 写 → webdav 读）
	code, body := webdavDo(t, http.MethodGet, davBase+"/home/hello.txt", nil, nil)
	if code != http.StatusOK || string(body) != "hello webdav" {
		t.Fatalf("GET hello.txt 不符: %d %q", code, body)
	}
	code, body = webdavDo(t, http.MethodGet, davBase+"/home/sub/deep.txt", nil, nil)
	if code != http.StatusOK || string(body) != "deep content" {
		t.Fatalf("GET deep.txt 不符: %d %q", code, body)
	}
	// PROPFIND 列目录含 sub
	code, body = webdavDo(t, "PROPFIND", davBase+"/home/",
		[]byte(`<?xml version="1.0"?><propfind xmlns="DAV:"><prop><displayname/></prop></propfind>`),
		map[string]string{"Depth": "1"})
	if code != 207 || !bytes.Contains(body, []byte("sub")) {
		t.Fatalf("PROPFIND 不符: %d %s", code, body)
	}
	// 目录 GET -> 200 HTML 浏览（浏览器直接查看备份内容）
	code, body = webdavDo(t, http.MethodGet, davBase+"/home/", nil, nil)
	if code != http.StatusOK || !bytes.Contains(body, []byte("hello.txt")) {
		t.Fatalf("GET 模块根应 200 HTML 含文件链接, got %d %s", code, body)
	}
	// 服务器根（虚拟根）：PROPFIND / 返回模块集合列表（挂载根的客户端入口）
	code, body = webdavDo(t, "PROPFIND", davBase+"/",
		[]byte(`<?xml version="1.0"?><propfind xmlns="DAV:"><prop><displayname/></prop></propfind>`),
		map[string]string{"Depth": "1"})
	if code != 207 || !bytes.Contains(body, []byte(`<D:href>/home/</D:href>`)) {
		t.Fatalf("PROPFIND 服务器根应 207 且含 /home/ 集合: %d %s", code, body)
	}

	// 3) WebDAV PUT 上传（跨前端一致性：webdav 写 → rsync 读）
	code, body = webdavDo(t, http.MethodPut, davBase+"/home/upload.txt", []byte("uploaded via webdav"), nil)
	if code != http.StatusCreated {
		t.Fatalf("PUT 失败: %d %s", code, body)
	}
	// rsync 恢复拉回比对
	dst := t.TempDir()
	cmd = exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(rsyncPort),
		"backup@127.0.0.1::home/", dst+"/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsync 恢复失败: %v\n%s", err, out)
	}
	for name, want := range map[string]string{
		"hello.txt":    "hello webdav",
		"sub/deep.txt": "deep content",
		"upload.txt":   "uploaded via webdav",
	} {
		got, err := os.ReadFile(filepath.Join(dst, name))
		if err != nil {
			t.Fatalf("恢复缺失 %s: %v", name, err)
		}
		if string(got) != want {
			t.Fatalf("恢复内容不符 %s: %q != %q", name, got, want)
		}
	}

	// 4) WebDAV DELETE + MOVE + MKCOL（写即快照生效，rsync 恢复验证）
	code, _ = webdavDo(t, http.MethodDelete, davBase+"/home/upload.txt", nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("DELETE 失败: %d", code)
	}
	code, _ = webdavDo(t, "MOVE", davBase+"/home/hello.txt", nil,
		map[string]string{"Destination": davBase + "/home/moved.txt"})
	if code != http.StatusCreated && code != http.StatusNoContent {
		t.Fatalf("MOVE 失败: %d", code)
	}
	code, _ = webdavDo(t, "MKCOL", davBase+"/home/newdir", nil, nil)
	if code != http.StatusCreated {
		t.Fatalf("MKCOL 失败: %d", code)
	}
	dst2 := t.TempDir()
	cmd = exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(rsyncPort),
		"backup@127.0.0.1::home/", dst2+"/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("二次恢复失败: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(dst2, "upload.txt")); !os.IsNotExist(err) {
		t.Fatal("DELETE 后 upload.txt 应不存在")
	}
	if _, err := os.Stat(filepath.Join(dst2, "hello.txt")); !os.IsNotExist(err) {
		t.Fatal("MOVE 后 hello.txt 应不存在")
	}
	if _, err := os.Stat(filepath.Join(dst2, "moved.txt")); err != nil {
		t.Fatalf("MOVE 后 moved.txt 应存在: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst2, "newdir")); err != nil {
		t.Fatalf("MKCOL 后 newdir 应存在: %v", err)
	}

	// 5) 单份模式收敛（缺省 snapshot=false）：多次写后仓库恒一份快照
	mod, err := core.OpenModule(modCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mod.Close()
	n, err := mod.Repo.SnapshotCountForTest()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("单份模式应恒 1 个快照, got %d", n)
	}

	// 6) 幂等 MKCOL/DELETE（绿联 NAS 定制 restic fork 兼容）：已存在目录重复
	// MKCOL 必须 201（修复前 405 致 mkdirAll 重试风暴 1.5 小时、零 PUT blob）、
	// DELETE 不存在必须 204（修复前 404 同样被 fork 当致命错误），且幂等请求
	// 不得产生新快照（否则单份模式持续写放大 / 多快照模式无限膨胀）。
	before, err := mod.Repo.SnapshotCountForTest()
	if err != nil {
		t.Fatal(err)
	}
	code, _ = webdavDo(t, "MKCOL", davBase+"/home/newdir", nil, nil)
	if code != http.StatusCreated {
		t.Fatalf("重复 MKCOL 应幂等 201, got %d", code)
	}
	code, _ = webdavDo(t, http.MethodDelete, davBase+"/home/nonexistent", nil, nil)
	if code != http.StatusNoContent {
		t.Fatalf("DELETE 不存在应幂等 204, got %d", code)
	}
	n, err = mod.Repo.SnapshotCountForTest()
	if err != nil {
		t.Fatal(err)
	}
	if n != before {
		t.Fatalf("幂等请求不应产生快照: before=%d after=%d", before, n)
	}

	// 7) 404 不存在
	code, _ = webdavDo(t, http.MethodGet, davBase+"/home/nonexistent", nil, nil)
	if code != http.StatusNotFound {
		t.Fatalf("GET 不存在应 404, got %d", code)
	}
}
