// internal/server/server_test.go
package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/webdav"

	"crysync/internal/config"
	"crysync/internal/crypto"
)

func TestServeEndToEnd(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "home.key")
	k, _ := crypto.GenerateKey()
	crypto.SaveKeyFile(keyPath, k)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cfg := &config.Config{
		Listen: fmt.Sprintf("127.0.0.1:%d", port),
		Auth:   config.AuthConfig{Users: map[string]string{"backup": "secret"}},
		Modules: []config.ModuleConfig{{
			Name: "home", Path: "/",
			Backend: config.BackendConfig{Type: "dir", Path: filepath.Join(dir, "data")},
			Keyfile: keyPath,
			Meta:    filepath.Join(dir, "home.db"),
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, cfg) }()
	time.Sleep(200 * time.Millisecond) // 等监听就绪

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "hello.txt"), []byte("e2e backup"), 0o644)
	pw := filepath.Join(dir, "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)
	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("端到端备份失败: %v\n%s", err, out)
	}

	// 错误密码必须失败
	pwBad := filepath.Join(dir, "pwbad")
	os.WriteFile(pwBad, []byte("wrong\n"), 0o600)
	cmd = exec.Command("rsync", "-a", "--password-file="+pwBad, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("错误密码应失败: %s", out)
	}

	// 未知模块必须失败
	cmd = exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::nope/")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("未知模块应失败: %s", out)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("Serve 退出异常: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve 未在取消后退出")
	}
}

var _ = bytes.Contains

// startDaemon 起一个完整 daemon（Dir 后端 + 认证），返回端口与配置。
func startDaemon(t *testing.T) (int, *config.Config) {
	t.Helper()
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "home.key")
	k, _ := crypto.GenerateKey()
	if err := crypto.SaveKeyFile(keyPath, k); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	cfg := &config.Config{
		Listen: fmt.Sprintf("127.0.0.1:%d", port),
		Auth:   config.AuthConfig{Users: map[string]string{"backup": "secret"}},
		Modules: []config.ModuleConfig{{
			Name: "home", Path: "/",
			Backend: config.BackendConfig{Type: "dir", Path: filepath.Join(dir, "data")},
			Keyfile: keyPath,
			Meta:    filepath.Join(dir, "home.db"),
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Error("Serve 未在取消后退出")
		}
	})
	time.Sleep(200 * time.Millisecond) // 等监听就绪
	return port, cfg
}

// rsyncRun 跑一条 rsync 命令（带密码文件），返回输出。
func rsyncRun(t *testing.T, port int, pwDir string, args ...string) ([]byte, error) {
	t.Helper()
	pw := filepath.Join(pwDir, "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)
	full := append([]string{"--password-file=" + pw, "--port", fmt.Sprint(port)}, args...)
	cmd := exec.Command("rsync", full...)
	return cmd.CombinedOutput()
}

// buildTree 造备份源：文件/目录/符号链接/空目录/中文名。
func buildTree(t *testing.T, src string) map[string]string {
	t.Helper()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.MkdirAll(filepath.Join(src, "emptydir"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello restore"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.bin"), bytes.Repeat([]byte{0xAB, 0xCD}, 100), 0o600)
	os.WriteFile(filepath.Join(src, "中文文件.txt"), []byte("中文内容"), 0o644)
	os.Symlink("a.txt", filepath.Join(src, "link1"))
	// 固定 mtime（秒对齐：flist mtime 秒精度）
	fixed := time.Date(2026, 8, 1, 12, 30, 45, 0, time.Local)
	for _, p := range []string{"a.txt", "sub/b.bin", "中文文件.txt"} {
		os.Chtimes(filepath.Join(src, p), fixed, fixed)
	}
	return map[string]string{
		"a.txt":     "hello restore",
		"sub/b.bin": string(bytes.Repeat([]byte{0xAB, 0xCD}, 100)),
		"中文文件.txt":  "中文内容",
	}
}

func assertRestored(t *testing.T, src, dest string, contents map[string]string) {
	t.Helper()
	for path, want := range contents {
		got, err := os.ReadFile(filepath.Join(dest, path))
		if err != nil {
			t.Fatalf("恢复缺少 %s: %v", path, err)
		}
		if string(got) != want {
			t.Fatalf("%s 内容不符: got %q want %q", path, got, want)
		}
		s, err := os.Stat(filepath.Join(src, path))
		if err != nil {
			t.Fatal(err)
		}
		d, err := os.Stat(filepath.Join(dest, path))
		if err != nil {
			t.Fatal(err)
		}
		if d.Mode().Perm() != s.Mode().Perm() {
			t.Fatalf("%s mode 不符: got %o want %o", path, d.Mode().Perm(), s.Mode().Perm())
		}
		if !d.ModTime().Equal(s.ModTime()) {
			t.Fatalf("%s mtime 不符: got %v want %v", path, d.ModTime(), s.ModTime())
		}
	}
	// 符号链接
	lt, err := os.Readlink(filepath.Join(dest, "link1"))
	if err != nil || lt != "a.txt" {
		t.Fatalf("符号链接恢复不符: %q %v", lt, err)
	}
	// 空目录
	if st, err := os.Stat(filepath.Join(dest, "emptydir")); err != nil || !st.IsDir() {
		t.Fatalf("空目录恢复失败: %v", err)
	}
}

// TestServeRestore 完整恢复链路：备份 -> 拉回（内容/mode/mtime/符号链接/空目录/
// 中文文件名）-> 修改后二次备份+二次拉回（增量恢复）-> --delete 拉取。
func TestServeRestore(t *testing.T) {
	port, _ := startDaemon(t)
	pwDir := t.TempDir()

	src := t.TempDir()
	contents := buildTree(t, src)
	if out, err := rsyncRun(t, port, pwDir, "-a", src+"/", "backup@127.0.0.1::home/"); err != nil {
		t.Fatalf("备份失败: %v\n%s", err, out)
	}

	// 首次拉回
	dest := t.TempDir()
	if out, err := rsyncRun(t, port, pwDir, "-a", "backup@127.0.0.1::home/", dest+"/"); err != nil {
		t.Fatalf("恢复失败: %v\n%s", err, out)
	}
	assertRestored(t, src, dest, contents)

	// 二次备份：修改一个文件 + 新增一个，再增量拉回（客户端本地已有旧文件，
	// generator 发真实块校验和与 basis 类型，覆盖 sums 读取与回显路径）
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello restore v2"), 0o644)
	os.WriteFile(filepath.Join(src, "new.txt"), []byte("brand new"), 0o644)
	fixed := time.Date(2026, 8, 1, 12, 30, 46, 0, time.Local)
	os.Chtimes(filepath.Join(src, "a.txt"), fixed, fixed)
	if out, err := rsyncRun(t, port, pwDir, "-a", src+"/", "backup@127.0.0.1::home/"); err != nil {
		t.Fatalf("二次备份失败: %v\n%s", err, out)
	}
	dest2 := t.TempDir()
	// 先放一份旧内容，模拟增量恢复的目标目录
	os.WriteFile(filepath.Join(dest2, "a.txt"), []byte("hello restore"), 0o644)
	if out, err := rsyncRun(t, port, pwDir, "-a", "backup@127.0.0.1::home/", dest2+"/"); err != nil {
		t.Fatalf("二次恢复失败: %v\n%s", err, out)
	}
	contents["a.txt"] = "hello restore v2"
	contents["new.txt"] = "brand new"
	assertRestored(t, src, dest2, contents)

	// --delete 拉取（覆盖 delete 模式 DONE 时序与 del stats 回显路径；v1 不清目标根，
	// 只验证不失败且文件正确）
	dest3 := t.TempDir()
	os.WriteFile(filepath.Join(dest3, "stale.txt"), []byte("stale"), 0o644)
	if out, err := rsyncRun(t, port, pwDir, "-a", "--delete", "backup@127.0.0.1::home/", dest3+"/"); err != nil {
		t.Fatalf("--delete 恢复失败: %v\n%s", err, out)
	}
	assertRestored(t, src, dest3, contents)
}

// TestServeRestoreNumericIDs：--numeric-ids 拉取（id list 跳过路径）。
func TestServeRestoreNumericIDs(t *testing.T) {
	port, _ := startDaemon(t)
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "f.txt"), []byte("numeric ids test"), 0o644)
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", src+"/", "backup@127.0.0.1::home/"); err != nil {
		t.Fatalf("备份失败: %v\n%s", err, out)
	}
	dest := t.TempDir()
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", "--numeric-ids", "backup@127.0.0.1::home/", dest+"/"); err != nil {
		t.Fatalf("--numeric-ids 恢复失败: %v\n%s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(dest, "f.txt"))
	if err != nil || string(got) != "numeric ids test" {
		t.Fatalf("内容不符: %q %v", got, err)
	}
}

// TestServeRestoreEmptySnapshot：备份空目录后拉回空 flist。
func TestServeRestoreEmptySnapshot(t *testing.T) {
	port, _ := startDaemon(t)
	src := t.TempDir()
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", src+"/", "backup@127.0.0.1::home/"); err != nil {
		t.Fatalf("空目录备份失败: %v\n%s", err, out)
	}
	dest := t.TempDir()
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", "backup@127.0.0.1::home/", dest+"/"); err != nil {
		t.Fatalf("空快照恢复失败: %v\n%s", err, out)
	}
	entries, err := os.ReadDir(dest)
	if err != nil || len(entries) != 0 {
		t.Fatalf("目标目录应保持为空: %v %v", entries, err)
	}
}

// TestServeRestoreSubdir：拉取模块子目录（flist 只含子树）。
func TestServeRestoreSubdir(t *testing.T) {
	port, _ := startDaemon(t)
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub", "deep"), 0o755)
	os.WriteFile(filepath.Join(src, "top.txt"), []byte("top"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "in.txt"), []byte("inside"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "deep", "d.txt"), []byte("deep"), 0o644)
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", src+"/", "backup@127.0.0.1::home/"); err != nil {
		t.Fatalf("备份失败: %v\n%s", err, out)
	}
	dest := t.TempDir()
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", "backup@127.0.0.1::home/sub/", dest+"/"); err != nil {
		t.Fatalf("子目录恢复失败: %v\n%s", err, out)
	}
	for _, want := range []string{"in.txt", "deep/d.txt"} {
		if _, err := os.Stat(filepath.Join(dest, want)); err != nil {
			t.Fatalf("子目录恢复缺少 %s: %v", want, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dest, "top.txt")); err == nil {
		t.Fatal("子目录恢复不应包含顶层文件")
	}
	// 单文件拉取
	dest2 := t.TempDir()
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", "backup@127.0.0.1::home/sub/deep/d.txt", dest2+"/"); err != nil {
		t.Fatalf("单文件恢复失败: %v\n%s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(dest2, "d.txt"))
	if err != nil || string(got) != "deep" {
		t.Fatalf("单文件内容不符: %q %v", got, err)
	}
}

// startWebDAVServer 起一个本地 WebDAV 测试服务器（x/net/webdav handler）。
func startWebDAVServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := &webdav.Handler{
		FileSystem: webdav.NewMemFS(),
		LockSystem: webdav.NewMemLS(),
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(ln)
	stop := func() {
		srv.Close()
		ln.Close()
	}
	t.Cleanup(stop)
	return fmt.Sprintf("http://%s", ln.Addr().String()), stop
}

// TestServeWebDAVBackend：webdav 后端 daemon 的完整备份+恢复链路。
func TestServeWebDAVBackend(t *testing.T) {
	davURL, _ := startWebDAVServer(t)
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "home.key")
	k, _ := crypto.GenerateKey()
	if err := crypto.SaveKeyFile(keyPath, k); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	cfg := &config.Config{
		Listen: fmt.Sprintf("127.0.0.1:%d", port),
		Modules: []config.ModuleConfig{{
			Name: "home", Path: "/",
			Backend: config.BackendConfig{Type: "webdav", URL: davURL},
			Keyfile: keyPath,
			Meta:    filepath.Join(dir, "home.db"),
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Serve(ctx, cfg)
	time.Sleep(200 * time.Millisecond)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("webdav e2e"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.bin"), bytes.Repeat([]byte{0x11}, 300), 0o600)
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", src+"/", "127.0.0.1::home/"); err != nil {
		t.Fatalf("webdav 备份失败: %v\n%s", err, out)
	}
	dest := t.TempDir()
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", "127.0.0.1::home/", dest+"/"); err != nil {
		t.Fatalf("webdav 恢复失败: %v\n%s", err, out)
	}
	for path, want := range map[string]string{"a.txt": "webdav e2e", "sub/b.bin": string(bytes.Repeat([]byte{0x11}, 300))} {
		got, err := os.ReadFile(filepath.Join(dest, path))
		if err != nil || string(got) != want {
			t.Fatalf("%s 恢复不符: %q %v", path, got, err)
		}
	}
}

// TestServeReadOnlyModule：只读模块可拉不可推。
func TestServeReadOnlyModule(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "ro.key")
	k, _ := crypto.GenerateKey()
	crypto.SaveKeyFile(keyPath, k)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	cfg := &config.Config{
		Listen: fmt.Sprintf("127.0.0.1:%d", port),
		Modules: []config.ModuleConfig{{
			Name: "ro", Path: "/", ReadOnly: true,
			Backend: config.BackendConfig{Type: "dir", Path: filepath.Join(dir, "data")},
			Keyfile: keyPath,
			Meta:    filepath.Join(dir, "ro.db"),
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Serve(ctx, cfg)
	time.Sleep(200 * time.Millisecond)

	pw := filepath.Join(dir, "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)
	// 推往只读模块必须失败
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "x.txt"), []byte("x"), 0o644)
	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "127.0.0.1::ro/")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("只读模块推送应失败: %s", out)
	}
}
