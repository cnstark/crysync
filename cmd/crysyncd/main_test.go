// cmd/crysyncd/main_test.go
package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"crysync/internal/core/crypto"
)

// TestDaemonVersion：--version 打印注入的版本号后退出（无需有效配置文件）。
func TestDaemonVersion(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "crysyncd")
	build := exec.Command("go", "build", "-ldflags", "-X main.version=v9.9.9-test", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("构建失败: %v\n%s", err, out)
	}
	out, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("--version 应成功: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "v9.9.9-test") {
		t.Fatalf("输出应含注入版本: %s", out)
	}
}

func TestDaemonSmoke(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "crysyncd")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建失败: %v\n%s", err, out)
	}
	keyPath := filepath.Join(dir, "home.key")
	k, _ := crypto.GenerateKey()
	crypto.SaveKeyFile(keyPath, k)

	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	conf := filepath.Join(dir, "crysync.yaml")
	confContent := fmt.Sprintf("listen: 127.0.0.1:%d\nauth:\n  users:\n    backup: secret\nmodules:\n  - name: home\n    path: /\n    backend: { type: dir, path: %s }\n    keyfile: %s\n    meta: %s\n",
		port, filepath.Join(dir, "data"), keyPath, filepath.Join(dir, "home.db"))
	os.WriteFile(conf, []byte(confContent), 0o600)

	cmd := exec.Command(bin, "--config", conf)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()

	// 等待端口可连
	var ready bool
	for i := 0; i < 50; i++ {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatal("daemon 未就绪")
	}
	// 发送 greeting 检查
	c, _ := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	defer c.Close()
	buf := make([]byte, 32)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Read(buf)
	if err != nil || string(buf[:n]) != "@RSYNCD: 31.0 md5\n" {
		t.Fatalf("greeting 错误: %q %v", buf[:n], err)
	}
	// 关闭客户连接后再发 SIGTERM（优雅退出需等待在途连接结束，
	// 若连接仍开着，daemon 的 handleConn 会一直等待其读取完成而无法退出）。
	c.Close()
	// SIGTERM 优雅退出
	cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("daemon 未在 SIGTERM 后退出")
	}
}
