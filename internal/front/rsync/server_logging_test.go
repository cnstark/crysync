// internal/front/rsync/server_logging_test.go
// 日志注入测试：注入 buffer logger 启动 Serve，断言 daemon 级事件（daemon_start/
// conn_error）与会话级派生字段（module/client/session）贯穿备份与恢复。
package rsync

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crysync/internal/config"
	"crysync/internal/core/crypto"
)

// startLoggedDaemon 同 startDaemon 但注入 buffer logger。
func startLoggedDaemon(t *testing.T) (int, *bytes.Buffer) {
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
	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, cfg, logger) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Error("Serve 未在取消后退出")
		}
	})
	time.Sleep(200 * time.Millisecond) // 等监听就绪
	return port, logBuf
}

// findLogLine 返回日志中第一行同时包含全部期望字段的行，找不到返回空。
func findLogLine(log string, wants ...string) string {
	for _, line := range strings.Split(log, "\n") {
		ok := true
		for _, w := range wants {
			if !strings.Contains(line, w) {
				ok = false
				break
			}
		}
		if ok {
			return line
		}
	}
	return ""
}

// TestServeLogging 断言 daemon_start、会话级派生字段（module/client/session）、
// 备份与恢复事件、conn_error（未知模块）。
func TestServeLogging(t *testing.T) {
	port, logBuf := startLoggedDaemon(t)

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "hello.txt"), []byte("log e2e"), 0o644)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)
	run := func(args ...string) {
		t.Helper()
		full := append([]string{"-a", "--password-file=" + pw, "--port", fmt.Sprint(port)}, args...)
		if out, err := exec.Command("rsync", full...).CombinedOutput(); err != nil {
			t.Fatalf("rsync 失败: %v\n%s", err, out)
		}
	}
	run(src + "/", "backup@127.0.0.1::home/")

	dest := t.TempDir()
	run("backup@127.0.0.1::home/", dest+"/")

	// 未知模块：触发 conn_error（握手失败）
	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::nope/")
	_, _ = cmd.CombinedOutput()
	time.Sleep(100 * time.Millisecond) // 等连接 goroutine 记完日志

	log := logBuf.String()
	for _, want := range [][]string{
		{"msg=daemon_start", "listen=127.0.0.1:", "modules=1"},
		// 会话级字段贯穿全部事件
		{"msg=session_start", "dir=backup", "module=home", "client=127.0.0.1:", "session="},
		{"msg=file", "path=hello.txt", "method=full", "module=home", "session="},
		{"msg=session_done", "module=home", "snapshot_id=1"},
		{"msg=session_start", "dir=restore", "module=home", "session="},
		{"msg=file_sent", "path=hello.txt", "size=7", "module=home"},
		{"msg=session_done", "dir=restore", "module=home"},
		{"msg=conn_error", "err="},
	} {
		if findLogLine(log, want...) == "" {
			t.Fatalf("日志缺少含全部字段 %v 的行\n完整日志:\n%s", want, log)
		}
	}
	// 恢复内容正确
	if got, _ := os.ReadFile(filepath.Join(dest, "hello.txt")); string(got) != "log e2e" {
		t.Fatalf("恢复内容不符: %q", got)
	}
}
