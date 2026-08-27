// internal/rsyncproto/logging_test.go
// 会话日志埋点测试：真实 rsync 客户端备份两次，断言 file/entry/session_start/
// session_done 事件与 method=quick_check/full/delta 字段。
package rsyncproto

import (
	"bufio"
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

	"crysync/internal/backend"
	"crysync/internal/config"
	"crysync/internal/crypto"
	"crysync/internal/meta"
	"crysync/internal/repo"
)

// startLoggedTestServer 同 startTestServer，但会话日志写入返回 buffer（TextHandler）。
func startLoggedTestServer(t *testing.T) (int, *bytes.Buffer) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	key, _ := crypto.GenerateKey()
	r := repo.New(db, backend.NewInMemory(), key, 64)
	module := &config.ModuleConfig{Name: "home", Path: "/", Snapshot: true}
	cfg := &config.Config{Auth: config.AuthConfig{Users: map[string]string{"backup": "secret"}},
		Modules: []config.ModuleConfig{*module}}

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				c.SetDeadline(time.Now().Add(2 * time.Minute))
				br := bufio.NewReader(c)
				if _, err := HandleModuleRequest(br, c, cfg, nil); err != nil {
					return
				}
				_ = RunReceiver(context.Background(), br, c, module, r, logger)
			}(conn)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port, logBuf
}

// startLoggedRouterServer 同 startLoggedTestServer 但走 RunSession 双向路由
// （备份 receiver / 恢复 sender），同一服务器可先备份再恢复。
func startLoggedRouterServer(t *testing.T) (int, *repo.Repo, *bytes.Buffer) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	key, _ := crypto.GenerateKey()
	r := repo.New(db, backend.NewInMemory(), key, 64)
	module := &config.ModuleConfig{Name: "home", Path: "/", Snapshot: true}
	cfg := &config.Config{Auth: config.AuthConfig{Users: map[string]string{"backup": "secret"}},
		Modules: []config.ModuleConfig{*module}}

	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				c.SetDeadline(time.Now().Add(2 * time.Minute))
				br := bufio.NewReader(c)
				if _, err := HandleModuleRequest(br, c, cfg, nil); err != nil {
					return
				}
				_ = RunSession(context.Background(), br, c, module, r, logger)
			}(conn)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port, r, logBuf
}

// TestSessionLogRestore 断言恢复会话日志（session_start dir=restore / entry_sent /
// file_sent / session_done files+bytes）。
func TestSessionLogRestore(t *testing.T) {
	port, _, logBuf := startLoggedRouterServer(t)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello restore"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.bin"), bytes.Repeat([]byte{0xAB}, 200), 0o644)
	os.Symlink("a.txt", filepath.Join(src, "link1"))
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

	log := logBuf.String()
	// 快照 4 条目（a.txt link1 sub sub/b.bin，快照不含顶层 "."）；bytes=13+200；200B@64B 块=4 refs
	for _, want := range [][]string{
		{"msg=session_start", "dir=restore", "entries=4", "argv="},
		{"msg=file_sent", "path=a.txt", "size=13", "chunks=1"},
		{"msg=file_sent", "path=sub/b.bin", "size=200", "chunks=4"},
		{"msg=entry_sent", "path=sub", "type=dir"},
		{"msg=entry_sent", "path=link1", "type=link", "target=a.txt"},
		{"msg=session_done", "dir=restore", "files=2", "bytes=213"},
	} {
		if findLine(log, want...) == "" {
			t.Fatalf("恢复日志缺少含全部字段 %v 的行\n完整日志:\n%s", want, log)
		}
	}
	// 恢复内容正确（日志之外的行为不回退）
	if got, _ := os.ReadFile(filepath.Join(dest, "a.txt")); string(got) != "hello restore" {
		t.Fatalf("恢复内容不符: %q", got)
	}
}

// findLine 返回日志中第一行同时包含全部期望字段的行（TextHandler 每行一条
// key=value 事件；按行匹配避免中间字段顺序干扰），找不到返回空。
func findLine(log string, wants ...string) string {
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

// TestSessionLogBackup 断言备份会话的完整日志事件序列（全量 + delta + quick check）。
func TestSessionLogBackup(t *testing.T) {
	port, logBuf := startLoggedTestServer(t)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello log"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.bin"), bytes.Repeat([]byte{0x01}, 1500), 0o644)
	os.Symlink("a.txt", filepath.Join(src, "link1"))
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	run := func() {
		t.Helper()
		cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
			src+"/", "backup@127.0.0.1::home/")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("rsync 备份失败: %v\n%s", err, out)
		}
	}
	run() // 首次：a.txt 全量、b.bin 全量、link1、sub 目录

	log1 := logBuf.String()
	// entries=5：rsync 发顶层 "." 条目；b.bin 1500B@64B 块=24 refs 但内容重复去重，
	// 全库仅 3 个新块（a.txt 1 + b.bin 2 种内容）
	for _, want := range [][]string{
		{"msg=session_start", "dir=backup", "entries=5"},
		{"msg=file", "path=a.txt", "size=9", "method=full", "literal=9", "chunks=1", "md5=ok"},
		{"msg=file", "path=sub/b.bin", "size=1500", "method=full", "chunks=24", "md5=ok"},
		{"msg=entry", "path=sub", "type=dir"},
		{"msg=entry", "path=link1", "type=link", "target=a.txt"},
		{"msg=session_done", "snapshot_id=1", "files=5", "transferred=2", "skipped=0",
			"dirs=1", "links=1", "chunks_stored=3"},
	} {
		if findLine(log1, want...) == "" {
			t.Fatalf("首次备份日志缺少含全部字段 %v 的行\n完整日志:\n%s", want, log1)
		}
	}
	if findLine(log1, "msg=session_start", "argv=") == "" {
		t.Fatal("session_start 应含客户端 argv")
	}

	// 二次：a.txt 改内容（12B < blength=700 无法形成匹配窗口 → delta 全 literal），
	// b.bin 追加 500B 0x01（1500→2000，前缀按 700B 块匹配）
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello log v2"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.bin"), bytes.Repeat([]byte{0x01}, 2000), 0o644)
	run()

	log2 := logBuf.String()
	for _, want := range [][]string{
		{"msg=file", "path=a.txt", "size=12", "method=delta", "matched=0", "literal=12", "chunks=1", "md5=ok"},
		{"msg=file", "path=sub/b.bin", "size=2000", "method=delta", "matched=1500", "literal=500", "chunks=32", "blength=700", "md5=ok"},
		{"msg=session_done", "snapshot_id=2", "files=5", "transferred=2", "skipped=0"},
	} {
		if findLine(log2, want...) == "" {
			t.Fatalf("二次备份日志缺少含全部字段 %v 的行\n完整日志:\n%s", want, log2)
		}
	}
	// 第三次：不动任何文件 → 全部 quick_check
	run()
	log3 := logBuf.String()
	if findLine(log3, "msg=file", "path=a.txt", "method=quick_check") == "" ||
		findLine(log3, "msg=file", "path=sub/b.bin", "method=quick_check") == "" {
		t.Fatalf("三次备份应全部 quick_check\n完整日志:\n%s", log3)
	}
	if line := findLine(log3, "msg=session_done", "snapshot_id=3"); !strings.Contains(line, "skipped=2") {
		t.Fatalf("三次备份 session_done 应 skipped=2: %q\n完整日志:\n%s", line, log3)
	}
}
