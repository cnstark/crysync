package rsyncproto

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
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

// 起一个完整 daemon（握手+协商+receiver）监听随机端口，返回端口号。
func startTestServer(t *testing.T) (int, *repo.Repo) {
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
	be := backend.NewInMemory()
	r := repo.New(db, be, key, 64)
	module := &config.ModuleConfig{Name: "home", Path: "/"}
	cfg := &config.Config{Auth: config.AuthConfig{Users: map[string]string{"backup": "secret"}},
		Modules: []config.ModuleConfig{*module}}

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
				// 注意：服务端不预先发 greeting——rsync 客户端先发自己的版本行，
				// HandleModuleRequest 收到后回版本行（双向 greeting）。
				if _, err := HandleModuleRequest(br, c, cfg); err != nil {
					log.Printf("握手失败: %v", err)
					return
				}
				if err := RunReceiver(context.Background(), br, c, module, r); err != nil {
					log.Printf("会话失败: %v", err)
					return
				}
			}(conn)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port, r
}

func TestRsyncClientBackup(t *testing.T) {
	port, r := startTestServer(t)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello rsync"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.bin"), bytes.Repeat([]byte{0x01}, 200), 0o644)
	os.Symlink("a.txt", filepath.Join(src, "link1"))

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsync 备份失败: %v\n%s", err, out)
	}

	sid, err := r.LatestSnapshotID()
	if err != nil || sid == 0 {
		t.Fatalf("应产生快照: %d %v", sid, err)
	}
	files, _ := r.GetFilesForTest(sid)
	var buf strings.Builder
	for _, f := range files {
		buf.WriteString(f.Path)
		buf.WriteString(";")
	}
	if !strings.Contains(buf.String(), "a.txt") || !strings.Contains(buf.String(), "sub/b.bin") ||
		!strings.Contains(buf.String(), "link1") {
		t.Fatalf("快照文件清单不完整: %s", buf.String())
	}
	var out bytes.Buffer
	if err := r.ReadFile(sid, "a.txt", &out); err != nil || out.String() != "hello rsync" {
		t.Fatalf("读取 a.txt: %v %q", err, out.String())
	}
}

func TestRsyncClientSecondBackup(t *testing.T) {
	port, r := startTestServer(t)
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("v1"), 0o644)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	run := func() error {
		cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
			src+"/", "backup@127.0.0.1::home/")
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("%v: %s", err, out)
		}
		return nil
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	// 第二次：文件变化 + 新增一个
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("v2 changed"), 0o644)
	os.WriteFile(filepath.Join(src, "new.txt"), []byte("new"), 0o644)
	if err := run(); err != nil {
		t.Fatal(err)
	}
	s2, _ := r.LatestSnapshotID()
	files2, _ := r.GetFilesForTest(s2)
	if len(files2) != 2 {
		t.Fatalf("第二次快照应有 2 个文件: %v", files2)
	}
	// 第一次快照还在
	files1, _ := r.GetFilesForTest(s2 - 1)
	if len(files1) != 1 || files1[0].Path != "a.txt" {
		t.Fatalf("第一次快照应有 a.txt: %v", files1)
	}
}

// TestRsyncClientDeepSort 覆盖深层目录与同前缀歧义路径的 flist 排序：
// SortFlistEntries 用扁平字符串键（Path+"/"），而真实 rsync 的 f_name_cmp
// （flist.c:3217）逐路径分量比较。本用例跑真实 rsync 客户端备份，验证
// 排序对更深/歧义层次仍与真实客户端一致（排序不一致会导致传输阶段
// ndx 对不上而协议崩溃，或快照清单缺少条目）。
func TestRsyncClientDeepSort(t *testing.T) {
	port, r := startTestServer(t)

	src := t.TempDir()
	// 同前缀歧义：a.txt 与 a/ 目录、a-b 与 a/ 前缀竞争
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("top a.txt"), 0o644)
	os.MkdirAll(filepath.Join(src, "a"), 0o755)
	os.WriteFile(filepath.Join(src, "a", "b.txt"), []byte("nested a/b.txt"), 0o644)
	os.WriteFile(filepath.Join(src, "a-b"), []byte("ambiguous a-b"), 0o644)
	// 三层嵌套 + 歧义：x/y/z.txt 与 x/y2 前缀竞争（x/y2 非目录，仅名字相似）
	os.MkdirAll(filepath.Join(src, "x", "y"), 0o755)
	os.WriteFile(filepath.Join(src, "x", "y", "z.txt"), []byte("deep x/y/z.txt"), 0o644)
	os.WriteFile(filepath.Join(src, "x", "y2"), []byte("ambiguous x/y2"), 0o644)

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsync 备份失败: %v\n%s", err, out)
	}

	sid, err := r.LatestSnapshotID()
	if err != nil || sid == 0 {
		t.Fatalf("应产生快照: %d %v", sid, err)
	}
	files, _ := r.GetFilesForTest(sid)
	var buf strings.Builder
	for _, f := range files {
		buf.WriteString(f.Path)
		buf.WriteString(";")
	}
	list := buf.String()
	for _, want := range []string{
		"a.txt",     // 顶层文件
		"a",         // 顶层目录
		"a/b.txt",   // 单层嵌套
		"a-b",       // 与 a/ 同前缀歧义
		"x",         // 三层嵌套第一层
		"x/y",       // 三层嵌套第二层
		"x/y/z.txt", // 三层嵌套文件
		"x/y2",      // 与 x/y/ 前缀歧义
	} {
		if !strings.Contains(list, want+";") {
			t.Fatalf("快照缺少路径 %q，完整清单: %s", want, list)
		}
	}
	// 抽查深层文件内容可读回
	var out bytes.Buffer
	if err := r.ReadFile(sid, "x/y/z.txt", &out); err != nil || out.String() != "deep x/y/z.txt" {
		t.Fatalf("读取 x/y/z.txt: %v %q", err, out.String())
	}
}
