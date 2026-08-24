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
	"regexp"
	"strconv"
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
				if err := RunReceiver(context.Background(), br, c, module, r, nil); err != nil {
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

// parseStat 解析 rsync --stats 输出中的 "Key: N bytes"（输出在 stderr，
// CombinedOutput 已合并）。大数可能带千分位逗号（big_num 格式化），先剥离。
func parseStat(t *testing.T, stats, key string) int64 {
	t.Helper()
	re := regexp.MustCompile(regexp.QuoteMeta(key) + `: ([\d,]+) bytes`)
	m := re.FindStringSubmatch(stats)
	if m == nil {
		t.Fatalf("%s 未出现在 --stats 输出中:\n%s", key, stats)
	}
	v, err := strconv.ParseInt(strings.ReplaceAll(m[1], ",", ""), 10, 64)
	if err != nil {
		t.Fatalf("解析 %s: %v", key, err)
	}
	return v
}

// runRsyncStats 执行一次备份并返回 --stats 输出。
func runRsyncStats(t *testing.T, port int, pw, src string) string {
	t.Helper()
	cmd := exec.Command("rsync", "-a", "--stats", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("rsync 备份失败: %v\n%s", err, out)
	}
	return string(out)
}

// TestRsyncQuickCheckUnchanged：未变化二次备份 → transferred 0（quick check 生效）。
func TestRsyncQuickCheckUnchanged(t *testing.T) {
	port, r := startTestServer(t)
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	runRsyncStats(t, port, pw, src) // 首次
	stats := runRsyncStats(t, port, pw, src)
	if !strings.Contains(stats, "Number of regular files transferred: 0") {
		t.Fatalf("quick check 未生效（二次备份仍有传输）:\n%s", stats)
	}
	sid, _ := r.LatestSnapshotID()
	if sid != 2 {
		t.Fatalf("应有 2 个快照: %d", sid)
	}
	// 第二次快照文件仍在且内容继承正确
	var out bytes.Buffer
	if err := r.ReadFile(sid, "a.txt", &out); err != nil || out.String() != "hello" {
		t.Fatalf("读取 a.txt: %v %q", err, out.String())
	}
}

// TestRsyncDeltaPartialUpdate：大文件局部修改 → Matched > 0 且 Literal ≈ 修改量。
func TestRsyncDeltaPartialUpdate(t *testing.T) {
	port, r := startTestServer(t)
	src := t.TempDir()
	// 1MiB 确定性内容（多块）
	big := make([]byte, 1<<20)
	for i := range big {
		big[i] = byte(i * 31)
	}
	os.WriteFile(filepath.Join(src, "big.bin"), big, 0o644)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	runRsyncStats(t, port, pw, src) // 首次全量
	// 中间改 1KB
	mid := bytes.Repeat([]byte{0xAB}, 1024)
	copy(big[1<<19:], mid)
	os.WriteFile(filepath.Join(src, "big.bin"), big, 0o644)
	stats := runRsyncStats(t, port, pw, src)

	matched := parseStat(t, stats, "Matched data")
	literal := parseStat(t, stats, "Literal data")
	if matched == 0 {
		t.Fatalf("delta 未生效（Matched data = 0）:\n%s", stats)
	}
	if literal > 4096 {
		t.Fatalf("literal 数据异常（应 ≈1KB+块边界余量）: %d", literal)
	}
	// 内容读回与修改后源一致
	sid, _ := r.LatestSnapshotID()
	var out bytes.Buffer
	if err := r.ReadFile(sid, "big.bin", &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), big) {
		t.Fatal("重组内容与源不一致")
	}
}

// TestRsyncMixedSecondBackupDelta：增改混合二次备份（模块备份无 --delete 时
// 快照保留旧文件，故"删"不适用）：新增 + 修改 → 快照文件数正确、内容读回一致。
// 恢复方向一致性由 server_test.go TestServeRestore 回归覆盖（其二次备份在 delta
// 实现后自动走真实块校验和路径）。
func TestRsyncMixedSecondBackupDelta(t *testing.T) {
	port, r := startTestServer(t)
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("v1"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.bin"), bytes.Repeat([]byte{0x01}, 1500), 0o644)
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
	run() // 首次
	// 修改 a.txt（变大触发 delta）+ 新增 c.txt
	os.WriteFile(filepath.Join(src, "a.txt"), bytes.Repeat([]byte("v2 longer content "), 50), 0o644)
	os.WriteFile(filepath.Join(src, "c.txt"), []byte("new file"), 0o644)
	run() // 二次

	s2, _ := r.LatestSnapshotID()
	if s2 != 2 {
		t.Fatalf("应有 2 个快照: %d", s2)
	}
	files, _ := r.GetFilesForTest(s2)
	got := map[string]bool{}
	for _, f := range files {
		got[f.Path] = true
	}
	for _, want := range []string{"a.txt", "sub", "sub/b.bin", "c.txt"} {
		if !got[want] {
			t.Fatalf("二次快照缺少 %s: %v", want, files)
		}
	}
	// 修改后的 a.txt 内容读回正确（delta 重组）
	wantA := bytes.Repeat([]byte("v2 longer content "), 50)
	var out bytes.Buffer
	if err := r.ReadFile(s2, "a.txt", &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), wantA) {
		t.Fatal("a.txt 二次备份内容不一致")
	}
	// 未变化的 sub/b.bin 由 CopyFiles 继承（quick check 跳过）
	out.Reset()
	if err := r.ReadFile(s2, "sub/b.bin", &out); err != nil || !bytes.Equal(out.Bytes(), bytes.Repeat([]byte{0x01}, 1500)) {
		t.Fatalf("sub/b.bin 继承内容不一致: %v %d", err, out.Len())
	}
	// 第一次快照保持 3 个条目（a.txt + sub 目录 + sub/b.bin）
	files1, _ := r.GetFilesForTest(1)
	if len(files1) != 3 {
		t.Fatalf("首次快照应有 3 个条目: %v", files1)
	}
}
