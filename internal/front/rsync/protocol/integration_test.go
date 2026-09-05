package protocol

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
	"syscall"
	"testing"
	"time"

	"crysync/internal/backend"
	"crysync/internal/config"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
	"crysync/internal/core/repo"
)

// 起一个完整 daemon（握手+协商+receiver）监听随机端口，返回端口号。
// v0.5 单一当前状态模型：每次成功会话原地更新当前清单（无快照历史）。
func startTestServer(t *testing.T) (int, *repo.Repo) {
	t.Helper()
	return startServerModule(t, &config.ModuleConfig{Name: "home", Path: "/"})
}

func startServerModule(t *testing.T, module *config.ModuleConfig) (int, *repo.Repo) {
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
	cfg := &config.Config{Front: config.FrontConfig{Rsync: &config.RsyncFrontConfig{Auth: config.AuthConfig{Users: map[string]string{"backup": "secret"}}}},
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
				if _, err := HandleModuleRequest(br, c, cfg, nil); err != nil {
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

	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	for _, f := range files {
		buf.WriteString(f.Path)
		buf.WriteString(";")
	}
	if !strings.Contains(buf.String(), "a.txt") || !strings.Contains(buf.String(), "sub/b.bin") ||
		!strings.Contains(buf.String(), "link1") {
		t.Fatalf("文件清单不完整: %s", buf.String())
	}
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "hello rsync" {
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
	// 第二次：文件变化 + 新增一个 → 单一当前状态原地收敛为 2 个文件
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("v2 changed"), 0o644)
	os.WriteFile(filepath.Join(src, "new.txt"), []byte("new"), 0o644)
	if err := run(); err != nil {
		t.Fatal(err)
	}
	files2, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files2) != 2 {
		t.Fatalf("当前清单应有 2 个文件: %v", files2)
	}
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "v2 changed" {
		t.Fatalf("读取 a.txt: %v %q", err, out.String())
	}
}

// TestSingleCopyInPlace：v0.5 单一当前状态端到端——真实 rsync 客户端多次
// 备份后当前清单原地收敛（首次 1 文件、二次 2 文件、无变化会话后不变），
// 内容读回一致。
func TestSingleCopyInPlace(t *testing.T) {
	port, r := startTestServer(t)
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("v1"), 0o644)
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
	run()
	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "a.txt" {
		t.Fatalf("首次备份后清单应恰 1 个文件: %v", files)
	}

	// 第二次：文件变化 + 新增一个 → 原地 upsert 收敛
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("v2 changed"), 0o644)
	os.WriteFile(filepath.Join(src, "new.txt"), []byte("new"), 0o644)
	run()
	files2, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files2) != 2 {
		t.Fatalf("二次备份后清单应恰 2 个文件: %v", files2)
	}
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "v2 changed" {
		t.Fatalf("读取 a.txt: %v %q", err, out.String())
	}

	// 第三次：无变化（纯 quick check）→ 清单不变、内容不变
	run()
	files3, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files3) != 2 {
		t.Fatalf("无变化会话后清单不应改变: %v", files3)
	}
	out.Reset()
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "v2 changed" {
		t.Fatalf("读取 a.txt: %v %q", err, out.String())
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

	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
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
	if err := r.ReadFile("x/y/z.txt", &out); err != nil || out.String() != "deep x/y/z.txt" {
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
	// 单一状态：文件仍在且内容不变
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "hello" {
		t.Fatalf("读取 a.txt: %v %q", err, out.String())
	}
	if files, _ := r.GetFilesForTest(); len(files) != 1 {
		t.Fatalf("清单应恰 1 个文件: %v", files)
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
	var out bytes.Buffer
	if err := r.ReadFile("big.bin", &out); err != nil {
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

	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
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
	if err := r.ReadFile("a.txt", &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), wantA) {
		t.Fatal("a.txt 二次备份内容不一致")
	}
	// 未变化的 sub/b.bin 由 quick check 跳过、内容保持不变
	out.Reset()
	if err := r.ReadFile("sub/b.bin", &out); err != nil || !bytes.Equal(out.Bytes(), bytes.Repeat([]byte{0x01}, 1500)) {
		t.Fatalf("sub/b.bin 继承内容不一致: %v %d", err, out.Len())
	}
}

// --- P0#1 preserve 选项矩阵回归（review 2026-08-24 #1）---

// TestRsyncBackupNoPreserve `rsync -r`（无 -og/-l）备份：客户端不发 uid/gid/symlink
// target 字段，服务端 Parse 需按 preserve 前提跳过（此前无条件读 → 死锁）。
// symlink 无 target：跳过落库并告警。
func TestRsyncBackupNoPreserve(t *testing.T) {
	port, r := startTestServer(t)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("no preserve"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("nested"), 0o644)
	os.Symlink("a.txt", filepath.Join(src, "link1"))

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-r", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsync -r 备份失败: %v\n%s", err, out)
	}

	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range files {
		got[f.Path] = true
	}
	// 文件与目录正常落库；link1 无 target 跳过
	for _, want := range []string{"a.txt", "sub", "sub/b.txt"} {
		if !got[want] {
			t.Fatalf("快照缺少 %s: %v", want, files)
		}
	}
	if got["link1"] {
		t.Fatalf("无 target 的 symlink 应跳过落库: %v", files)
	}
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "no preserve" {
		t.Fatalf("a.txt 内容: %v %q", err, out.String())
	}
}

// TestRsyncRestoreNoPreserve `rsync -r`（无 -og/-l）恢复：服务端 sender 不发
// uid/gid/symlink target 字段（此前无条件发 → 客户端解析错位乱码）。
func TestRsyncRestoreNoPreserve(t *testing.T) {
	port, _, _ := startLoggedRouterServer(t)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("restore content"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("nested"), 0o644)
	os.Symlink("a.txt", filepath.Join(src, "link1"))

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	// 先 -a 备份
	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("备份失败: %v\n%s", err, out)
	}
	// 无 preserve 恢复（rsync -r，无 -l/-og）
	dst := t.TempDir()
	cmd = exec.Command("rsync", "-r", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/", dst+"/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsync -r 恢复失败: %v\n%s", err, out)
	}
	data, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	if err != nil || string(data) != "restore content" {
		t.Fatalf("恢复 a.txt: %v %q", err, data)
	}
	data, err = os.ReadFile(filepath.Join(dst, "sub", "b.txt"))
	if err != nil || string(data) != "nested" {
		t.Fatalf("恢复 sub/b.txt: %v %q", err, data)
	}
}

// TestRsyncListOnlyNoRecurse `rsync --list-only`（无 -a/-r）：客户端隐含过滤为单层，
// 服务端只发顶层一层 flist（此前发整棵树 → rejecting unrequested file-list name）。
func TestRsyncListOnlyNoRecurse(t *testing.T) {
	port, _, _ := startLoggedRouterServer(t)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "docs", "deep"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("top level"), 0o644)
	os.WriteFile(filepath.Join(src, "docs", "readme.md"), []byte("second level"), 0o644)
	os.WriteFile(filepath.Join(src, "docs", "deep", "file.txt"), []byte("third"), 0o644)

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("备份失败: %v\n%s", err, out)
	}
	// --list-only 无 -r：只列一层
	cmd = exec.Command("rsync", "--list-only", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("rsync --list-only 失败: %v\n%s", err, out)
	}
	list := string(out)
	if !strings.Contains(list, "a.txt") || !strings.Contains(list, "docs") {
		t.Fatalf("列表应含顶层条目:\n%s", list)
	}
	if strings.Contains(list, "readme.md") || strings.Contains(list, "deep") {
		t.Fatalf("无 -r 时不应列出深层条目:\n%s", list)
	}
}

// TestRsyncBackupRL `rsync -rl`（有 l 无 -og）混合组合：symlink target 在 wire 上、
// uid/gid 不在。
func TestRsyncBackupRL(t *testing.T) {
	port, r := startTestServer(t)

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("rl test"), 0o644)
	os.Symlink("a.txt", filepath.Join(src, "link1"))

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-rl", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsync -rl 备份失败: %v\n%s", err, out)
	}

	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, f := range files {
		got[f.Path] = f.LinkTarget
	}
	if got["link1"] != "a.txt" {
		t.Fatalf("-rl 时 symlink target 应落库: %v", files)
	}
}

// TestRsyncBackupSubpathPrefix P0#2：子路径备份落库必须带前缀。
// `rsync -a src/ host::mod/sub1/` 的 flist 条目名相对传输根（a.txt 等），
// 落库路径应为 sub1/a.txt；两个子路径备份互不覆盖；quick check 基准也按
// 子路径前缀查询（二次备份子路径零传输，且不误命中其他子路径的旧文件）。
func TestRsyncBackupSubpathPrefix(t *testing.T) {
	port, r := startTestServer(t)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	src1 := t.TempDir()
	os.MkdirAll(filepath.Join(src1, "sub"), 0o755)
	os.WriteFile(filepath.Join(src1, "a.txt"), []byte("from sub1"), 0o644)
	os.WriteFile(filepath.Join(src1, "sub", "b.txt"), []byte("nested sub1"), 0o644)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src1+"/", "backup@127.0.0.1::home/sub1/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("子路径备份失败: %v\n%s", err, out)
	}

	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range files {
		got[f.Path] = true
	}
	if !got["sub1/a.txt"] || !got["sub1/sub/b.txt"] || !got["sub1/sub"] {
		t.Fatalf("子路径备份应带 sub1/ 前缀落库: %v", files)
	}
	if got["a.txt"] || got["sub/b.txt"] {
		t.Fatalf("子路径备份不应落库到模块根: %v", files)
	}
	var out bytes.Buffer
	if err := r.ReadFile("sub1/a.txt", &out); err != nil || out.String() != "from sub1" {
		t.Fatalf("读取 sub1/a.txt: %v %q", err, out.String())
	}

	// 第二个子路径：同名文件不同内容，互不覆盖
	src2 := t.TempDir()
	os.WriteFile(filepath.Join(src2, "a.txt"), []byte("from sub2"), 0o644)
	cmd = exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src2+"/", "backup@127.0.0.1::home/sub2/")
	if out2, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("第二个子路径备份失败: %v\n%s", err, out2)
	}
	files2, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	got2 := map[string]bool{}
	for _, f := range files2 {
		got2[f.Path] = true
	}
	if !got2["sub2/a.txt"] || !got2["sub1/a.txt"] {
		t.Fatalf("两个子路径应并存: %v", files2)
	}
	var out2 bytes.Buffer
	if err := r.ReadFile("sub1/a.txt", &out2); err != nil || out2.String() != "from sub1" {
		t.Fatalf("sub1/a.txt 被子路径备份覆盖: %v %q", err, out2.String())
	}

	// quick check 基准按前缀查询：再次备份 sub1（内容不变）零传输
	cmd = exec.Command("rsync", "-a", "--stats", "--password-file="+pw, "--port", fmt.Sprint(port),
		src1+"/", "backup@127.0.0.1::home/sub1/")
	outB, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sub1 二次备份失败: %v\n%s", err, outB)
	}
	if !strings.Contains(string(outB), "Number of regular files transferred: 0") {
		t.Fatalf("子路径 quick check 未生效（二次备份仍有传输）:\n%s", outB)
	}
}

// TestRsyncSubpathRestore P0#2 端到端：子路径备份后从同一子路径恢复。
func TestRsyncSubpathRestore(t *testing.T) {
	port, _, _ := startLoggedRouterServer(t)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("subpath content"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("nested"), 0o644)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/sub1/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("子路径备份失败: %v\n%s", err, out)
	}

	dst := t.TempDir()
	cmd = exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/sub1/", dst+"/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("子路径恢复失败: %v\n%s", err, out)
	}
	data, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	if err != nil || string(data) != "subpath content" {
		t.Fatalf("恢复 a.txt: %v %q", err, data)
	}
	data, err = os.ReadFile(filepath.Join(dst, "sub", "b.txt"))
	if err != nil || string(data) != "nested" {
		t.Fatalf("恢复 sub/b.txt: %v %q", err, data)
	}
}

// TestRsyncSubpathDelta 子路径备份的 delta 增量：修改子路径下大文件后二次备份，
// quick check/delta 基准必须按子路径前缀查询（basis 读取也按前缀路径）。
func TestRsyncSubpathDelta(t *testing.T) {
	port, r := startTestServer(t)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	src := t.TempDir()
	big := make([]byte, 1<<20)
	for i := range big {
		big[i] = byte(i * 31)
	}
	os.WriteFile(filepath.Join(src, "big.bin"), big, 0o644)

	run := func() string {
		cmd := exec.Command("rsync", "-a", "--stats", "--password-file="+pw, "--port", fmt.Sprint(port),
			src+"/", "backup@127.0.0.1::home/sub1/")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("子路径备份失败: %v\n%s", err, out)
		}
		return string(out)
	}
	run() // 首次全量

	// 局部修改 → delta
	mid := bytes.Repeat([]byte{0xAB}, 1024)
	copy(big[1<<19:], mid)
	os.WriteFile(filepath.Join(src, "big.bin"), big, 0o644)
	stats := run()

	if !strings.Contains(stats, "Number of regular files transferred: 1") {
		t.Fatalf("修改后应有 1 个文件传输:\n%s", stats)
	}
	matched := parseStat(t, stats, "Matched data")
	if matched < 900_000 {
		t.Fatalf("delta 匹配字节过少: %d\n%s", matched, stats)
	}
	var out bytes.Buffer
	if err := r.ReadFile("sub1/big.bin", &out); err != nil || !bytes.Equal(out.Bytes(), big) {
		t.Fatalf("读取 sub1/big.bin: %v (len=%d want=%d)", err, out.Len(), len(big))
	}
}

// TestRsyncCompressRejected P0#3：-z 压缩会话被明确拒绝（此前压缩 token 流被当
// 普通流解析 → "字面量块过大"崩溃）。要求：客户端收到明确错误文本、非零退出、
// 不产生快照。
func TestRsyncCompressRejected(t *testing.T) {
	port, r := startTestServer(t)
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("compress me"), 0o644)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-az", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("-z 压缩会话应被拒绝:\n%s", out)
	}
	if !strings.Contains(strings.ToLower(string(out)), "compress") {
		t.Fatalf("错误信息应说明压缩不支持:\n%s", out)
	}
	if files, err := r.GetFilesForTest(); err != nil || len(files) != 0 {
		t.Fatalf("被拒绝的会话不应落库任何文件: %v %v", files, err)
	}
}

// TestRsyncCompressRestoreRejected 恢复方向 -az（客户端要求服务端 sender 压缩
// 发送）同样拒绝。
func TestRsyncCompressRestoreRejected(t *testing.T) {
	port, _, _ := startLoggedRouterServer(t)
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("restore me"), 0o644)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("备份失败: %v\n%s", err, out)
	}
	dst := t.TempDir()
	cmd = exec.Command("rsync", "-az", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/", dst+"/")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("恢复方向 -z 应被拒绝:\n%s", out)
	}
	if !strings.Contains(strings.ToLower(string(out)), "compress") {
		t.Fatalf("错误信息应说明压缩不支持:\n%s", out)
	}
}

// TestRsyncBackupChecksum P0#4：`rsync -ac` 备份——客户端每条 REGULAR flist 条目
// 尾部附 16 字节内容 MD5，服务端须读掉保持流同步（此前字段错位误报
// "暂不支持设备/特殊文件类型"）。
func TestRsyncBackupChecksum(t *testing.T) {
	port, r := startTestServer(t)
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("checksum mode"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("nested"), 0o644)
	os.Symlink("a.txt", filepath.Join(src, "link1"))
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-ac", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsync -ac 备份失败: %v\n%s", err, out)
	}
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "checksum mode" {
		t.Fatalf("读取 a.txt: %v %q", err, out.String())
	}
	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range files {
		got[f.Path] = true
	}
	if !got["sub/b.txt"] || !got["link1"] {
		t.Fatalf("快照清单不完整: %v", files)
	}
}

// TestRsyncRestoreChecksum 恢复方向 `rsync -ac`：客户端 argv 短包含 'c'，
// 服务端 sender 发 flist 时每条 REGULAR 条目尾部同样须附 16 字节内容 MD5
// （客户端 recv_file_entry 无条件读该段，缺失则整条流错位）。
func TestRsyncRestoreChecksum(t *testing.T) {
	port, _, _ := startLoggedRouterServer(t)
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("checksum restore"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("nested"), 0o644)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("备份失败: %v\n%s", err, out)
	}
	dst := t.TempDir()
	cmd = exec.Command("rsync", "-ac", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/", dst+"/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rsync -ac 恢复失败: %v\n%s", err, out)
	}
	data, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	if err != nil || string(data) != "checksum restore" {
		t.Fatalf("恢复 a.txt: %v %q", err, data)
	}
	data, err = os.ReadFile(filepath.Join(dst, "sub", "b.txt"))
	if err != nil || string(data) != "nested" {
		t.Fatalf("恢复 sub/b.txt: %v %q", err, data)
	}
}

// TestRsyncBackupFifoSpecial P0#5：源目录含 FIFO（-a 隐含 -D=--devices --specials）。
// 此前 flist 解析遇 FIFO mode 直接报"暂不支持设备/特殊/其他文件类型"，整个备份
// 会话失败（rsync rc=12）；修复后设备/特殊条目读完整字段流但跳过落库（警告日志 +
// 客户端提示），其余文件正常备份，rsync rc=0，恢复结果中无 FIFO。
func TestRsyncBackupFifoSpecial(t *testing.T) {
	port, _, logBuf := startLoggedRouterServer(t)

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("fifo test"), 0o644)
	if err := syscall.Mkfifo(filepath.Join(src, "myfifo"), 0o644); err != nil {
		t.Skipf("mkfifo 失败: %v", err)
	}

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("含 FIFO 的 -a 备份应成功: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "skipping non-regular file") {
		t.Fatalf("客户端应收到跳过提示:\n%s", out)
	}
	if !strings.Contains(logBuf.String(), "skip_special_file") {
		t.Fatalf("服务端日志应含 skip_special_file:\n%s", logBuf)
	}

	// 恢复对照：a.txt 内容一致，myfifo 不存在（即快照中无 FIFO 行）
	dst := t.TempDir()
	cmd = exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/", dst+"/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("恢复失败: %v\n%s", err, out)
	}
	data, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	if err != nil || string(data) != "fifo test" {
		t.Fatalf("恢复 a.txt: %v %q", err, data)
	}
	if _, err := os.Lstat(filepath.Join(dst, "myfifo")); err == nil {
		t.Fatal("恢复结果不应包含 myfifo")
	}
}

// TestRsyncBackupNoLinksSymlink 无 -l 备份含 symlink 源（rsync -r）：客户端 flist
// 中 symlink 条目 mode 保留 S_IFLNK 但不带 target 段（flist.c:925 前提），服务端
// 跳过落库。与真实 rsyncd 3.4.1 行为对照：rc=0，客户端经 MSG_INFO 收到
// "skipping non-regular file"（generator.c:2113），恢复结果无该 symlink。
// 回归背景：曾因解码侧无 -l 仍读 target 段导致流错位挂死（7d93a62 修复）。
func TestRsyncBackupNoLinksSymlink(t *testing.T) {
	port, _, logBuf := startLoggedRouterServer(t)

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("nolinks"), 0o644)
	os.Symlink("a.txt", filepath.Join(src, "b.txt"))

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-r", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("无 -l 备份应成功: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), `skipping non-regular file "b.txt"`) {
		t.Fatalf("客户端应收到跳过提示（对照真实 rsyncd generator.c:2113）:\n%s", out)
	}
	if !strings.Contains(logBuf.String(), "skip_link_no_target") {
		t.Fatalf("服务端日志应含 skip_link_no_target:\n%s", logBuf)
	}

	// 无 -l 列目录（恢复方向 flist 编码不带 target 段）也应正常
	cmd = exec.Command("rsync", "--list-only", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil || !strings.Contains(string(out), "a.txt") {
		t.Fatalf("无 -l 列目录应正常列出:\n%v\n%s", err, out)
	}

	// 恢复对照：a.txt 内容一致，b.txt 不存在（快照中无 symlink 行）
	dst := t.TempDir()
	cmd = exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/", dst+"/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("恢复失败: %v\n%s", err, out)
	}
	data, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	if err != nil || string(data) != "nolinks" {
		t.Fatalf("恢复 a.txt: %v %q", err, data)
	}
	if _, err := os.Lstat(filepath.Join(dst, "b.txt")); err == nil {
		t.Fatal("恢复结果不应包含未备份的 b.txt")
	}
}

// TestRsyncEmptySessionNoWrite P1#6：空 flist 备份会话（无 -a 的空目录推送，
// argv 形如 "--server -e.LsfxCIvu --stats . home/"，entries=0）不应产生任何写——
// 单一状态模型下空会话 = 无 upsert 无删除，当前清单保持不变。
func TestRsyncEmptySessionNoWrite(t *testing.T) {
	port, r := startTestServer(t)

	empty := t.TempDir() // 空目录
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("keep"), 0o644)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	push := func(args ...string) {
		t.Helper()
		base := []string{"--password-file=" + pw, "--port", fmt.Sprint(port)}
		if out, err := exec.Command("rsync", append(base, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("rsync %v 失败: %v\n%s", args, err, out)
		}
	}

	// 空模块上的空会话：零写入
	push("--stats", empty+"/", "backup@127.0.0.1::home/")
	if files, err := r.GetFilesForTest(); err != nil || len(files) != 0 {
		t.Fatalf("空模块空会话不应落库: %v %v", files, err)
	}
	// 正常备份一个文件
	push("-a", src+"/", "backup@127.0.0.1::home/")
	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "a.txt" {
		t.Fatalf("正常备份应落库 a.txt: %v", files)
	}
	// 已有内容后的空会话：清单不变（无 upsert 无删除）
	push("--stats", empty+"/", "backup@127.0.0.1::home/")
	files2, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files2) != 1 || files2[0].Path != "a.txt" {
		t.Fatalf("空会话不应改变清单: %v", files2)
	}
}

// TestRsyncSourceMissingNoHang P1#8：客户端源目录不存在（sender io_error=1）时
// flist 结尾发 XMIT_IO_ERROR_ENDLIST(0x1004)+varint 哨兵（flist.c:2388-2391）。
// 此前解析器把哨兵当普通条目读字段流 → 错位卡死到连接超时；修复后服务端识别
// 哨兵结束 flist，会话快速正常收尾（客户端自身报 link_stat 错误 rc=23），不产生快照。
func TestRsyncSourceMissingNoHang(t *testing.T) {
	port, r := startTestServer(t)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	missing := filepath.Join(t.TempDir(), "no-such-dir")
	done := make(chan struct{})
	var out []byte
	var rerr error
	go func() {
		cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
			missing+"/", "backup@127.0.0.1::home/")
		out, rerr = cmd.CombinedOutput()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("源目录缺失的会话不应挂起（IO_ERROR_ENDLIST 未识别）")
	}
	if rerr == nil {
		t.Fatalf("客户端应报源缺失错误:\n%s", out)
	}
	if files, err := r.GetFilesForTest(); err != nil || len(files) != 0 {
		t.Fatalf("失败会话不应落库任何文件: %v %v", files, err)
	}
}

// TestRsyncDeleteSemantics P1#7：--delete 备份的当前状态语义——传输根（子路径
// 前缀）内、本次 flist 未覆盖的文件与目录行应被原地删除（恢复时不"复活"客户端
// 已删除的文件）。同时验证：范围界定（其他子路径不受影响）。io_error 联动
// （源缺失时禁删）由 TestRsyncSourceMissingIoErrorNoDelete 覆盖。
func TestRsyncDeleteSemantics(t *testing.T) {
	port, r, _ := startLoggedRouterServer(t)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.MkdirAll(filepath.Join(src, "emptydir"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("keep"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("gone"), 0o644)
	os.WriteFile(filepath.Join(src, "emptydir", "keep.txt"), []byte("gone dir"), 0o644)
	// 另一子路径先备份：验证 --delete 范围界定不殃及
	other := t.TempDir()
	os.WriteFile(filepath.Join(other, "x.txt"), []byte("other"), 0o644)

	rsyncRun := func(args ...string) {
		t.Helper()
		base := []string{"--password-file=" + pw, "--port", fmt.Sprint(port)}
		if out, err := exec.Command("rsync", append(base, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("rsync %v 失败: %v\n%s", args, err, out)
		}
	}
	rowsOf := func() map[string]bool {
		t.Helper()
		files, err := r.GetFilesForTest()
		if err != nil {
			t.Fatal(err)
		}
		m := map[string]bool{}
		for _, f := range files {
			m[f.Path] = true
		}
		return m
	}

	rsyncRun("-a", other+"/", "backup@127.0.0.1::home/other/")
	rsyncRun("-a", src+"/", "backup@127.0.0.1::home/del/")
	if m := rowsOf(); !m["del/sub/b.txt"] || !m["del/emptydir/keep.txt"] || !m["other/x.txt"] {
		t.Fatalf("首次备份清单不符: %v", m)
	}

	// 客户端删除 b.txt 与整个 emptydir 后 --delete 推送
	os.Remove(filepath.Join(src, "sub", "b.txt"))
	os.RemoveAll(filepath.Join(src, "emptydir"))
	rsyncRun("-a", "--delete", src+"/", "backup@127.0.0.1::home/del/")
	m2 := rowsOf()
	if m2["del/sub/b.txt"] || m2["del/emptydir"] || m2["del/emptydir/keep.txt"] {
		t.Fatalf("--delete 后客户端已删条目应从当前清单消失: %v", m2)
	}
	if !m2["del/a.txt"] || !m2["del/sub"] || !m2["other/x.txt"] {
		t.Fatalf("--delete 不应误删保留条目/其他子路径: %v", m2)
	}
	// 恢复验证：b.txt / emptydir 不复活
	dst := t.TempDir()
	rsyncRun("-a", "backup@127.0.0.1::home/del/", dst+"/")
	if _, err := os.Stat(filepath.Join(dst, "sub", "b.txt")); !os.IsNotExist(err) {
		t.Fatalf("已删文件不应恢复复活: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "emptydir")); !os.IsNotExist(err) {
		t.Fatalf("已删目录不应恢复复活: %v", err)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "a.txt")); err != nil || string(b) != "keep" {
		t.Fatalf("a.txt 应完好: %v %q", err, b)
	}
}

// TestRsyncSourceMissingIoErrorNoDelete P1#7/P1#8 联动：rsync 语义 io_error≠0 时
// 禁用删除（flist.c:1402：发送侧出错时源清单不完整，删除会误删）。先备份有效
// 数据，再用源缺失 + --delete 推送——不应清空已有数据。
func TestRsyncSourceMissingIoErrorNoDelete(t *testing.T) {
	port, r, _ := startLoggedRouterServer(t)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("keep"), 0o644)
	base := []string{"--password-file=" + pw, "--port", fmt.Sprint(port)}
	if out, err := exec.Command("rsync", append([]string{"-a"}, append(base, src+"/", "backup@127.0.0.1::home/")...)...).CombinedOutput(); err != nil {
		t.Fatalf("首次备份失败: %v\n%s", err, out)
	}
	if files, err := r.GetFilesForTest(); err != nil || len(files) != 1 {
		t.Fatalf("首次备份应落库: %v %v", files, err)
	}

	// 源目录消失 + --delete：客户端 sender io_error=1，服务端必须禁删
	cmd := exec.Command("rsync", append([]string{"-a", "--delete"},
		append(base, filepath.Join(src, "gone-sub")+"/", "backup@127.0.0.1::home/")...)...)
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("源缺失 + --delete 应报错:\n%s", out)
	}
	// io_error 会话不得删除已有数据（--delete 被禁、无文件传输 → 清单不变）
	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "a.txt" {
		t.Fatalf("io_error 会话删除了已有数据: %v", files)
	}
}

// TestRsyncFileOpenDeniedPartial P1#9：客户端 open 失败（chmod 000）时发
// MSG_NO_SEND(102)+ndx 后跳过该文件继续下一个（sender.c:722-724）。rsync 语义：
// 该文件报错、其余文件正常完成（rc=23）。此前服务端 MuxStream 丢弃消息帧，
// recvNdxEcho 死等被跳过文件的回显 → 会话永久挂起。修复后：会话正常完成
// （好文件入库、失败文件不落库），恢复好文件完好。
func TestRsyncFileOpenDeniedPartial(t *testing.T) {
	port, r, logBuf := startLoggedRouterServer(t)
	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("good a"), 0o644)
	os.WriteFile(filepath.Join(src, "secret.txt"), []byte("unreadable"), 0o644)
	os.WriteFile(filepath.Join(src, "z.txt"), []byte("good z"), 0o644)
	if err := os.Chmod(filepath.Join(src, "secret.txt"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(filepath.Join(src, "secret.txt"), 0o644) })

	// 挂起防护：当前实现死锁，20 秒内未返回即失败
	done := make(chan struct{})
	var out []byte
	var rerr error
	go func() {
		cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
			src+"/", "backup@127.0.0.1::home/")
		out, rerr = cmd.CombinedOutput()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("客户端 open 失败的会话不应挂起（MSG_NO_SEND 未处理）")
	}
	if rerr == nil {
		t.Fatalf("含不可读文件应 rc!=0:\n%s", out)
	}
	if !strings.Contains(string(out), "secret.txt") {
		t.Fatalf("客户端应报 secret.txt 错误:\n%s", out)
	}

	// 其余文件正常完成：当前清单含好文件、不含失败文件
	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range files {
		got[f.Path] = true
	}
	if !got["a.txt"] || !got["z.txt"] {
		t.Fatalf("好文件应入库: %v", files)
	}
	if got["secret.txt"] {
		t.Fatalf("open 失败的文件不应入库: %v", files)
	}
	if findLine(logBuf.String(), "msg=file_skipped") == "" {
		t.Fatalf("日志应含 file_skipped:\n%s", logBuf.String())
	}

	// 恢复好文件完好
	dst := t.TempDir()
	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/", dst+"/")
	if rout, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("恢复失败: %v\n%s", err, rout)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "a.txt")); err != nil || string(b) != "good a" {
		t.Fatalf("恢复 a.txt: %v %q", err, b)
	}
}

// TestRsyncDryRunBackup dry-run 备份（argv 'n'，!do_xfers）：真实客户端 sender 对
// 传输请求只回显 ndx+iflags（sender.c:638-642），不读 sum_head/不发 token 流；服务端
// 须同样只发 ndx+iflags（generator.c:2390 !do_xfers 跳过 sums）且不落库（含静态
// 目录条目——单一状态模型下目录行落库同样是写）。复现来源：UGOS（极空间）备份
// 任务先发 dry-run 会话，此前服务端照常发空 sum_head 并死等回显导致 EOF 报错。
func TestRsyncDryRunBackup(t *testing.T) {
	port, r, _ := startLoggedRouterServer(t)

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("v1"), 0o644)

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	// 先正常备份一次
	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("首次备份失败: %v\n%s", err, out)
	}
	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "a.txt" {
		t.Fatalf("首次备份应落库 a.txt: %v", files)
	}

	// 新文件 + 变化文件后 dry-run 推送
	os.WriteFile(filepath.Join(src, "new.txt"), []byte("brand new"), 0o644)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("v2 changed"), 0o644)
	cmd = exec.Command("rsync", "-n", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dry-run 备份失败: %v\n%s", err, out)
	}

	// dry-run 零写入：清单不变、内容不变
	files2, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files2) != 1 {
		t.Fatalf("dry-run 不应落库新文件: %v", files2)
	}
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "v1" {
		t.Fatalf("dry-run 不应改变已备份内容: %v %q", err, out.String())
	}

	// dry-run 之后再真实备份，新内容正常入库（dry-run 无残留影响）
	cmd = exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dry-run 后真实备份失败: %v\n%s", err, out)
	}
	files3, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files3) != 2 {
		t.Fatalf("真实备份后清单应含 2 个文件: %v", files3)
	}
	var out2 bytes.Buffer
	if err := r.ReadFile("new.txt", &out2); err != nil || out2.String() != "brand new" {
		t.Fatalf("真实备份后应能读到新文件: %v %q", err, out2.String())
	}
}

// TestRsyncDryRunRestore dry-run 恢复（rsync -n 拉取）：客户端 generator 对传输
// 请求只发 ndx+iflags 不发 sum_head（generator.c:2390），服务端 sender 须只回显
// ndx+iflags（sender.c:638-642）不读 sums 不发文件数据。
func TestRsyncDryRunRestore(t *testing.T) {
	port, _, _ := startLoggedRouterServer(t)

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "b.txt"), []byte("world"), 0o644)

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("备份失败: %v\n%s", err, out)
	}

	dst := t.TempDir()
	cmd = exec.Command("rsync", "-n", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/", dst+"/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dry-run 恢复失败: %v\n%s", err, out)
	}
	// dry-run 恢复不写任何文件
	ents, _ := os.ReadDir(dst)
	if len(ents) != 0 {
		t.Fatalf("dry-run 恢复不应写目标目录: %v", ents)
	}

	// 真实恢复仍正常
	cmd = exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/", dst+"/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("真实恢复失败: %v\n%s", err, out)
	}
	if b, err := os.ReadFile(filepath.Join(dst, "a.txt")); err != nil || string(b) != "hello" {
		t.Fatalf("真实恢复 a.txt: %v %q", err, b)
	}
}

// TestRsyncRestoreMissingPath 恢复方向请求不存在的路径：对齐真实 rsyncd——
// MSG_ERROR_XFER 报 change_dir/link_stat 失败（客户端 rc=23），空 flist 发完直接
// 断连（main.c:993-999 空 flist exit_cleanup，不进 goodbye）。此前返回 rc=0 空列表，
// UGOS（极空间）探测 备份任务*.ubk/config 时误判后放弃备份。
func TestRsyncRestoreMissingPath(t *testing.T) {
	port, _, _ := startLoggedRouterServer(t)

	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("nested"), 0o644)

	pw := filepath.Join(t.TempDir(), "pw")
	os.WriteFile(pw, []byte("secret\n"), 0o600)

	cmd := exec.Command("rsync", "-a", "--password-file="+pw, "--port", fmt.Sprint(port),
		src+"/", "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("备份失败: %v\n%s", err, out)
	}

	// 带目录的文件请求（UGOS 探测场景）：父目录不存在 → change_dir 错误
	cmd = exec.Command("rsync", "--list-only", "-r", "--password-file="+pw,
		"--port", fmt.Sprint(port),
		"backup@127.0.0.1::home/备份任务1_20260825_16114230.ubk/config")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("路径不存在应 rc=23，实际成功:\n%s", out)
	}
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 23 {
		t.Fatalf("期待退出码 23，实际: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), `change_dir "备份任务1_20260825_16114230.ubk" (in home) failed: No such file or directory (2)`) {
		t.Fatalf("应含 change_dir 错误消息:\n%s", out)
	}

	// 顶层文件请求（无目录部分）：link_stat 错误
	cmd = exec.Command("rsync", "--list-only", "-r", "--password-file="+pw,
		"--port", fmt.Sprint(port), "backup@127.0.0.1::home/missing.txt")
	out, err = cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 23 {
		t.Fatalf("期待退出码 23，实际: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), `link_stat "missing.txt" (in home) failed: No such file or directory (2)`) {
		t.Fatalf("应含 link_stat 错误消息:\n%s", out)
	}

	// 父路径是文件（非目录）：change_dir + ENOTDIR（真实 rsyncd 同款：
	// change_dir "a.txt" (in test) failed: Not a directory (20)）
	cmd = exec.Command("rsync", "--list-only", "-r", "--password-file="+pw,
		"--port", fmt.Sprint(port), "backup@127.0.0.1::home/a.txt/nope")
	out, err = cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 23 {
		t.Fatalf("期待退出码 23，实际: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), `change_dir "a.txt" (in home) failed: Not a directory (20)`) {
		t.Fatalf("应含 change_dir ENOTDIR 错误消息:\n%s", out)
	}

	// 父目录存在、文件不存在：link_stat + ENOENT
	cmd = exec.Command("rsync", "--list-only", "-r", "--password-file="+pw,
		"--port", fmt.Sprint(port), "backup@127.0.0.1::home/sub/nope.txt")
	out, err = cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 23 {
		t.Fatalf("期待退出码 23，实际: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), `link_stat "sub/nope.txt" (in home) failed: No such file or directory (2)`) {
		t.Fatalf("应含 link_stat 错误消息:\n%s", out)
	}

	// 存在路径不受影响
	cmd = exec.Command("rsync", "--list-only", "--password-file="+pw,
		"--port", fmt.Sprint(port), "backup@127.0.0.1::home/")
	if out, err := cmd.CombinedOutput(); err != nil || !strings.Contains(string(out), "a.txt") {
		t.Fatalf("正常列表失败: %v\n%s", err, out)
	}
}
