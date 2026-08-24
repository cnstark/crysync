// internal/logging/rotate_test.go
package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readAll(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	return string(b)
}

func TestRotateOnSizeExceed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crysyncd.log")
	// maxFiles=3，软上限 100 字节
	w, err := NewRotateWriter(path, 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	// 第一行 60B：未超限
	if _, err := w.Write([]byte(strings.Repeat("a", 59) + "\n")); err != nil {
		t.Fatal(err)
	}
	// 第二行 60B：将超 100 → 先轮转（旧行完整进 .1），新行进新文件
	if _, err := w.Write([]byte(strings.Repeat("b", 59) + "\n")); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, path+".1"); got != strings.Repeat("a", 59)+"\n" {
		t.Fatalf("log.1 应为完整旧行，得到 %d 字节", len(got))
	}
	if got := readAll(t, path); got != strings.Repeat("b", 59)+"\n" {
		t.Fatalf("log 应为新行，得到 %d 字节", len(got))
	}
}

func TestRotateRetainCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crysyncd.log")
	w, err := NewRotateWriter(path, 50, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 10; i++ {
		if _, err := w.Write([]byte(fmt.Sprintf("line-%02d-%s\n", i, strings.Repeat("x", 40)))); err != nil {
			t.Fatal(err)
		}
	}
	// 只保留 log.1 log.2，.3 不应存在
	for _, p := range []string{path + ".1", path + ".2"} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s 应存在: %v", p, err)
		}
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatalf("log.3 应被删除（保留 2 个）")
	}
	// 最旧保留的是最后被轮转挤入 .2 的内容
	if !strings.HasPrefix(readAll(t, path+".2"), "line-") {
		t.Fatal("log.2 内容错乱")
	}
}

func TestNoRotateWhenMaxFilesZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crysyncd.log")
	w, err := NewRotateWriter(path, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 20; i++ {
		if _, err := w.Write([]byte("0123456789\n")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
		t.Fatal("max_files=0 不应轮转")
	}
	if len(readAll(t, path)) != 220 {
		t.Fatal("全部内容应在单文件内")
	}
}

func TestStartupRotateWhenExceeded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crysyncd.log")
	writeFile(t, path, strings.Repeat("o", 200))
	w, err := NewRotateWriter(path, 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	// 启动时已超限：旧内容进 .1，当前 log 为空
	if len(readAll(t, path+".1")) != 200 {
		t.Fatal("启动轮转应把超限旧文件移入 .1")
	}
	if len(readAll(t, path)) != 0 {
		t.Fatal("启动轮转后 log 应为空")
	}
}

func TestConcurrentWriteNoInterleave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crysyncd.log")
	w, err := NewRotateWriter(path, 1<<20, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	const goroutines, lines = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < lines; i++ {
				// 每行固定 100 字节且带行首编号，交错会破坏行完整性
				if _, err := fmt.Fprintf(w, "[%02d-%03d] %s\n", g, i, strings.Repeat("y", 90)); err != nil {
					t.Errorf("写失败: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
	content := readAll(t, path)
	for _, line := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
		// 每行 99 字节（不含换行）且格式 [gg-iii] yyy…，交错会破坏固定长度与格式
		if len(line) != 99 || !strings.HasSuffix(line, strings.Repeat("y", 90)) {
			t.Fatalf("交错损坏：行 %q（长度 %d）", line[:16], len(line))
		}
	}
	if n := strings.Count(content, "\n"); n != goroutines*lines {
		t.Fatalf("行数 %d 应为 %d", n, goroutines*lines)
	}
}
