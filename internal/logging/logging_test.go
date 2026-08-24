// internal/logging/logging_test.go
package logging

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newLogger 用可注入 stderr 构造 logger（生产 New 的测试变体，stderr 传 io.Discard）。
func newLogger(t *testing.T, file, level string, maxSizeMB, maxFiles int) (*slog.Logger, string) {
	t.Helper()
	logger, err := newWithStderr(io.Discard, file, level, int64(maxSizeMB)<<20, maxFiles)
	if err != nil {
		t.Fatal(err)
	}
	return logger, file
}

func TestLoggerWritesToFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crysyncd.log")
	logger, _ := newLogger(t, path, "info", 16, 5)
	logger.Info("file", "path", "src/a.txt", "method", "delta")
	if got := readAll(t, path); !strings.Contains(got, "msg=file") ||
		!strings.Contains(got, "path=src/a.txt") || !strings.Contains(got, "method=delta") {
		t.Fatalf("日志行不完整: %q", got)
	}
}

func TestLoggerLevelFilter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crysyncd.log")
	// info 级：debug 不落盘
	logger, _ := newLogger(t, path, "info", 16, 5)
	logger.Debug("negotiated", "compat_flags", 0)
	if strings.Contains(readAll(t, path), "negotiated") {
		t.Fatal("info 级别不应输出 debug 事件")
	}
	// debug 级：debug 落盘
	path2 := filepath.Join(dir, "debug.log")
	logger2, _ := newLogger(t, path2, "debug", 16, 5)
	logger2.Debug("negotiated", "compat_flags", 0)
	if !strings.Contains(readAll(t, path2), "compat_flags=0") {
		t.Fatal("debug 级别应输出 debug 事件")
	}
}

func TestNewFileEmptyStderrOnly(t *testing.T) {
	// file 为空：仅 stderr，不创建任何文件，不报错
	logger, err := newWithStderr(io.Discard, "", "info", 16, 5)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("hello")
	if entries, _ := os.ReadDir(t.TempDir()); len(entries) != 0 {
		t.Fatal("不应创建日志文件")
	}
}

func TestNewInvalidLevel(t *testing.T) {
	if _, err := newWithStderr(io.Discard, "/tmp/x.log", "verbose", 16, 5); err == nil {
		t.Fatal("非法 level 应报错")
	}
}
