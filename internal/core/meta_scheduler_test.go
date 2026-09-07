package core

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/webdav"

	"crysync/internal/config"
)

func TestMetaBackupSchedulerImmediateAndRetained(t *testing.T) {
	root := t.TempDir()
	module := &config.ModuleConfig{
		Name: "home",
		Backend: config.BackendConfig{
			Type: "dir",
			Path: filepath.Join(root, "data"),
		},
		Keyfile: filepath.Join(root, "key"),
		Meta:    filepath.Join(root, "meta", "home.db"),
		MetaBackup: config.MetaBackupConfig{
			Interval: "25ms",
			Retain:   2,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() { done <- RunMetaBackupScheduler(ctx, module, logger) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		entries, _ := os.ReadDir(filepath.Join(module.Backend.Path, "meta"))
		if len(entries) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("调度器未及时创建 Meta 备份")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("调度器未响应取消")
	}
	entries, err := os.ReadDir(filepath.Join(module.Backend.Path, "meta"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 2 {
		t.Fatalf("保留策略未生效，备份数=%d", len(entries))
	}
}

// TestMetaBackupSchedulerInitRetry 自愈循环：后端未就绪 → 退避重试 →
// 后端就绪后自动初始化 key/meta 并产出首个远端 Meta 备份。
func TestMetaBackupSchedulerInitRetry(t *testing.T) {
	dir := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bePort := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	module := &config.ModuleConfig{
		Name: "home",
		Backend: config.BackendConfig{
			Type: "webdav",
			URL:  fmt.Sprintf("http://127.0.0.1:%d", bePort),
		},
		Keyfile: filepath.Join(dir, "key"),
		Meta:    filepath.Join(dir, "meta", "home.db"),
		MetaBackup: config.MetaBackupConfig{
			Interval: "100ms",
			Retain:   2,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	go func() { done <- RunMetaBackupScheduler(ctx, module, logger) }()

	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(module.Keyfile); !os.IsNotExist(err) {
		cancel()
		t.Fatalf("后端未就绪时不应创建 key: %v", err)
	}

	beRoot := filepath.Join(dir, "backend")
	if err := os.MkdirAll(beRoot, 0o755); err != nil {
		cancel()
		t.Fatal(err)
	}
	beLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", bePort))
	if err != nil {
		cancel()
		t.Fatalf("重新绑定端口失败: %v", err)
	}
	beSrv := &http.Server{Handler: &webdav.Handler{
		FileSystem: webdav.Dir(beRoot),
		LockSystem: webdav.NewMemLS(),
	}}
	go beSrv.Serve(beLn) //nolint:errcheck // 测试服务器错误经断言可见
	defer beSrv.Close()

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(module.Keyfile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("自愈循环未在 3s 内完成初始化")
		}
		time.Sleep(50 * time.Millisecond)
	}
	metaDir := filepath.Join(beRoot, "meta")
	deadline = time.Now().Add(3 * time.Second)
	for {
		entries, _ := os.ReadDir(metaDir)
		hasBackup := false
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".cmeta") {
				hasBackup = true
			}
		}
		if hasBackup {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("自愈后未生成远端 Meta 备份")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("调度器未响应取消")
	}
	if !strings.Contains(logs.String(), "msg=module_recovered") {
		t.Fatalf("自愈成功后应记录 module_recovered，日志=%s", logs.String())
	}
}

func TestGCSchedulerReclaimsAndLogs(t *testing.T) {
	root := t.TempDir()
	module := &config.ModuleConfig{
		Name:    "home",
		Backend: config.BackendConfig{Type: "dir", Path: filepath.Join(root, "data")},
		Keyfile: filepath.Join(root, "key"),
		Meta:    filepath.Join(root, "meta", "home.db"),
		GC:      config.GCConfig{Interval: "10ms"},
	}
	opened, err := OpenModule(module)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := opened.Repo.StoreChunk([]byte("orphan")); err != nil {
		opened.Close()
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	go func() { done <- RunGCScheduler(ctx, module, logger) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		be, err := openBackend(module)
		if err != nil {
			t.Fatal(err)
		}
		blobs, err := be.List()
		if err != nil {
			t.Fatal(err)
		}
		if len(blobs) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("GC 调度器未回收孤儿 blob")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GC 调度器未响应取消")
	}
	if got := logs.String(); !strings.Contains(got, "msg=gc_start") ||
		!strings.Contains(got, "msg=gc_complete") || !strings.Contains(got, "reclaimed_blobs=1") {
		t.Fatalf("GC 日志不完整: %s", got)
	}
}
