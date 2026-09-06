package core

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

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
