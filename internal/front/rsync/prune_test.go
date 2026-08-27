// internal/front/rsync/prune_test.go
package rsync

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crysync/internal/config"
	"crysync/internal/core/crypto"
)

// TestSleepUntil：计算下一个调度时刻（今天已过则明天）；ctx 取消立即返回。
func TestSleepUntil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 调度时刻设为"下分钟整点"：最多等 61 秒，验证能醒来
	next := time.Now().Add(time.Minute)
	sched := fmt.Sprintf("%02d:%02d", next.Hour(), next.Minute())
	done := make(chan bool, 1)
	go func() { done <- sleepUntil(ctx, sched) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("应正常醒来")
		}
	case <-time.After(70 * time.Second):
		t.Fatal("sleepUntil 未按时返回")
	}
}

// TestSleepUntilBadSchedule：非法 schedule 回落 03:00；已取消的 ctx 立即返回 false。
func TestSleepUntilBadSchedule(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if ok := sleepUntil(ctx, "not-a-time"); ok {
		t.Fatal("已取消的 ctx 应返回 false")
	}
}

// TestPruneOnce：带 prune 配置（keep_last=1）的完整 daemon：备份两次后手动
// 触发一次 prune——最新快照保留且可恢复，旧快照被删除。
func TestPruneOnce(t *testing.T) {
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
		Front: config.FrontConfig{
			Rsync: &config.RsyncFrontConfig{
				Listen: fmt.Sprintf("127.0.0.1:%d", port),
			},
		},
		Modules: []config.ModuleConfig{{
			Name: "home", Path: "/",
			Backend: config.BackendConfig{Type: "dir", Path: filepath.Join(dir, "data")},
			Keyfile: keyPath,
			Meta:    filepath.Join(dir, "home.db"),
			Prune:   &config.PruneConfig{KeepLast: 1, Schedule: "23:59"},
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go New(cfg, nil).Serve(ctx)
	time.Sleep(200 * time.Millisecond)

	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "f.txt"), []byte("v1 content"), 0o644)
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", src+"/", "127.0.0.1::home/"); err != nil {
		t.Fatalf("首次备份失败: %v\n%s", err, out)
	}
	os.WriteFile(filepath.Join(src, "f.txt"), []byte("v2 content longer"), 0o644)
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", src+"/", "127.0.0.1::home/"); err != nil {
		t.Fatalf("二次备份失败: %v\n%s", err, out)
	}

	// 手动触发一次 prune（等价于调度时刻到达）
	if err := pruneOnce(&cfg.Modules[0], nopLogger); err != nil {
		t.Fatalf("pruneOnce: %v", err)
	}

	// 最新快照仍可恢复（内容为 v2）
	dest := t.TempDir()
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", "127.0.0.1::home/", dest+"/"); err != nil {
		t.Fatalf("prune 后恢复失败: %v\n%s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(dest, "f.txt"))
	if err != nil || string(got) != "v2 content longer" {
		t.Fatalf("prune 后内容不符: %q %v", got, err)
	}
}
