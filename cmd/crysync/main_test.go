// cmd/crysync/main_test.go
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crysync/internal/config"
	"crysync/internal/core/meta"
	"crysync/internal/front/rsync"
)

func TestBuildSmoke(t *testing.T) {
	// 构建 CLI 并执行 init
	dir := t.TempDir()
	bin := filepath.Join(dir, "crysync")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("构建失败: %v\n%s", err, out)
	}

	conf := filepath.Join(dir, "crysync.yaml")
	confContent := "listen: 127.0.0.1:873\nmodules:\n  - name: home\n    path: /\n    backend: { type: dir, path: " + filepath.Join(dir, "data") + " }\n    keyfile: " + filepath.Join(dir, "keys", "home.key") + "\n    meta: " + filepath.Join(dir, "meta", "home.db") + "\n"
	if err := os.WriteFile(conf, []byte(confContent), 0o600); err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command(bin, "init", "--config", conf).CombinedOutput()
	if err != nil {
		t.Fatalf("init 失败: %v\n%s", err, out)
	}
	if _, err := os.Stat(filepath.Join(dir, "keys", "home.key")); err != nil {
		t.Fatalf("密钥文件未生成: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "meta", "home.db")); err != nil {
		t.Fatalf("元数据库未生成: %v", err)
	}
	// 幂等：再次执行不报错
	if out, err := exec.Command(bin, "init", "--config", conf).CombinedOutput(); err != nil {
		t.Fatalf("重复 init 失败: %v\n%s", err, out)
	}
	// 无参数 → 打印用法并报错
	if out, err := exec.Command(bin).CombinedOutput(); err == nil {
		t.Fatalf("无参数应报错: %s", out)
	}
}

// TestCLIVersion：--version / version 子命令打印注入的版本号
//（CI 用 ldflags -X main.version 注入，此处验证注入机制可用）。
func TestCLIVersion(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "crysync")
	build := exec.Command("go", "build", "-ldflags", "-X main.version=v9.9.9-test", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("构建失败: %v\n%s", err, out)
	}
	for _, arg := range []string{"--version", "version"} {
		out, err := exec.Command(bin, arg).CombinedOutput()
		if err != nil {
			t.Fatalf("%s 应成功: %v\n%s", arg, err, out)
		}
		if !strings.Contains(string(out), "v9.9.9-test") {
			t.Fatalf("%s 输出应含注入版本: %s", arg, out)
		}
	}
}

// TestCLISnapshotsAndPrune：snapshots 列表 / set-active / prune 全流程。
// 用 rsync.OpenRepoForModule 直接造快照（避免起 daemon），CLI 走真实二进制。
func TestCLISnapshotsAndPrune(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "crysync")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建失败: %v\n%s", err, out)
	}
	conf := filepath.Join(dir, "crysync.yaml")
	confContent := "listen: 127.0.0.1:873\nmodules:\n  - name: home\n    path: /\n" +
		"    backend: { type: dir, path: " + filepath.Join(dir, "data") + " }\n" +
		"    keyfile: " + filepath.Join(dir, "keys", "home.key") + "\n" +
		"    meta: " + filepath.Join(dir, "meta", "home.db") + "\n" +
		"    prune: { keep_last: 1 }\n"
	if err := os.WriteFile(conf, []byte(confContent), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(bin, "init", "--config", conf).CombinedOutput(); err != nil {
		t.Fatalf("init 失败: %v\n%s", err, out)
	}

	// 通过仓库层造 2 个快照
	cfg, err := config.Load(conf)
	if err != nil {
		t.Fatal(err)
	}
	r, closeRepo, err := rsync.OpenRepoForModule(&cfg.Modules[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"snap a", "snap b"} {
		txn, err := r.BeginSnapshot(time.Now())
		if err != nil {
			t.Fatal(err)
		}
		c, _, _ := r.StoreChunk([]byte(data))
		if err := txn.UpsertFile(meta.FileRow{Path: "f.txt", Mode: 0o644, Size: int64(len(data))},
			[]meta.ChunkRef{{ChunkID: c, IDX: 0}}); err != nil {
			t.Fatal(err)
		}
		if _, err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	closeRepo()

	// snapshots 列表：应含 2 个快照行
	out, err := exec.Command(bin, "snapshots", "--config", conf).CombinedOutput()
	if err != nil {
		t.Fatalf("snapshots 失败: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "2 个快照") || !strings.Contains(string(out), "活跃") {
		t.Fatalf("快照列表输出不符: %s", out)
	}

	// set-active 到快照 1（旧快照）
	if out, err := exec.Command(bin, "snapshots", "--config", conf, "--module", "home", "--set-active", "1").CombinedOutput(); err != nil {
		t.Fatalf("set-active 失败: %v\n%s", err, out)
	}
	out, _ = exec.Command(bin, "snapshots", "--config", conf, "--module", "home").CombinedOutput()
	if !strings.Contains(string(out), "1   ") || !strings.Contains(string(out), "*") {
		t.Fatalf("活跃标记应指向快照 1: %s", out)
	}

	// set-active 不存在的快照 → 报错
	if out, err := exec.Command(bin, "snapshots", "--config", conf, "--module", "home", "--set-active", "99").CombinedOutput(); err == nil {
		t.Fatalf("不存在的快照应报错: %s", out)
	}

	// prune（keep_last=1）：删除 1 个快照
	out, err = exec.Command(bin, "prune", "--config", conf, "--module", "home").CombinedOutput()
	if err != nil {
		t.Fatalf("prune 失败: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "删除 1 个快照") {
		t.Fatalf("prune 输出不符: %s", out)
	}
	// 列表应只剩 1 个快照
	out, _ = exec.Command(bin, "snapshots", "--config", conf, "--module", "home").CombinedOutput()
	if !strings.Contains(string(out), "1 个快照") {
		t.Fatalf("prune 后应剩 1 个快照: %s", out)
	}
	// 不存在的模块 → 报错
	if out, err := exec.Command(bin, "snapshots", "--config", conf, "--module", "nope").CombinedOutput(); err == nil {
		t.Fatalf("不存在的模块应报错: %s", out)
	}
}
