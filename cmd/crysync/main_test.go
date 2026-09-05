// cmd/crysync/main_test.go
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
