package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "crysync.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAndValidate(t *testing.T) {
	p := writeTemp(t, `
listen: "127.0.0.1:873"
auth:
  users:
    backup: secret
modules:
  - name: home
    path: "/"
    backend: { type: dir, path: /tmp/data }
    keyfile: /tmp/keys/home.key
    meta: /tmp/meta/home.db
`)
	c, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Listen != "127.0.0.1:873" || c.Auth.Users["backup"] != "secret" {
		t.Fatalf("解析错误: %+v", c)
	}
	m := c.Modules[0]
	if m.Name != "home" || m.Backend.Type != "dir" {
		t.Fatalf("模块解析错误: %+v", m)
	}
	if m.ChunkSizeBytes() != 4194304 {
		t.Fatalf("默认分块大小应为 4MiB")
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateErrors(t *testing.T) {
	p := writeTemp(t, "listen: \"\"\nmodules: []\n")
	if _, err := Load(p); err == nil {
		t.Fatal("空配置应报错")
	}
	p = writeTemp(t, "modules:\n  - name: a\n    backend: { type: dir, path: /x }\n")
	if _, err := Load(p); err == nil {
		t.Fatal("缺少 keyfile/meta 应报错")
	}
}

func TestDuplicateModuleName(t *testing.T) {
	p := writeTemp(t, `
modules:
  - name: a
    path: /
    backend: { type: dir, path: /x }
    keyfile: /k
    meta: /m
  - name: a
    path: /
    backend: { type: dir, path: /x }
    keyfile: /k
    meta: /m
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err == nil {
		t.Fatal("重复模块名应报错")
	}
}
