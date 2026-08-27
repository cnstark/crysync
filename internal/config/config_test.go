package config

import (
	"os"
	"path/filepath"
	"strings"
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
front:
  rsync:
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
	if c.Front.Rsync.Listen != "127.0.0.1:873" || c.Front.Rsync.Auth.Users["backup"] != "secret" {
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
	p := writeTemp(t, "front:\n  rsync:\n    listen: \"\"\nmodules:\n  - name: a\n    path: /\n    backend: { type: dir, path: /x }\n    keyfile: /k\n    meta: /m\n")
	c, err := Load(p)
	if err != nil {
		t.Fatalf("合法结构（空 listen + 一个模块）应通过 Load: %v", err)
	}
	if err := c.Validate(); err == nil {
		t.Fatal("空 listen 应校验失败")
	} else if !strings.Contains(err.Error(), "listen") {
		t.Fatalf("校验错误应提及 listen, got %v", err)
	}
	p = writeTemp(t, "modules:\n  - name: a\n    backend: { type: dir, path: /x }\n")
	if _, err := Load(p); err == nil {
		t.Fatal("缺少 keyfile/meta 应报错")
	}
}

func TestDuplicateModuleName(t *testing.T) {
	p := writeTemp(t, `
front:
  rsync:
    listen: "127.0.0.1:873"
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

func TestLogConfigDefaults(t *testing.T) {
	// 无 log 节：全部默认值（file 空 = 仅 stderr）
	p := writeTemp(t, `
front:
  rsync:
    listen: "127.0.0.1:873"
modules:
  - name: home
    path: /
    backend: { type: dir, path: /x }
    keyfile: /k
    meta: /m
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Log.File != "" || c.Log.Level != "info" || c.Log.SizeLimitMB() != 16 || c.Log.RetainFiles() != 5 {
		t.Fatalf("log 默认值错误: %+v", c.Log)
	}
}

func TestLogConfigExplicit(t *testing.T) {
	p := writeTemp(t, `
listen: "127.0.0.1:873"
log:
  file: /var/log/crysync/crysyncd.log
  level: debug
  max_size_mb: 8
  max_files: 2
modules:
  - name: home
    path: /
    backend: { type: dir, path: /x }
    keyfile: /k
    meta: /m
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Log.File != "/var/log/crysync/crysyncd.log" || c.Log.Level != "debug" ||
		c.Log.SizeLimitMB() != 8 || c.Log.RetainFiles() != 2 {
		t.Fatalf("log 解析错误: %+v", c.Log)
	}
}

func TestLogConfigInvalidLevel(t *testing.T) {
	p := writeTemp(t, `
listen: "127.0.0.1:873"
log:
  file: /tmp/x.log
  level: verbose
modules:
  - name: home
    path: /
    backend: { type: dir, path: /x }
    keyfile: /k
    meta: /m
`)
	if _, err := Load(p); err == nil {
		t.Fatal("非法 level 应报错")
	}
}

func TestLogConfigInvalidSize(t *testing.T) {
	for _, bad := range []string{"max_size_mb: 0", "max_files: -1"} {
		p := writeTemp(t, `
listen: "127.0.0.1:873"
log:
  file: /tmp/x.log
  `+bad+`
modules:
  - name: home
    path: /
    backend: { type: dir, path: /x }
    keyfile: /k
    meta: /m
`)
		if _, err := Load(p); err == nil {
			t.Fatalf("%s 应报错", bad)
		}
	}
}

func TestLogConfigMaxFilesZero(t *testing.T) {
	// 显式 0 = 不轮转（区别于缺省 5）
	p := writeTemp(t, `
listen: "127.0.0.1:873"
log:
  file: /tmp/x.log
  max_files: 0
modules:
  - name: home
    path: /
    backend: { type: dir, path: /x }
    keyfile: /k
    meta: /m
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Log.RetainFiles() != 0 {
		t.Fatalf("显式 max_files=0 应为不轮转，得到 %d", c.Log.RetainFiles())
	}
}

// TestEnvExpansion：配置文本在 yaml 解析前做 ${VAR} 环境变量展开——
// 引号内引用可安全承载含特殊字符（#、:）的值；未定义变量展开为空。
// TestSnapshotToggle：snapshot 缺省 false（单份模式），显式 true 解析为 true。
func TestSnapshotToggle(t *testing.T) {
	p := writeTemp(t, `
front:
  rsync:
    listen: "127.0.0.1:873"
modules:
  - name: a
    path: /
    backend: { type: dir, path: /x }
    keyfile: /k
    meta: /m
  - name: b
    path: /
    backend: { type: dir, path: /x }
    keyfile: /k
    meta: /m
    snapshot: true
`)
	c, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Modules[0].Snapshot {
		t.Fatal("缺省 snapshot 应为 false（单份模式）")
	}
	if !c.Modules[1].Snapshot {
		t.Fatal("显式 snapshot: true 应解析为 true")
	}
}

// TestFrontConfig：front 节解析与 Validate 校验（至少一个前端）。
func TestFrontConfig(t *testing.T) {
	c, err := Load("testdata/front.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.Front.Rsync == nil || c.Front.Rsync.Listen != "0.0.0.0:873" {
		t.Fatalf("rsync front 不符: %+v", c.Front.Rsync)
	}
	if c.Front.WebDAV == nil || c.Front.WebDAV.Listen != "0.0.0.0:8080" {
		t.Fatalf("webdav front 不符: %+v", c.Front.WebDAV)
	}
	if c.Front.WebDAV.Auth.Users["backup"] != "secret" {
		t.Fatalf("webdav auth 不符: %+v", c.Front.WebDAV.Auth)
	}
	// 无 front → 校验失败
	c2 := &Config{Modules: c.Modules}
	if err := c2.Validate(); err == nil {
		t.Fatal("无 front 应校验失败")
	}
}

func TestFrontConfigNoWebDAV(t *testing.T) {
	c, err := Load("testdata/front-rsync-only.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.Front.WebDAV != nil {
		t.Fatalf("未配置 webdav 应为 nil: %+v", c.Front.WebDAV)
	}
}

func TestEnvExpansion(t *testing.T) {
	t.Setenv("CS_TEST_RSYNC_PASS", "p#ss:word")
	t.Setenv("CS_TEST_WEBDAV_URL", "http://quarkdav:8080/crysync/")
	p := writeTemp(t, `front:
  rsync:
    listen: "0.0.0.0:873"
    auth:
      users: { backup: "${CS_TEST_RSYNC_PASS}" }
modules:
  - name: quark
    path: "/"
    backend:
      type: webdav
      url: "${CS_TEST_WEBDAV_URL}"
      username: "${CS_TEST_UNSET_VAR}"
    keyfile: /keys/quark.key
    meta: /meta/quark.db
`)
	c, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Front.Rsync.Auth.Users["backup"] != "p#ss:word" {
		t.Fatalf("密码应展开为含特殊字符的 env 值: %q", c.Front.Rsync.Auth.Users["backup"])
	}
	if c.Modules[0].Backend.URL != "http://quarkdav:8080/crysync/" {
		t.Fatalf("url 应展开: %q", c.Modules[0].Backend.URL)
	}
	if c.Modules[0].Backend.Username != "" {
		t.Fatalf("未定义变量应展开为空: %q", c.Modules[0].Backend.Username)
	}
}
