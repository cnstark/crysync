// internal/front/rsync/init_test.go
// 自动初始化（部署自举）测试：密钥/元数据缺失时的创建与安全边界。
package rsync

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crysync/internal/config"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
)

// testModule 构造指向临时目录的模块配置（不落任何文件）。
func testModule(t *testing.T) *config.ModuleConfig {
	t.Helper()
	dir := t.TempDir()
	return &config.ModuleConfig{
		Name: "home", Path: "/",
		Backend: config.BackendConfig{Type: "dir", Path: filepath.Join(dir, "data")},
		Keyfile: filepath.Join(dir, "home.key"),
		Meta:    filepath.Join(dir, "home.db"),
	}
}

// TestOpenRepoAutoInitFresh：密钥与元数据库均不存在时自动创建。
func TestOpenRepoAutoInitFresh(t *testing.T) {
	m := testModule(t)
	_, closeRepo, err := OpenRepoForModule(m)
	if err != nil {
		t.Fatalf("自动初始化失败: %v", err)
	}
	closeRepo()
	fi, err := os.Stat(m.Keyfile)
	if err != nil {
		t.Fatalf("密钥文件未创建: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("密钥权限应为 0600，实际 %o", fi.Mode().Perm())
	}
	if _, err := os.Stat(m.Meta); err != nil {
		t.Fatalf("元数据库未创建: %v", err)
	}
	// 二次打开：密钥保持不变（自动初始化只发生一次）
	first, err := os.ReadFile(m.Keyfile)
	if err != nil {
		t.Fatal(err)
	}
	_, closeRepo, err = OpenRepoForModule(m)
	if err != nil {
		t.Fatal(err)
	}
	closeRepo()
	second, _ := os.ReadFile(m.Keyfile)
	if string(first) != string(second) {
		t.Fatal("重复打开不应更换密钥")
	}
}

// TestOpenRepoMetaAutoCreate：密钥已存在、元数据库缺失时自动建库，密钥不动。
func TestOpenRepoMetaAutoCreate(t *testing.T) {
	m := testModule(t)
	k, _ := crypto.GenerateKey()
	if err := crypto.SaveKeyFile(m.Keyfile, k); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(m.Keyfile)
	_, closeRepo, err := OpenRepoForModule(m)
	if err != nil {
		t.Fatalf("密钥在而元数据缺失时应自动建库: %v", err)
	}
	closeRepo()
	if _, err := os.Stat(m.Meta); err != nil {
		t.Fatalf("元数据库未创建: %v", err)
	}
	after, _ := os.ReadFile(m.Keyfile)
	if string(before) != string(after) {
		t.Fatal("自动建库不应改动密钥文件")
	}
}

// TestOpenRepoRejectKeyMissingMetaExists：元数据库存在而密钥缺失（密钥卷丢失/
// 未挂载的典型特征）时拒绝自动初始化，避免静默换钥导致旧快照无法解密。
func TestOpenRepoRejectKeyMissingMetaExists(t *testing.T) {
	m := testModule(t)
	db, err := meta.Open(m.Meta)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	_, _, err = OpenRepoForModule(m)
	if err == nil || !strings.Contains(err.Error(), "拒绝自动生成新密钥") {
		t.Fatalf("应拒绝自动初始化，实际: %v", err)
	}
	if _, statErr := os.Stat(m.Keyfile); !os.IsNotExist(statErr) {
		t.Fatal("拒绝时不应创建密钥文件")
	}
}

// TestOpenRepoConcurrentAutoInit：多个连接同时首开同一模块，全部成功且密钥一致。
func TestOpenRepoConcurrentAutoInit(t *testing.T) {
	m := testModule(t)
	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, closeRepo, err := OpenRepoForModule(m)
			if err != nil {
				errs <- err
				return
			}
			closeRepo()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发打开失败: %v", err)
	}
	first, err := os.ReadFile(m.Keyfile)
	if err != nil || len(first) != 4+1+32 {
		t.Fatalf("密钥文件异常: %d 字节 %v", len(first), err)
	}
}

// TestEnsureModuleInitIdempotent：EnsureModuleInit 幂等（重复调用不换密钥）。
func TestEnsureModuleInitIdempotent(t *testing.T) {
	m := testModule(t)
	if _, err := EnsureModuleInit(m); err != nil {
		t.Fatal(err)
	}
	first, _ := os.ReadFile(m.Keyfile)
	if _, err := EnsureModuleInit(m); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(m.Keyfile)
	if string(first) != string(second) {
		t.Fatal("重复初始化不应更换密钥")
	}
	if _, err := os.Stat(m.Meta); err != nil {
		t.Fatalf("元数据库未创建: %v", err)
	}
}

// TestServeAutoInitEndToEnd：全新环境（无密钥无元数据）直接起 daemon，
// 启动即自动初始化，备份+恢复全链路可用（依赖 PATH 中的真实 rsync）。
func TestServeAutoInitEndToEnd(t *testing.T) {
	dir := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	cfg := &config.Config{
		Listen: fmt.Sprintf("127.0.0.1:%d", port),
		Auth:   config.AuthConfig{Users: map[string]string{"backup": "secret"}},
		Modules: []config.ModuleConfig{{
			Name: "home", Path: "/",
			Backend: config.BackendConfig{Type: "dir", Path: filepath.Join(dir, "data")},
			Keyfile: filepath.Join(dir, "home.key"),
			Meta:    filepath.Join(dir, "home.db"),
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, cfg, nil) }()
	time.Sleep(200 * time.Millisecond) // 等监听就绪

	// daemon 启动即完成初始化，无需手动 crysync init
	if _, err := os.Stat(cfg.Modules[0].Keyfile); err != nil {
		t.Fatalf("启动后密钥应已自动生成: %v", err)
	}
	if _, err := os.Stat(cfg.Modules[0].Meta); err != nil {
		t.Fatalf("启动后元数据库应已自动创建: %v", err)
	}
	src := t.TempDir()
	os.WriteFile(filepath.Join(src, "auto.txt"), []byte("auto init"), 0o644)
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", src+"/", "backup@127.0.0.1::home/"); err != nil {
		t.Fatalf("自动初始化后备份失败: %v\n%s", err, out)
	}
	dest := t.TempDir()
	if out, err := rsyncRun(t, port, t.TempDir(), "-a", "backup@127.0.0.1::home/", dest+"/"); err != nil {
		t.Fatalf("自动初始化后恢复失败: %v\n%s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(dest, "auto.txt"))
	if err != nil || string(got) != "auto init" {
		t.Fatalf("恢复内容不符: %q %v", got, err)
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("Serve 退出异常: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve 未在取消后退出")
	}
}

// TestServeInitFailureDoesNotKillDaemon：单模块初始化失败（密钥路径不可写）只记
// 日志，daemon 继续监听其他连接。
func TestServeInitFailureDoesNotKillDaemon(t *testing.T) {
	dir := t.TempDir()
	// blocker 是普通文件，其下的密钥路径 MkdirAll 必然失败
	if err := os.WriteFile(filepath.Join(dir, "blocker"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	cfg := &config.Config{
		Listen: fmt.Sprintf("127.0.0.1:%d", port),
		Auth:   config.AuthConfig{Users: map[string]string{"backup": "secret"}},
		Modules: []config.ModuleConfig{{
			Name: "home", Path: "/",
			Backend: config.BackendConfig{Type: "dir", Path: filepath.Join(dir, "data")},
			Keyfile: filepath.Join(dir, "blocker", "k.key"),
			Meta:    filepath.Join(dir, "home.db"),
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, cfg, nil) }()
	time.Sleep(200 * time.Millisecond)

	select {
	case err := <-errCh:
		t.Fatalf("初始化失败不应导致 daemon 退出: %v", err)
	default:
	}
	conn, err := net.DialTimeout("tcp", cfg.Listen, time.Second)
	if err != nil {
		t.Fatalf("daemon 应仍在监听: %v", err)
	}
	conn.Close()
	cancel()
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Serve 未在取消后退出")
	}
}
