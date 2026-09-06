package core

import (
	"path/filepath"
	"testing"

	"crysync/internal/config"
)

func TestRuntimeSharesInflightLimiterAcrossModules(t *testing.T) {
	root := t.TempDir()
	module := func(name string) *config.ModuleConfig {
		return &config.ModuleConfig{
			Name:      name,
			Backend:   config.BackendConfig{Type: "dir", Path: filepath.Join(root, name, "data")},
			Keyfile:   filepath.Join(root, name, "key"),
			Meta:      filepath.Join(root, name, "meta.db"),
			ChunkSize: 64,
		}
	}
	rt := NewRuntime(3)
	a, err := rt.OpenModule(module("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := rt.OpenModule(module("b"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if a.Repo.InflightLimiter() == nil || a.Repo.InflightLimiter() != b.Repo.InflightLimiter() {
		t.Fatal("同一 Runtime 打开的模块必须共享同一个 inflight 限制器")
	}
}
