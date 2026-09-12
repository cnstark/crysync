package core

import (
	"log/slog"

	"crysync/internal/config"
	"crysync/internal/core/repo"
)

// Runtime 是一个 crysyncd 进程的共享运行时资源集合。
type Runtime struct {
	inflight *repo.InflightLimiter
}

func NewRuntime(maxInflightChunks int) *Runtime {
	if maxInflightChunks <= 0 {
		maxInflightChunks = config.DefaultMaxInflightChunks
	}
	return &Runtime{inflight: repo.NewInflightLimiter(maxInflightChunks)}
}

// OpenModule 使用当前 Runtime 打开模块，使所有前端共享在途槽位。
func (rt *Runtime) OpenModule(module *config.ModuleConfig) (*Module, error) {
	return openModule(module, rt, nil)
}

// OpenModuleWithLogger is the Runtime equivalent of OpenModuleWithLogger.
func (rt *Runtime) OpenModuleWithLogger(module *config.ModuleConfig, logger *slog.Logger) (*Module, error) {
	return openModule(module, rt, logger)
}
