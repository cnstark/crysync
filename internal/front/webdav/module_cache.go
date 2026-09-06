// internal/front/webdav/module_cache.go
// 模块仓库缓存：启动预填充 + 请求懒打开。懒打开 per-module 单飞
// （并发请求共享同一次打开，防止请求风暴），失败进入冷却期
// （冷却期内请求直接 503，不重试）。就绪模块名按配置声明顺序输出。
package webdav

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"crysync/internal/core"
)

// errModuleNotReady 模块未就绪（懒打开失败或冷却期内），由调用方映射 503。
var errModuleNotReady = errors.New("模块未就绪")

type moduleEntry struct {
	mu            sync.Mutex // 保护 handler/冷却/状态；打开期间持有，天然单飞
	handler       http.Handler
	cooldownUntil time.Time
	failing       bool // 上一轮打开失败（状态变化判定：新一轮失败的首次）
}

type moduleCache struct {
	cooldown time.Duration
	order    []string
	entries  map[string]*moduleEntry
}

func newModuleCache(cooldown time.Duration, order []string) *moduleCache {
	c := &moduleCache{
		cooldown: cooldown,
		order:    order,
		entries:  make(map[string]*moduleEntry, len(order)),
	}
	for _, name := range order {
		c.entries[name] = &moduleEntry{}
	}
	return c
}

// preload 启动预填充（仅用于启动时成功打开的模块，无冷却语义）。
func (c *moduleCache) preload(name string, h http.Handler) {
	if e, ok := c.entries[name]; ok {
		e.mu.Lock()
		e.handler = h
		e.failing = false
		e.mu.Unlock()
	}
}

// open 返回模块 handler。未就绪时：冷却期外触发懒打开（单飞），
// 失败记录冷却并返回 errModuleNotReady；冷却期内静默失败。
// firstFailure=true 表示新一轮失败开始（调用方据此记一次 WARN）。
// openOne 由调用方注入；build 在打开成功后调用一次构造 handler 链并缓存。
func (c *moduleCache) open(name string, openOne func() (*core.Module, error), build func(*core.Module) http.Handler) (http.Handler, bool, error) {
	e, ok := c.entries[name]
	if !ok {
		return nil, false, errModuleNotReady
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.handler != nil {
		return e.handler, false, nil
	}
	now := time.Now()
	if now.Before(e.cooldownUntil) {
		return nil, false, errModuleNotReady
	}
	// 冷却期结束后进入新一轮探测，下一次失败应重新记录状态变化。
	e.failing = false
	mod, err := openOne()
	if err != nil {
		first := !e.failing
		e.failing = true
		e.cooldownUntil = now.Add(c.cooldown)
		return nil, first, errModuleNotReady
	}
	e.handler = build(mod)
	e.failing = false
	e.cooldownUntil = time.Time{}
	return e.handler, false, nil
}

// names 返回就绪（handler 非 nil）模块名，按配置声明顺序。
func (c *moduleCache) names() []string {
	out := make([]string, 0, len(c.order))
	for _, name := range c.order {
		e := c.entries[name]
		e.mu.Lock()
		ready := e.handler != nil
		e.mu.Unlock()
		if ready {
			out = append(out, name)
		}
	}
	return out
}
