package rsync

import (
	"testing"

	"crysync/internal/config"
)

// TestModuleConnLimiter（P2#15）：对齐 rsyncd max connections 语义——
// 0 = 无限制、负值 = 禁用模块、正数 = 上限；超限拒绝且不虚增计数（回退），
// 释放后名额恢复。
func TestModuleConnLimiter(t *testing.T) {
	// 正数上限
	l := &moduleConnLimiter{}
	m := &config.ModuleConfig{Name: "home", MaxConnections: 2}
	if !l.allow(m) || !l.allow(m) {
		t.Fatal("前 2 个连接应允许")
	}
	if l.allow(m) {
		t.Fatal("第 3 个连接应超限拒绝")
	}
	if l.allow(m) {
		t.Fatal("超限拒绝不应虚增计数（回退后仍应拒绝）")
	}
	l.release("home")
	if !l.allow(m) {
		t.Fatal("释放后名额应恢复")
	}

	// 0 = 无限制（缺省）
	l0 := &moduleConnLimiter{}
	m0 := &config.ModuleConfig{Name: "a"}
	for i := 0; i < 10; i++ {
		if !l0.allow(m0) {
			t.Fatal("max_connections=0 应无限制")
		}
	}

	// 负值 = 禁用模块
	ln := &moduleConnLimiter{}
	mn := &config.ModuleConfig{Name: "b", MaxConnections: -1}
	if ln.allow(mn) {
		t.Fatal("负值应禁用模块（恒拒绝）")
	}

	// 模块间计数独立
	lm := &moduleConnLimiter{}
	ma := &config.ModuleConfig{Name: "x", MaxConnections: 1}
	mb := &config.ModuleConfig{Name: "y", MaxConnections: 1}
	if !lm.allow(ma) || !lm.allow(mb) {
		t.Fatal("不同模块计数应独立")
	}
	if lm.allow(ma) || lm.allow(mb) {
		t.Fatal("各模块第 2 个连接应超限")
	}
}
