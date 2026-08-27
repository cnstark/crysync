// cmd/crysync/main.go
package main

import (
	"flag"
	"fmt"
	"os"

	"crysync/internal/backend"
	"crysync/internal/config"
	"crysync/internal/core"
	"crysync/internal/core/prune"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "crysync: %v\n", err)
		os.Exit(1)
	}
}

// version 由构建时注入（ldflags -X main.version=<tag>），本地构建缺省 dev。
var version = "dev"

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("缺少子命令")
	}
	switch args[0] {
	case "version", "--version", "-v":
		fmt.Printf("crysync %s\n", version)
		return nil
	case "init":
		return cmdInit(args[1:])
	case "snapshots":
		return cmdSnapshots(args[1:])
	case "prune":
		return cmdPrune(args[1:])
	default:
		usage()
		return fmt.Errorf("未知子命令: %s", args[0])
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法: crysync <init|snapshots|prune|version> [选项]")
	fmt.Fprintln(os.Stderr, "  init       显式初始化模块密钥与元数据库（可选，daemon 启动自动执行；--config）")
	fmt.Fprintln(os.Stderr, "  snapshots  列出模块快照（--config [--module NAME]）")
	fmt.Fprintln(os.Stderr, "  prune      手动执行模块保留策略与孤儿 blob 回收（--config --module NAME）")
	fmt.Fprintln(os.Stderr, "  version    打印版本号")
}

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	confPath := fs.String("config", "", "配置文件路径")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *confPath == "" {
		return fmt.Errorf("init 需要 --config")
	}
	cfg, err := config.Load(*confPath)
	if err != nil {
		return err
	}
	// 与 daemon 启动共用同一套初始化逻辑（幂等；密钥缺失但元数据库已存在时拒绝）
	for i := range cfg.Modules {
		m := &cfg.Modules[i]
		autoInit, err := core.EnsureModuleInit(m)
		if err != nil {
			return fmt.Errorf("模块 %s: %w", m.Name, err)
		}
		if autoInit {
			fmt.Printf("模块 %s: 密钥已生成，元数据已初始化\n", m.Name)
		} else {
			fmt.Printf("模块 %s: 密钥已存在，元数据已初始化\n", m.Name)
		}
	}
	return nil
}

// loadModules 加载配置并过滤出目标模块（--module 未指定时全部）。
func loadModules(confPath, moduleName string) (*config.Config, []*config.ModuleConfig, error) {
	if confPath == "" {
		return nil, nil, fmt.Errorf("需要 --config")
	}
	cfg, err := config.Load(confPath)
	if err != nil {
		return nil, nil, err
	}
	var out []*config.ModuleConfig
	for i := range cfg.Modules {
		m := &cfg.Modules[i]
		if moduleName != "" && m.Name != moduleName {
			continue
		}
		out = append(out, m)
	}
	if moduleName != "" && len(out) == 0 {
		return nil, nil, fmt.Errorf("模块 %s 不存在", moduleName)
	}
	return cfg, out, nil
}

// cmdSnapshots 列出快照：每个模块一行表头 + 快照行（id、时间、文件数、活跃标记）。
func cmdSnapshots(args []string) error {
	fs := flag.NewFlagSet("snapshots", flag.ExitOnError)
	confPath := fs.String("config", "", "配置文件路径")
	moduleName := fs.String("module", "", "模块名（缺省全部）")
	setActive := fs.Int64("set-active", 0, "将指定快照设为活跃（恢复时间点）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, modules, err := loadModules(*confPath, *moduleName)
	if err != nil {
		return err
	}
	for _, m := range modules {
		mod, err := core.OpenModule(m)
		if err != nil {
			return fmt.Errorf("模块 %s: %w", m.Name, err)
		}
		defer mod.Close()
		snaps, err := mod.Repo.SnapshotList()
		if err != nil {
			return err
		}
		active, err := mod.Repo.ActiveSnapshotID()
		if err != nil {
			return err
		}
		// --set-active：切换恢复时间点（设计文档 §5.2）
		if *setActive > 0 {
			found := false
			for _, s := range snaps {
				if s.ID == *setActive {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("模块 %s: 快照 %d 不存在", m.Name, *setActive)
			}
			if err := mod.Repo.SetActiveSnapshot(*setActive); err != nil {
				return err
			}
			fmt.Printf("模块 %s: 活跃快照已切换为 %d\n", m.Name, *setActive)
			continue
		}
		fmt.Printf("模块 %s（%d 个快照）：\n", m.Name, len(snaps))
		fmt.Printf("  %-4s %-26s %-8s %s\n", "ID", "创建时间", "文件数", "活跃")
		for _, s := range snaps {
			n, err := mod.Repo.SnapshotFileCount(s.ID)
			if err != nil {
				return err
			}
			mark := ""
			if s.ID == active {
				mark = "*"
			}
			fmt.Printf("  %-4d %-26s %-8d %s\n", s.ID, s.CreatedAt.Local().Format("2006-01-02 15:04:05"), n, mark)
		}
	}
	return nil
}

// cmdPrune 手动执行模块保留策略清理 + 孤儿 blob 回收。
func cmdPrune(args []string) error {
	fs := flag.NewFlagSet("prune", flag.ExitOnError)
	confPath := fs.String("config", "", "配置文件路径")
	moduleName := fs.String("module", "", "模块名")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, modules, err := loadModules(*confPath, *moduleName)
	if err != nil {
		return err
	}
	for _, m := range modules {
		mod, err := core.OpenModule(m)
		if err != nil {
			return fmt.Errorf("模块 %s: %w", m.Name, err)
		}
		removed, blobs, err := mod.Repo.Prune(modulePolicy(m))
		mod.Close()
		if err != nil {
			return fmt.Errorf("模块 %s: %w", m.Name, err)
		}
		fmt.Printf("模块 %s: 删除 %d 个快照，回收 %d 个 blob\n", m.Name, removed, blobs)
	}
	return nil
}

// modulePolicy 模块保留策略（无配置时全 0 = 不删除）。
func modulePolicy(m *config.ModuleConfig) prune.Policy {
	if m.Prune == nil {
		return prune.Policy{}
	}
	return m.Prune.Policy()
}

var _ = backend.NewInMemory // 保留引用：后续子命令使用后端
