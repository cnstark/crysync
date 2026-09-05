// cmd/crysync/main.go
package main

import (
	"flag"
	"fmt"
	"os"

	"crysync/internal/config"
	"crysync/internal/core"
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
	default:
		usage()
		return fmt.Errorf("未知子命令: %s", args[0])
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法: crysync <init|version> [选项]")
	fmt.Fprintln(os.Stderr, "  init     显式初始化模块密钥与元数据库（可选，daemon 启动自动执行；--config）")
	fmt.Fprintln(os.Stderr, "  version  打印版本号")
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