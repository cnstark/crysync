// cmd/crysyncd/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"crysync/internal/config"
	"crysync/internal/logging"
	"crysync/internal/server"
)

func main() {
	confPath := flag.String("config", "/etc/crysync/crysync.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*confPath)
	if err != nil {
		log.Fatalf("加载配置: %v", err)
	}
	// 持久化日志：log.file 为空时仅输出 stderr（同时写文件与 stderr）
	logger, err := logging.New(cfg.Log.File, cfg.Log.Level,
		cfg.Log.SizeLimitMB(), cfg.Log.RetainFiles())
	if err != nil {
		log.Fatalf("初始化日志: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := server.Serve(ctx, cfg, logger); err != nil {
		fmt.Fprintf(os.Stderr, "crysyncd: %v\n", err)
		os.Exit(1)
	}
}
