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
	"crysync/internal/front"
	"crysync/internal/front/rsync"
	"crysync/internal/front/webdav"
	"crysync/internal/logging"
)

// version 由构建时注入（ldflags -X main.version=<tag>），本地构建缺省 dev。
var version = "dev"

func main() {
	confPath := flag.String("config", "/etc/crysync/crysync.yaml", "配置文件路径")
	showVersion := flag.Bool("version", false, "打印版本号并退出")
	flag.Parse()

	if *showVersion {
		fmt.Printf("crysyncd %s\n", version)
		return
	}

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

	// 前端层组装：配置了哪个前端就启动哪个（至少一个，Validate 已保证）
	var fronts []front.Front
	if cfg.Front.Rsync != nil {
		fronts = append(fronts, rsync.New(cfg, logger))
	}
	if cfg.Front.WebDAV != nil {
		fronts = append(fronts, webdav.New(cfg, logger))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, len(fronts))
	for _, f := range fronts {
		go func(f front.Front) {
			errCh <- f.Serve(ctx)
		}(f)
	}
	select {
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "crysyncd: %v\n", err)
			os.Exit(1)
		}
	case <-ctx.Done():
	}
}
