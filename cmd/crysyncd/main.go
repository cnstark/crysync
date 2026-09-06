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
	"crysync/internal/core"
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
	runtime := core.NewRuntime(cfg.Upload.MaxInflightChunks)
	var fronts []front.Front
	if cfg.Front.Rsync != nil {
		fronts = append(fronts, rsync.NewWithRuntime(cfg, logger, runtime))
	}
	if cfg.Front.WebDAV != nil {
		fronts = append(fronts, webdav.NewWithRuntime(cfg, logger, runtime))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// 在任何前端监听前完成模块初始化或灾难恢复，避免 key 存在而 Meta 缺失时
	// 短暂暴露一个空仓库。单模块失败沿用现有隔离语义，其他模块继续启动。
	ready := make(map[string]bool, len(cfg.Modules))
	for i := range cfg.Modules {
		m := &cfg.Modules[i]
		result, err := core.PrepareModule(m)
		if err != nil {
			logger.Error("module_init_error", "module", m.Name, "err", err.Error())
			continue
		}
		ready[m.Name] = true
		if result.KeyCreated {
			logger.Info("module_auto_init", "module", m.Name)
		}
		if result.MetaRestored {
			logger.Info("module_meta_restored", "module", m.Name)
		}
	}
	for i := range cfg.Modules {
		m := &cfg.Modules[i]
		if !ready[m.Name] {
			continue
		}
		go func() {
			if err := core.RunMetaBackupScheduler(ctx, m, logger); err != nil && ctx.Err() == nil {
				logger.Error("meta_backup_scheduler_error", "module", m.Name, "err", err.Error())
			}
		}()
	}
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
