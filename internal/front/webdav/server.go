// internal/front/webdav/server.go
// WebDAV 前端：HTTP 监听、Basic 认证、模块路由（每模块一个 x/net/webdav
// Handler）、模块仓库缓存。
package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/webdav"

	"crysync/internal/config"
	"crysync/internal/core"
)

const moduleOpenCooldown = 30 * time.Second // 懒打开失败冷却（与后台退避上限一致）

type Server struct {
	cfg     *config.Config
	logger  *slog.Logger
	runtime *core.Runtime
	// openCooldown 懒打开失败冷却期（集成测试可缩短；生产用默认值）。
	openCooldown time.Duration
}

// New 构造 WebDAV 前端（cfg.Front.WebDAV 为 nil 时调用方不应构造）。
func New(cfg *config.Config, logger *slog.Logger) *Server {
	return NewWithRuntime(cfg, logger, core.NewRuntime(cfg.Upload.MaxInflightChunks))
}

func NewWithRuntime(cfg *config.Config, logger *slog.Logger, runtime *core.Runtime) *Server {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if runtime == nil {
		runtime = core.NewRuntime(cfg.Upload.MaxInflightChunks)
	}
	return &Server{cfg: cfg, logger: logger, runtime: runtime, openCooldown: moduleOpenCooldown}
}

func (s *Server) Name() string { return "webdav" }

// Serve 监听并服务 WebDAV 请求，直到 ctx 取消。
// 顺序要点：先完成模块缓存（OpenModule）+ buildMux，再 net.Listen——
// 保证监听建立时 mux 已就绪；打开失败的模块由请求路径懒打开自愈。
func (s *Server) Serve(ctx context.Context) error {
	wd := s.cfg.Front.WebDAV

	// 模块仓库缓存：启动时预填充成功打开的模块；失败记日志跳过，
	// 该模块的后续请求经懒打开自愈（成功自动入缓存，未就绪 503）。
	order := make([]string, 0, len(s.cfg.Modules))
	for i := range s.cfg.Modules {
		order = append(order, s.cfg.Modules[i].Name)
	}
	cache := newModuleCache(s.openCooldown, order)
	var opened []*core.Module
	for i := range s.cfg.Modules {
		m := &s.cfg.Modules[i]
		mod, err := s.runtime.OpenModuleWithLogger(m, s.logger)
		if err != nil {
			s.logger.Error("module_open_error", "module", m.Name, "err", err.Error())
			continue
		}
		opened = append(opened, mod)
		cache.preload(m.Name, s.buildModuleHandler(m, mod))
	}
	srv := &http.Server{Handler: requestTrace(s.buildMux(cache), s.logger)}

	ln, err := net.Listen("tcp", wd.Listen)
	if err != nil {
		// 监听失败：关闭已打开的模块仓库，避免句柄泄漏。
		for _, mod := range opened {
			_ = mod.Close()
		}
		return fmt.Errorf("监听 %s: %w", wd.Listen, err)
	}
	defer ln.Close()
	s.logger.Info("webdav_start", "listen", wd.Listen, "modules", len(cache.names()))

	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	err = srv.Serve(ln)
	if err != nil && !errors.Is(err, http.ErrServerClosed) && ctx.Err() == nil {
		return err
	}
	return nil
}

// buildModuleHandler 构造模块的完整 handler 链（懒打开成功后调用一次并缓存）。
func (s *Server) buildModuleHandler(m *config.ModuleConfig, mod *core.Module) http.Handler {
	prefix := "/" + m.Name
	h := &webdav.Handler{
		FileSystem: &moduleFS{
			store:       mod.FileStore,
			writer:      mod.FileWriter,
			readOnly:    m.ReadOnly,
			urlRoot:     prefix,
			rootName:    m.Name,
			rootModTime: time.Now().UTC(),
		},
		LockSystem: noopLockSystem{},
		Logger: func(r *http.Request, err error) {
			if err != nil {
				s.logger.Warn("webdav_request_error", "method", r.Method, "path", r.URL.Path, "err", err.Error())
			}
		},
	}
	// 子树 pattern："/home/" 与 "/home/a.txt" 都命中；"/home" 由 ServeMux
	// 301 重定向到 "/home/"。不设置 Handler.Prefix：让 x/net/webdav 保留
	// /home 这段路径，并由 moduleFS 剥离它。这样模块根在 PROPFIND 中是
	// 有名字的集合，而不是 x/net 特意隐藏名称的处理器根目录。
	// dirBrowse：浏览器 GET 目录渲染 HTML 文件列表，标准客户端走 PROPFIND
	// 不受影响；idempotentDelete：短路 DELETE。
	return readOnlyGuard(m.ReadOnly,
		idempotentDelete(dirBrowse(streamingPut(h, mod.FileWriter, prefix), mod.FileStore, prefix), mod.FileWriter, prefix))
}

func (s *Server) buildMux(cache *moduleCache) http.Handler {
	mux := http.NewServeMux()
	auth := basicAuth(s.cfg.Front.WebDAV.Auth.Users)
	for i := range s.cfg.Modules {
		m := &s.cfg.Modules[i]
		prefix := "/" + m.Name
		mux.Handle(prefix+"/", auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h, first, err := cache.open(m.Name,
				func() (*core.Module, error) { return s.runtime.OpenModuleWithLogger(m, s.logger) },
				func(mod *core.Module) http.Handler { return s.buildModuleHandler(m, mod) })
			if err != nil {
				if first {
					s.logger.Warn("module_open_error", "module", m.Name, "err", err.Error())
				}
				w.Header().Set("Retry-After", "30")
				http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
				return
			}
			h.ServeHTTP(w, r)
		})))
	}
	// 虚拟根（catch-all）：挂载服务器根的客户端 PROPFIND / 可见模块列表
	//（按配置声明顺序，仅含已就绪模块；懒打开成功后自动出现）；未认证时先得 401。
	mux.Handle("/", auth(&rootHandler{names: cache.names}))
	return mux
}
