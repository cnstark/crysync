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

	"golang.org/x/net/webdav"

	"crysync/internal/config"
	"crysync/internal/core"
)

type Server struct {
	cfg    *config.Config
	logger *slog.Logger
}

// New 构造 WebDAV 前端（cfg.Front.WebDAV 为 nil 时调用方不应构造）。
func New(cfg *config.Config, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{cfg: cfg, logger: logger}
}

func (s *Server) Name() string { return "webdav" }

// Serve 监听并服务 WebDAV 请求，直到 ctx 取消。
func (s *Server) Serve(ctx context.Context) error {
	wd := s.cfg.Front.WebDAV
	ln, err := net.Listen("tcp", wd.Listen)
	if err != nil {
		return fmt.Errorf("监听 %s: %w", wd.Listen, err)
	}
	defer ln.Close()
	s.logger.Info("webdav_start", "listen", wd.Listen, "modules", len(s.cfg.Modules))

	// 模块仓库缓存：启动时逐模块打开（失败记日志跳过，该模块请求 404）
	modules := map[string]*core.Module{}
	for i := range s.cfg.Modules {
		m := &s.cfg.Modules[i]
		mod, err := core.OpenModule(m)
		if err != nil {
			s.logger.Error("module_open_error", "module", m.Name, "err", err.Error())
			continue
		}
		modules[m.Name] = mod
	}

	srv := &http.Server{Handler: s.buildMux(modules)}
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

func (s *Server) buildMux(modules map[string]*core.Module) http.Handler {
	mux := http.NewServeMux()
	auth := basicAuth(s.cfg.Front.WebDAV.Auth.Users)
	for i := range s.cfg.Modules {
		m := &s.cfg.Modules[i]
		mod, ok := modules[m.Name]
		if !ok {
			continue
		}
		h := &webdav.Handler{
			FileSystem: &moduleFS{store: mod.FileStore, writer: mod.FileWriter, readOnly: m.ReadOnly},
			LockSystem: noopLockSystem{},
			Logger: func(r *http.Request, err error) {
				if err != nil {
					s.logger.Warn("webdav_request_error", "method", r.Method, "path", r.URL.Path, "err", err.Error())
				}
			},
		}
		// 子树 pattern："/home/" 与 "/home/a.txt" 都命中；"/home" 由 ServeMux
		// 301 重定向到 "/home/"（WebDAV 客户端访问根的标准形态）
		prefix := "/" + m.Name
		mux.Handle(prefix+"/", auth(readOnlyGuard(m.ReadOnly, http.StripPrefix(prefix, h))))
	}
	return mux
}
