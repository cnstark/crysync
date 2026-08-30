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
// 顺序要点：先完成模块缓存（OpenModule）+ buildMux，再 net.Listen——
// 保证监听建立时 mux 已就绪（若先监听后开模块，期间的并发请求会命中
// 默认 mux 返回 404 page not found，并行套件负载下是不可靠的就绪窗口）。
func (s *Server) Serve(ctx context.Context) error {
	wd := s.cfg.Front.WebDAV

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

	ln, err := net.Listen("tcp", wd.Listen)
	if err != nil {
		// 监听失败：关闭已打开的模块仓库，避免句柄泄漏
		//（正常退出不关闭——in-flight 请求可能仍在用，维持原语义）
		for _, mod := range modules {
			_ = mod.Close()
		}
		return fmt.Errorf("监听 %s: %w", wd.Listen, err)
	}
	defer ln.Close()
	s.logger.Info("webdav_start", "listen", wd.Listen, "modules", len(modules))

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
		// 301 重定向到 "/home/"（WebDAV 客户端访问根的标准形态）。
		// Handler.Prefix 设为模块名，由 x/net/webdav 统一剥离请求路径与
		// Destination 头的 /home 前缀（http.StripPrefix 只剥请求路径，不剥
		// MOVE/COPY 的 Destination 头，会导致目标路径带模块前缀而写失败）。
		prefix := "/" + m.Name
		h.Prefix = prefix
		// dirBrowse：浏览器 GET 目录渲染 HTML 文件列表（标准客户端走
		// PROPFIND 不受影响；x/net/webdav 对目录 GET 固定 405）
		mux.Handle(prefix+"/", auth(readOnlyGuard(m.ReadOnly, dirBrowse(h, mod.FileStore, prefix))))
	}
	// 虚拟根（catch-all）：挂载服务器根的客户端 PROPFIND / 可见模块列表
	//（按配置声明顺序，仅含已就绪模块）；未认证时先得 401 质询。
	// 此前根上无处理器：PROPFIND / 得裸 404，WebDAV 客户端报
	// "根文件夹不存在或无权限"。
	rootNames := make([]string, 0, len(modules))
	for i := range s.cfg.Modules {
		if _, ok := modules[s.cfg.Modules[i].Name]; ok {
			rootNames = append(rootNames, s.cfg.Modules[i].Name)
		}
	}
	mux.Handle("/", auth(&rootHandler{moduleNames: rootNames}))
	return mux
}
