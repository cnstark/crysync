// internal/front/rsync/server.go
package rsync

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"crysync/internal/config"
	"crysync/internal/core"
	"crysync/internal/core/meta"
	"crysync/internal/core/types"
	"crysync/internal/front/rsync/protocol"
	"crysync/internal/trace"
)

// rsyncService 组合 Session + FileStore，供协议会话按 RsyncService 使用。
// Session/FileStore 在 rsync 前端由同一 *repo.Repo 承担，两接口共享的读方法
// （GetFileRow/StreamFile/ChunkSizeBytes/FileRows）在此显式转发到 Session
// 以消歧（嵌入两接口同名方法会产生歧义选择器）。
type rsyncService struct {
	types.Session
	types.FileStore
}

func (s *rsyncService) StoreChunkContext(ctx context.Context, data []byte) (int64, bool, error) {
	if cs, ok := s.Session.(types.ContextSession); ok {
		return cs.StoreChunkContext(ctx, data)
	}
	return s.Session.StoreChunk(data)
}

func (s *rsyncService) GetFileRow(path string) (meta.FileRow, bool, error) {
	return s.Session.GetFileRow(path)
}
func (s *rsyncService) StreamFile(path string, seed int32, w io.Writer) (int64, [16]byte, error) {
	return s.Session.StreamFile(path, seed, w)
}
func (s *rsyncService) ChunkSizeBytes() int {
	return s.Session.ChunkSizeBytes()
}
func (s *rsyncService) FileRows(prefix string) ([]meta.FileRow, error) {
	return s.Session.FileRows(prefix)
}
func (s *rsyncService) WriteSessionLock() func() {
	return s.Session.WriteSessionLock()
}

// nopLogger：未注入 logger 时的兜底（丢弃全部日志）。
var nopLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// newSessionID 生成 4 hex 随机会话标识（日志关联同一连接的全部事件）。
func newSessionID() string {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000"
	}
	return hex.EncodeToString(b[:])
}

// Server rsync 前端服务：监听并服务 rsync 协议连接（cfg.Front.Rsync 提供 listen/auth）。
type Server struct {
	cfg     *config.Config
	logger  *slog.Logger
	runtime *core.Runtime
}

// New 构造 rsync 前端（cfg.Front.Rsync 为 nil 时调用方不应构造）。
// logger 为 nil 时全部日志丢弃（向后兼容测试/嵌入用法）。
func New(cfg *config.Config, logger *slog.Logger) *Server {
	return NewWithRuntime(cfg, logger, core.NewRuntime(cfg.Upload.MaxInflightChunks))
}

func NewWithRuntime(cfg *config.Config, logger *slog.Logger, runtime *core.Runtime) *Server {
	if logger == nil {
		logger = nopLogger
	}
	if runtime == nil {
		runtime = core.NewRuntime(cfg.Upload.MaxInflightChunks)
	}
	return &Server{cfg: cfg, logger: logger, runtime: runtime}
}

func (s *Server) Name() string { return "rsync" }

// Serve 监听并服务 rsync 连接，直到 ctx 取消；配置了 prune 的模块启动每日调度。
func (s *Server) Serve(ctx context.Context) error {
	rc := s.cfg.Front.Rsync
	ln, err := net.Listen("tcp", rc.Listen)
	if err != nil {
		return fmt.Errorf("监听 %s: %w", rc.Listen, err)
	}
	defer ln.Close()
	s.logger.Info("daemon_start", "listen", rc.Listen, "modules", len(s.cfg.Modules))

	// 启动时逐模块初始化或恢复（幂等）；失败仅记日志不中断监听，
	// 连接路径 core.OpenModule 会重试并通过 @ERROR 反馈客户端
	for i := range s.cfg.Modules {
		autoInit, err := core.EnsureModuleInit(&s.cfg.Modules[i])
		if err != nil {
			s.logger.Error("module_init_error", "module", s.cfg.Modules[i].Name, "err", err.Error())
			continue
		}
		if autoInit {
			s.logger.Info("module_auto_init", "module", s.cfg.Modules[i].Name)
		}
	}

	var wg sync.WaitGroup
	defer wg.Wait()
	// 模块并发连接计数（对齐 rsyncd max connections：0 = 无限制、负值 = 禁用模块、
	// 正数 = 上限；claim_connection 语义，连接结束释放）
	limiter := &moduleConnLimiter{}
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			s.logger.Warn("conn_error", "err", err.Error())
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			c.SetDeadline(time.Now().Add(24 * time.Hour))
			if err := handleConn(ctx, c, s.cfg, s.logger, limiter, s.runtime); err != nil && !errors.Is(err, protocol.ErrClientClosed) {
				if isLocalProbeDisconnect(c, err) {
					s.logger.Debug("conn_closed", "client", c.RemoteAddr().String(), "err", err.Error())
				} else {
					s.logger.Warn("conn_error", "client", c.RemoteAddr().String(), "err", err.Error())
				}
			}
		}(conn)
	}
	return nil
}

// isLocalProbeDisconnect identifies the TCP connect-and-close pattern used by
// the container health check. It is expected traffic, not a failed rsync
// session, and therefore belongs at debug level.
func isLocalProbeDisconnect(c net.Conn, err error) bool {
	if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		return false
	}
	host, _, splitErr := net.SplitHostPort(c.RemoteAddr().String())
	ip := net.ParseIP(host)
	return splitErr == nil && ip != nil && ip.IsLoopback()
}

// moduleConnLimiter：模块并发连接计数。allow 在模块选定后、认证前占坑（须与
// release 配对，由 handleConn 的 defer 保证）；计数随 Serve 生命周期（每次监听独立）。
type moduleConnLimiter struct {
	counts sync.Map // 模块名 -> *atomic.Int64
}

// allow 返回模块是否接受新连接；接受时已占坑。
func (l *moduleConnLimiter) allow(m *config.ModuleConfig) bool {
	if m.MaxConnections == 0 {
		return true // 无限制（缺省）
	}
	if m.MaxConnections < 0 {
		return false // 负值禁用模块（rsyncd.conf.5.md：for 循环体不执行恒失败）
	}
	v, _ := l.counts.LoadOrStore(m.Name, &atomic.Int64{})
	n := v.(*atomic.Int64)
	if n.Add(1) <= int64(m.MaxConnections) {
		return true
	}
	n.Add(-1) // 超限回退，防计数虚高导致永久拒绝
	return false
}

// release 连接结束时释放一个占坑。
func (l *moduleConnLimiter) release(name string) {
	if v, ok := l.counts.Load(name); ok {
		v.(*atomic.Int64).Add(-1)
	}
}

// handleConn：handshake 与协议共享同一个 bufio.Reader（buffer 可能预读协议字节，
// 必须传给后续协议解析，否则预读字节丢失）。握手成功后派生会话级 logger
// （module/client/session 字段贯穿该会话全部日志事件）。
func handleConn(ctx context.Context, conn net.Conn, cfg *config.Config, logger *slog.Logger, limiter *moduleConnLimiter, runtime *core.Runtime) error {
	// 握手阶段（greeting→模块选择→认证→argv 协商）读超时上限：防半开/挂死
	// 客户端占住连接 24h（rsync 3.5.0 DAEMON_HANDSHAKE_TIMEOUT=60 同旨）；
	// 协商完成后由 protocol.applyIoTimeout 覆盖（--timeout=N 滚动或恢复 24h）
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	br := bufio.NewReader(conn)
	module, err := protocol.HandleModuleRequest(br, conn, cfg, limiter.allow)
	if err != nil {
		return err
	}
	defer limiter.release(module.Name)
	sessionID := newSessionID()
	sess := logger.With("module", module.Name,
		"client", conn.RemoteAddr().String(), "session", sessionID)
	ctx = trace.WithID(ctx, sessionID)
	mod, err := runtime.OpenModuleWithLogger(module, sess)
	if err != nil {
		fmt.Fprintf(conn, "@ERROR: %v\n", err)
		return err
	}
	defer mod.Close()
	svc := &rsyncService{Session: mod.Session, FileStore: mod.FileStore}
	// 方向由 RunSession 在 argv 协商后判定（--sender = 恢复）；只读模块拒绝推送
	return protocol.RunSessionWithReader(ctx, br, conn, module, svc, sess)
}
