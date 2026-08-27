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
	"time"

	"crysync/internal/config"
	"crysync/internal/core"
	"crysync/internal/core/meta"
	"crysync/internal/core/prune"
	"crysync/internal/core/types"
	"crysync/internal/front/rsync/protocol"
)

// rsyncService 组合 Session + FileStore，供协议会话按 RsyncService 使用。
// Session/FileStore 在 rsync 前端由同一 *repo.Repo 承担，两接口共享的读方法
// （ActiveSnapshotID/GetFileRow/StreamFile/ChunkSizeBytes）在此显式转发到
// Session 以消歧（嵌入两接口同名方法会产生歧义选择器）。
type rsyncService struct {
	types.Session
	types.FileStore
}

func (s *rsyncService) ActiveSnapshotID() (int64, error) { return s.Session.ActiveSnapshotID() }
func (s *rsyncService) GetFileRow(snapshotID int64, path string) (meta.FileRow, bool, error) {
	return s.Session.GetFileRow(snapshotID, path)
}
func (s *rsyncService) StreamFile(snapshotID int64, path string, seed int32, w io.Writer) (int64, [16]byte, error) {
	return s.Session.StreamFile(snapshotID, path, seed, w)
}
func (s *rsyncService) ChunkSizeBytes() int {
	return s.Session.ChunkSizeBytes()
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
	cfg    *config.Config
	logger *slog.Logger
}

// New 构造 rsync 前端（cfg.Front.Rsync 为 nil 时调用方不应构造）。
// logger 为 nil 时全部日志丢弃（向后兼容测试/嵌入用法）。
func New(cfg *config.Config, logger *slog.Logger) *Server {
	if logger == nil {
		logger = nopLogger
	}
	return &Server{cfg: cfg, logger: logger}
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

	// 启动时逐模块自动初始化（密钥+元数据，幂等）；失败仅记日志不中断监听，
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
	// prune 调度：每模块独立 goroutine，按 schedule 每日执行
	for i := range s.cfg.Modules {
		m := &s.cfg.Modules[i]
		if modulePrunePolicy(m) == (prune.Policy{}) {
			continue
		}
		wg.Add(1)
		go func(module *config.ModuleConfig) {
			defer wg.Done()
			runPruneLoop(ctx, module, s.logger)
		}(m)
	}
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
			if err := handleConn(ctx, c, s.cfg, s.logger, limiter); err != nil && !errors.Is(err, protocol.ErrClientClosed) {
				s.logger.Warn("conn_error", "client", c.RemoteAddr().String(), "err", err.Error())
			}
		}(conn)
	}
	return nil
}

// modulePrunePolicy 返回模块的保留策略（无配置时为全 0）。
func modulePrunePolicy(m *config.ModuleConfig) prune.Policy {
	if m.Prune == nil {
		return prune.Policy{}
	}
	return m.Prune.Policy()
}

// runPruneLoop 每模块每日调度：睡到下一个 schedule 时刻 -> 打开仓库执行 Prune。
func runPruneLoop(ctx context.Context, module *config.ModuleConfig, logger *slog.Logger) {
	schedule := "03:00"
	if module.Prune != nil && module.Prune.Schedule != "" {
		schedule = module.Prune.Schedule
	}
	for {
		if !sleepUntil(ctx, schedule) {
			return
		}
		if err := pruneOnce(module, logger); err != nil {
			logger.Error("prune_error", "module", module.Name, "err", err.Error())
		}
	}
}

// pruneOnce 打开模块仓库执行一次保留策略清理（删除被裁快照 + 孤儿 blob 回收）。
func pruneOnce(module *config.ModuleConfig, logger *slog.Logger) error {
	mod, err := core.OpenModule(module)
	if err != nil {
		return fmt.Errorf("打开仓库失败: %w", err)
	}
	defer mod.Close()
	removed, blobs, err := mod.Repo.Prune(modulePrunePolicy(module))
	if err != nil {
		return err
	}
	if removed > 0 || blobs > 0 {
		logger.Info("prune_done", "module", module.Name,
			"removed_snapshots", removed, "reclaimed_blobs", blobs)
	}
	return nil
}

// sleepUntil 睡到下一个 HH:MM 时刻；ctx 取消返回 false。
func sleepUntil(ctx context.Context, hhmm string) bool {
	h, m := 3, 0
	if _, err := fmt.Sscanf(hhmm, "%d:%d", &h, &m); err != nil {
		h, m = 3, 0
	}
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	timer := time.NewTimer(time.Until(next))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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
//（module/client/session 字段贯穿该会话全部日志事件）。
func handleConn(ctx context.Context, conn net.Conn, cfg *config.Config, logger *slog.Logger, limiter *moduleConnLimiter) error {
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
	mod, err := core.OpenModule(module)
	if err != nil {
		fmt.Fprintf(conn, "@ERROR: %v\n", err)
		return err
	}
	defer mod.Close()
	svc := &rsyncService{Session: mod.Session, FileStore: mod.FileStore}
	sess := logger.With("module", module.Name,
		"client", conn.RemoteAddr().String(), "session", newSessionID())
	// 方向由 RunSession 在 argv 协商后判定（--sender = 恢复）；只读模块拒绝推送
	return protocol.RunSessionWithReader(ctx, br, conn, module, svc, sess)
}
