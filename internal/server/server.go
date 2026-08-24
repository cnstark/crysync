// internal/server/server.go
package server

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
	"os"
	"sync"
	"time"

	"crysync/internal/backend"
	"crysync/internal/config"
	"crysync/internal/crypto"
	"crysync/internal/meta"
	"crysync/internal/prune"
	"crysync/internal/repo"
	"crysync/internal/rsyncproto"
)

// OpenRepoForModule 打开模块仓库：校验密钥、打开元数据、构造后端。
func OpenRepoForModule(module *config.ModuleConfig) (*repo.Repo, func() error, error) {
	if _, err := os.Stat(module.Keyfile); err != nil {
		return nil, nil, fmt.Errorf("模块 %s 密钥文件不可用（先运行 crysync init）: %w", module.Name, err)
	}
	key, err := crypto.LoadKeyFile(module.Keyfile)
	if err != nil {
		return nil, nil, err
	}
	db, err := meta.Open(module.Meta)
	if err != nil {
		return nil, nil, err
	}
	var be backend.Backend
	switch module.Backend.Type {
	case "dir":
		be, err = backend.NewDir(module.Backend.Path)
	case "webdav":
		be, err = backend.NewWebDAV(module.Backend.URL, module.Backend.Username, module.Backend.Password)
	default:
		db.Close()
		return nil, nil, fmt.Errorf("模块 %s: 未知后端类型 %q", module.Name, module.Backend.Type)
	}
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	if err := be.Ping(); err != nil {
		db.Close()
		return nil, nil, fmt.Errorf("模块 %s 后端不可用: %w", module.Name, err)
	}
	r := repo.New(db, be, key, module.ChunkSizeBytes())
	return r, func() error { return db.Close() }, nil
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

// Serve 监听并服务 rsync 连接，直到 ctx 取消；配置了 prune 的模块启动每日调度。
// logger 为 nil 时全部日志丢弃（向后兼容测试/嵌入用法）。
func Serve(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	if logger == nil {
		logger = nopLogger
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("监听 %s: %w", cfg.Listen, err)
	}
	defer ln.Close()
	logger.Info("daemon_start", "listen", cfg.Listen, "modules", len(cfg.Modules))

	var wg sync.WaitGroup
	defer wg.Wait()
	// prune 调度：每模块独立 goroutine，按 schedule 每日执行
	for i := range cfg.Modules {
		m := &cfg.Modules[i]
		if modulePrunePolicy(m) == (prune.Policy{}) {
			continue
		}
		wg.Add(1)
		go func(module *config.ModuleConfig) {
			defer wg.Done()
			runPruneLoop(ctx, module, logger)
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
			logger.Warn("conn_error", "err", err.Error())
			continue
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			c.SetDeadline(time.Now().Add(24 * time.Hour))
			if err := handleConn(ctx, c, cfg, logger); err != nil && !errors.Is(err, rsyncproto.ErrClientClosed) {
				logger.Warn("conn_error", "client", c.RemoteAddr().String(), "err", err.Error())
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
	r, closeRepo, err := OpenRepoForModule(module)
	if err != nil {
		return fmt.Errorf("打开仓库失败: %w", err)
	}
	defer closeRepo()
	removed, blobs, err := r.Prune(modulePrunePolicy(module))
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

// handleConn：handshake 与协议共享同一个 bufio.Reader（buffer 可能预读协议字节，
// 必须传给后续协议解析，否则预读字节丢失）。握手成功后派生会话级 logger
//（module/client/session 字段贯穿该会话全部日志事件）。
func handleConn(ctx context.Context, conn net.Conn, cfg *config.Config, logger *slog.Logger) error {
	br := bufio.NewReader(conn)
	module, err := rsyncproto.HandleModuleRequest(br, conn, cfg)
	if err != nil {
		return err
	}
	r, closeRepo, err := OpenRepoForModule(module)
	if err != nil {
		fmt.Fprintf(conn, "@ERROR: %v\n", err)
		return err
	}
	defer closeRepo()
	sess := logger.With("module", module.Name,
		"client", conn.RemoteAddr().String(), "session", newSessionID())
	// 方向由 RunSession 在 argv 协商后判定（--sender = 恢复）；只读模块拒绝推送
	return rsyncproto.RunSessionWithReader(ctx, br, conn, module, r, sess)
}
