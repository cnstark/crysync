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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"crysync/internal/backend"
	"crysync/internal/config"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
	"crysync/internal/core/prune"
	"crysync/internal/core/repo"
	"crysync/internal/front/rsync/protocol"
)

// OpenRepoForModule 打开模块仓库：必要时自动初始化（密钥+元数据）、
// 校验密钥、打开元数据、构造后端。
func OpenRepoForModule(module *config.ModuleConfig) (*repo.Repo, func() error, error) {
	if _, err := ensureKeyfile(module); err != nil {
		return nil, nil, err
	}
	key, err := crypto.LoadKeyFile(module.Keyfile)
	if err != nil {
		return nil, nil, err
	}
	// meta.Open 幂等：meta 文件不存在时自动建目录与 schema，已存在时直接打开
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

// keyfileMutexes 同进程内按密钥路径串行化自动初始化（多个连接同时首开同一模块）。
var keyfileMutexes sync.Map // keyfile 路径 -> *sync.Mutex

// ensureKeyfile 确保模块密钥文件就绪，返回是否新创建了密钥（供调用方记日志）：
//   - 密钥已存在：直接返回；
//   - 密钥缺失但元数据库已存在：拒绝自动初始化（keys 卷丢失/未挂载时静默换钥
//     会让旧快照引用的 blob 永久无法解密），报错交由人工决策；
//   - 两者均不存在：自动生成密钥（部署自举，等价于自动执行 crysync init）。
func ensureKeyfile(module *config.ModuleConfig) (bool, error) {
	exists := func(path string) (bool, error) {
		_, err := os.Stat(path)
		if err == nil {
			return true, nil
		}
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if ok, err := exists(module.Keyfile); err != nil {
		return false, fmt.Errorf("模块 %s 检查密钥文件: %w", module.Name, err)
	} else if ok {
		return false, nil
	}
	mu, _ := keyfileMutexes.LoadOrStore(module.Keyfile, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()
	// 持锁后复查：并发连接可能已创建
	if ok, err := exists(module.Keyfile); err != nil {
		return false, fmt.Errorf("模块 %s 检查密钥文件: %w", module.Name, err)
	} else if ok {
		return false, nil
	}
	if ok, err := exists(module.Meta); err != nil {
		return false, fmt.Errorf("模块 %s 检查元数据库: %w", module.Name, err)
	} else if ok {
		return false, fmt.Errorf("模块 %s: 元数据库已存在但密钥文件缺失（密钥卷丢失或未挂载？），"+
			"为避免旧快照无法解密拒绝自动生成新密钥；请恢复密钥文件，或确认放弃旧数据后删除 %s",
			module.Name, module.Meta)
	}
	k, err := crypto.GenerateKey()
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(module.Keyfile), 0o700); err != nil {
		return false, fmt.Errorf("创建密钥目录: %w", err)
	}
	// O_EXCL 兜底跨进程竞态（如 CLI init 与 daemon 同时首建）：沿用已存在密钥
	created, err := crypto.SaveKeyFileExclusive(module.Keyfile, k)
	if err != nil {
		return false, err
	}
	return created, nil
}

// EnsureModuleInit 确保模块密钥与元数据库就绪（幂等；daemon 启动与 CLI init 共用）：
// ensureKeyfile + meta.Open 建库。返回是否新创建了密钥（供调用方记日志/打印）。
func EnsureModuleInit(module *config.ModuleConfig) (bool, error) {
	autoInit, err := ensureKeyfile(module)
	if err != nil {
		return false, err
	}
	db, err := meta.Open(module.Meta)
	if err != nil {
		return false, err
	}
	return autoInit, db.Close()
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

	// 启动时逐模块自动初始化（密钥+元数据，幂等）；失败仅记日志不中断监听，
	// 连接路径 OpenRepoForModule 会重试并通过 @ERROR 反馈客户端
	for i := range cfg.Modules {
		autoInit, err := EnsureModuleInit(&cfg.Modules[i])
		if err != nil {
			logger.Error("module_init_error", "module", cfg.Modules[i].Name, "err", err.Error())
			continue
		}
		if autoInit {
			logger.Info("module_auto_init", "module", cfg.Modules[i].Name)
		}
	}

	var wg sync.WaitGroup
	defer wg.Wait()
	// 模块并发连接计数（对齐 rsyncd max connections：0 = 无限制、负值 = 禁用模块、
	// 正数 = 上限；claim_connection 语义，连接结束释放）
	limiter := &moduleConnLimiter{}
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
			if err := handleConn(ctx, c, cfg, logger, limiter); err != nil && !errors.Is(err, protocol.ErrClientClosed) {
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
	r, closeRepo, err := OpenRepoForModule(module)
	if err != nil {
		fmt.Fprintf(conn, "@ERROR: %v\n", err)
		return err
	}
	defer closeRepo()
	sess := logger.With("module", module.Name,
		"client", conn.RemoteAddr().String(), "session", newSessionID())
	// 方向由 RunSession 在 argv 协商后判定（--sender = 恢复）；只读模块拒绝推送
	return protocol.RunSessionWithReader(ctx, br, conn, module, r, sess)
}
