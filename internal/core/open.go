// internal/core/open.go
// 模块打开逻辑：中端层负责"配置 → 初始化或恢复 → 打开仓库"，前端与 CLI 共用。
package core

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"crysync/internal/backend"
	"crysync/internal/config"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
	"crysync/internal/core/repo"
)

// Module 模块仓库服务视图：前端通过 Session/FileStore 访问，CLI 通过 Repo 访问完整能力。
type Module struct {
	Repo       *repo.Repo
	Session    Session
	FileStore  FileStore
	FileWriter FileWriter
	Close      func() error
}

// OpenModule 打开模块仓库：全新后端自动初始化；只有 key 时从远端恢复 Meta。
func OpenModule(module *config.ModuleConfig) (*Module, error) {
	return openModule(module, nil)
}

func openModule(module *config.ModuleConfig, rt *Runtime) (*Module, error) {
	be, err := openBackend(module)
	if err != nil {
		return nil, err
	}
	if err := be.Ping(); err != nil {
		return nil, fmt.Errorf("模块 %s 后端不可用: %w", module.Name, err)
	}
	if _, err := ensureModuleReady(module, be); err != nil {
		return nil, err
	}
	key, err := crypto.LoadKeyFile(module.Keyfile)
	if err != nil {
		return nil, err
	}
	db, err := meta.OpenExisting(module.Meta)
	if err != nil {
		return nil, err
	}
	r := repo.New(db, be, key, module.ChunkSizeBytes())
	// 上传并发上限（跨连接共享闸门）：限制 backend.Put 网络写并发，0 = 不限制
	r.SetUploadConcurrency(module.MaxUploadConcurrency)
	if rt != nil {
		r.SetInflightLimiter(rt.inflight)
	}
	// v0.5：单一当前状态模型——FileWriter 直接指向 repo（无快照裁剪包装）。
	// 写方法内部：blob 上传无锁并发 + 行更新模块级短锁（见 repo 注释）。
	return &Module{
		Repo:       r,
		Session:    r,
		FileStore:  r,
		FileWriter: r,
		Close:      func() error { return db.Close() },
	}, nil
}

func openBackend(module *config.ModuleConfig) (backend.Backend, error) {
	switch module.Backend.Type {
	case "dir":
		return backend.NewDir(module.Backend.Path, module.Backend.BucketDepth)
	case "webdav":
		return backend.NewWebDAV(module.Backend.URL, module.Backend.Username, module.Backend.Password, module.Backend.BucketDepth)
	default:
		return nil, fmt.Errorf("模块 %s: 未知后端类型 %q", module.Name, module.Backend.Type)
	}
}

// keyfileMutexes 同进程内按密钥路径串行化自动初始化（多个连接同时首开同一模块）。
var keyfileMutexes sync.Map    // keyfile 路径 -> *sync.Mutex
var moduleInitMutexes sync.Map // Meta 路径 -> *sync.Mutex

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
			"为避免旧数据无法解密拒绝自动生成新密钥；请恢复密钥文件，或确认放弃旧数据后删除 %s",
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

type ModuleInitResult struct {
	KeyCreated   bool
	MetaRestored bool
}

// EnsureModuleInit 保留原有布尔结果接口；完整状态使用 PrepareModule。
func EnsureModuleInit(module *config.ModuleConfig) (bool, error) {
	result, err := PrepareModule(module)
	return result.KeyCreated, err
}

// PrepareModule 探测后端并完成新仓库初始化或 Meta 灾难恢复。
func PrepareModule(module *config.ModuleConfig) (ModuleInitResult, error) {
	be, err := openBackend(module)
	if err != nil {
		return ModuleInitResult{}, err
	}
	if err := be.Ping(); err != nil {
		return ModuleInitResult{}, fmt.Errorf("模块 %s 后端不可用: %w", module.Name, err)
	}
	return ensureModuleReady(module, be)
}

func ensureModuleReady(module *config.ModuleConfig, be backend.Backend) (ModuleInitResult, error) {
	v, _ := moduleInitMutexes.LoadOrStore(module.Meta, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	keyExists, err := fileExists(module.Keyfile)
	if err != nil {
		return ModuleInitResult{}, err
	}
	metaExists, err := fileExists(module.Meta)
	if err != nil {
		return ModuleInitResult{}, err
	}
	if keyExists && metaExists {
		return ModuleInitResult{}, nil
	}
	if !keyExists && metaExists {
		return ModuleInitResult{}, fmt.Errorf("模块 %s: 元数据库已存在但密钥文件缺失，拒绝自动生成新密钥", module.Name)
	}
	names, err := be.ListMetaContext(context.Background())
	if err != nil {
		return ModuleInitResult{}, fmt.Errorf("模块 %s 列出远端 Meta 备份: %w", module.Name, err)
	}
	if keyExists {
		if len(names) == 0 {
			return ModuleInitResult{}, fmt.Errorf("模块 %s: 密钥存在但本地 Meta 缺失，且远端没有可恢复备份", module.Name)
		}
		key, err := crypto.LoadKeyFile(module.Keyfile)
		if err != nil {
			return ModuleInitResult{}, err
		}
		if _, err := repo.RestoreMeta(context.Background(), module.Meta, be, key); err != nil {
			return ModuleInitResult{}, fmt.Errorf("模块 %s 恢复 Meta: %w", module.Name, err)
		}
		return ModuleInitResult{MetaRestored: true}, nil
	}
	if len(names) > 0 {
		return ModuleInitResult{}, fmt.Errorf("模块 %s: 远端存在 Meta 备份但密钥缺失，拒绝初始化新仓库", module.Name)
	}
	blobs, err := be.List()
	if err != nil {
		return ModuleInitResult{}, fmt.Errorf("模块 %s 检查远端数据: %w", module.Name, err)
	}
	if len(blobs) > 0 {
		return ModuleInitResult{}, fmt.Errorf("模块 %s: 远端已有数据 blob，但 key、Meta 和远端 Meta 备份均缺失，拒绝覆盖式初始化", module.Name)
	}
	autoInit, err := ensureKeyfile(module)
	if err != nil {
		return ModuleInitResult{}, err
	}
	db, err := meta.Open(module.Meta)
	if err != nil {
		return ModuleInitResult{KeyCreated: autoInit}, err
	}
	if _, err := db.EnsureRepositoryID(); err != nil {
		db.Close()
		return ModuleInitResult{KeyCreated: autoInit}, err
	}
	return ModuleInitResult{KeyCreated: autoInit}, db.Close()
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// RunMetaBackupScheduler 模块 Meta 备份自愈循环：模块未就绪时指数退避重试
// 初始化（后端启动竞态/临时故障自动恢复），就绪后立即备份并按 interval
// 周期备份，直至 ctx 取消。常规返回 nil（ctx 取消）。
func RunMetaBackupScheduler(ctx context.Context, module *config.ModuleConfig, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	failures := 0
	inFailure := false
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := runBackupUntilWithReadyCallback(ctx, module, logger, func() {
			if inFailure && ctx.Err() == nil {
				logger.Info("module_recovered", "module", module.Name)
			}
		})
		if err == nil {
			return nil
		}
		failures++
		if failures == 1 || failures%8 == 0 {
			logger.Warn("module_init_retry", "module", module.Name,
				"failures", failures, "err", err.Error())
		}
		inFailure = true
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(nextInitBackoff(failures)):
		}
	}
}

// runBackupUntil 打开模块并循环备份 Meta：初始化/打开失败返回 err
// （由外层退避重试），备份失败仅记日志不中断循环，ctx 取消返回 nil。
func runBackupUntil(ctx context.Context, module *config.ModuleConfig, logger *slog.Logger) error {
	return runBackupUntilWithReadyCallback(ctx, module, logger, nil)
}

// runBackupUntilWithReadyCallback 在模块初始化完成、首次备份开始前调用 onReady。
// 回调仅用于记录恢复等状态变化，不参与初始化与锁保护。
func runBackupUntilWithReadyCallback(ctx context.Context, module *config.ModuleConfig, logger *slog.Logger, onReady func()) error {
	be, err := openBackend(module)
	if err != nil {
		return err
	}
	if _, err := ensureModuleReady(module, be); err != nil {
		return err
	}
	key, err := crypto.LoadKeyFile(module.Keyfile)
	if err != nil {
		return err
	}
	db, err := meta.OpenExisting(module.Meta)
	if err != nil {
		return err
	}
	defer db.Close()
	if onReady != nil && ctx.Err() == nil {
		onReady()
	}
	backup := func() {
		name, err := repo.CreateMetaBackup(ctx, db, be, key, module.MetaBackupRetain())
		if err != nil {
			logger.Error("meta_backup_error", "module", module.Name, "err", err.Error())
			return
		}
		logger.Info("meta_backup_complete", "module", module.Name, "backup", name)
	}
	backup()
	ticker := time.NewTicker(module.MetaBackupInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			backup()
		}
	}
}
