// internal/core/open.go
// 模块打开逻辑：中端层负责"配置 → 打开仓库"，前端与 CLI 共用。
// 打开时自动初始化（密钥+元数据）、校验密钥、构造后端。
package core

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

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

// OpenModule 打开模块仓库：必要时自动初始化（密钥+元数据）、
// 校验密钥、打开元数据、构造后端。
func OpenModule(module *config.ModuleConfig) (*Module, error) {
	return openModule(module, nil)
}

func openModule(module *config.ModuleConfig, rt *Runtime) (*Module, error) {
	if _, err := ensureKeyfile(module); err != nil {
		return nil, err
	}
	key, err := crypto.LoadKeyFile(module.Keyfile)
	if err != nil {
		return nil, err
	}
	db, err := meta.Open(module.Meta)
	if err != nil {
		return nil, err
	}
	var be backend.Backend
	switch module.Backend.Type {
	case "dir":
		be, err = backend.NewDir(module.Backend.Path)
	case "webdav":
		be, err = backend.NewWebDAV(module.Backend.URL, module.Backend.Username, module.Backend.Password)
	default:
		db.Close()
		return nil, fmt.Errorf("模块 %s: 未知后端类型 %q", module.Name, module.Backend.Type)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	if err := be.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("模块 %s 后端不可用: %w", module.Name, err)
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
