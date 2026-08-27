// internal/core/svc.go
// core 包是中端层门面：向前端（front/*）与 CLI 暴露窄接口与模块服务工厂。
// 接口定义在 types 子包（叶子包，避免与 repo 循环依赖），此处 re-export，
// 外部统一使用 core.Session / core.FileStore 等名字。
package core

import "crysync/internal/core/types"

type SessionTxn = types.SessionTxn
type Session = types.Session
type FileStore = types.FileStore
type RsyncService = types.RsyncService