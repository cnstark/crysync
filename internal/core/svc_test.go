// internal/core/svc_test.go
package core

import (
	"crysync/internal/core/repo"
)

// 编译期断言：repo 实现面向前端的两个窄接口，事务实现 SessionTxn。
// 任何一端方法集漂移（receiver/sender 调用面变化、repo 方法改名）都会在此编译失败。
var _ Session = (*repo.Repo)(nil)
var _ FileStore = (*repo.Repo)(nil)
var _ SessionTxn = (*repo.SnapshotTxn)(nil)