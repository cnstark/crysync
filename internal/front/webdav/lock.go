// internal/front/webdav/lock.go
// noopLockSystem 接受 LOCK/UNLOCK 但不维护任何状态：Windows/macOS 客户端
// 偶发 LOCK 请求不报错；写冲突真正保障靠 SQLite 单写者 + 快照事务串行化。
package webdav

import (
	"time"

	"golang.org/x/net/webdav"
)

type noopLockSystem struct{}

func (noopLockSystem) Confirm(now time.Time, name0, name1 string, conditions ...webdav.Condition) (func(), error) {
	return func() {}, nil
}
func (noopLockSystem) Create(now time.Time, details webdav.LockDetails) (string, error) {
	return "opaquelocktoken:crysync-noop", nil
}
func (noopLockSystem) Refresh(now time.Time, token string, duration time.Duration) (webdav.LockDetails, error) {
	return webdav.LockDetails{}, nil
}
func (noopLockSystem) Unlock(now time.Time, token string) error { return nil }
