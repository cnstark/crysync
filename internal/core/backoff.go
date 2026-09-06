// internal/core/backoff.go
// 模块初始化重试退避：1s 起翻倍，30s 封顶（后端启动竞态秒级恢复、
// 长故障仅约每半分钟探测一次）。
package core

import "time"

const (
	initBackoffMin = time.Second
	initBackoffMax = 30 * time.Second
)

// nextInitBackoff 返回第 failures 次连续失败后的等待时延。
func nextInitBackoff(failures int) time.Duration {
	delay := initBackoffMin << (failures - 1)
	if delay > initBackoffMax || delay <= 0 {
		return initBackoffMax
	}
	return delay
}
