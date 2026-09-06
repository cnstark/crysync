package repo

import (
	"context"
	"sync"
)

// InflightLimiter 限制进程内同时处于读取、处理和上传阶段的块数。
// 每次 Acquire 成功必须调用返回的 release；release 可安全重复调用。
type InflightLimiter struct {
	sem chan struct{}
}

// NewInflightLimiter 创建固定容量的在途块限制器。
func NewInflightLimiter(max int) *InflightLimiter {
	if max <= 0 {
		max = 1
	}
	return &InflightLimiter{sem: make(chan struct{}, max)}
}

func (l *InflightLimiter) Acquire(ctx context.Context) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	// 槽位空闲与取消同时就绪时，select 会随机选择；先检查一次，避免已经
	// 取消的请求仍取得槽位并开始分配、读取下一个块。
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case l.sem <- struct{}{}:
		var once sync.Once
		return func() {
			once.Do(func() { <-l.sem })
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
