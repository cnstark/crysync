package repo

import (
	"context"
	"testing"
	"time"
)

func TestInflightLimiterCancelAndReuse(t *testing.T) {
	l := NewInflightLimiter(1)
	canceled, cancelImmediately := context.WithCancel(context.Background())
	cancelImmediately()
	if _, err := l.Acquire(canceled); err != context.Canceled {
		t.Fatalf("已有空闲槽位时也应优先返回取消，得到 %v", err)
	}
	release, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Acquire(ctx); err != context.Canceled {
		t.Fatalf("取消等待应返回 context.Canceled，得到 %v", err)
	}
	release()
	release()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release2, err := l.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	release2()
}
