// internal/front/webdav/module_cache_test.go
package webdav

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crysync/internal/core"
)

func TestModuleCacheOpenSingleFlightAndCooldown(t *testing.T) {
	cache := newModuleCache(100*time.Millisecond, []string{"home"})

	var calls atomic.Int32
	openOne := func() (*core.Module, error) {
		calls.Add(1)
		return nil, errors.New("后端不可用")
	}
	build := func(*core.Module) http.Handler { return http.NotFoundHandler() }

	_, first, err := cache.open("home", openOne, build)
	if err == nil || !first {
		t.Fatalf("首次失败应 firstFailure=true 且 err 非 nil, got first=%v err=%v", first, err)
	}
	_, first, err = cache.open("home", openOne, build)
	if err == nil || first {
		t.Fatalf("冷却期内应静默失败, got first=%v err=%v", first, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("冷却期内不应重试打开器, calls=%d", got)
	}
	time.Sleep(120 * time.Millisecond)
	_, first, err = cache.open("home", openOne, build)
	if err == nil || !first {
		t.Fatalf("新一轮失败应 firstFailure=true, got first=%v err=%v", first, err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("冷却期外应重试打开器, calls=%d", got)
	}
}

func TestModuleCacheOpenSuccessUnblocksConcurrent(t *testing.T) {
	cache := newModuleCache(50*time.Millisecond, []string{"home"})

	start := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	openOne := func() (*core.Module, error) {
		calls.Add(1)
		<-release
		return &core.Module{}, nil
	}
	build := func(*core.Module) http.Handler { return http.NotFoundHandler() }

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, err := cache.open("home", openOne, build)
			errs[i] = err
		}(i)
	}
	close(start)
	close(release)
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("并发打开应单飞只调一次, calls=%d", got)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("打开成功后并发请求不应失败, i=%d err=%v", i, err)
		}
	}
	h, first, err := cache.open("home", openOne, build)
	if err != nil || first || h == nil {
		t.Fatalf("成功后应命中缓存, got first=%v err=%v", first, err)
	}
}

func TestModuleCacheNames(t *testing.T) {
	cache := newModuleCache(time.Second, []string{"a", "b", "c"})
	cache.preload("b", http.NotFoundHandler())
	cache.preload("a", http.NotFoundHandler())
	names := cache.names()
	if len(names) != 2 || names[0] != "a" || names[1] != "b" {
		t.Fatalf("names 应按配置顺序含就绪模块, got %v", names)
	}
}
