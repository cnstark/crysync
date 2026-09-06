package backend

import (
	"context"
	"fmt"
	"sync"
)

type Backend interface {
	Put(name string, data []byte) error
	Get(name string) ([]byte, error)
	Delete(name string) error
	List() ([]string, error)
	Ping() error
}

// ContextBackend 可取消的后端扩展接口。Backend 保留旧方法以兼容嵌入者。
type ContextBackend interface {
	Backend
	PutContext(context.Context, string, []byte) error
}

// InMemory：单元测试用的假后端。并发安全（v0.5 起 blob 上传锁外并发，
// 测试用例会多 goroutine 同时写同一测试后端）。
type InMemory struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewInMemory() *InMemory { return &InMemory{m: map[string][]byte{}} }

func (m *InMemory) Put(name string, data []byte) error {
	return m.PutContext(context.Background(), name, data)
}
func (m *InMemory) PutContext(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[name] = append([]byte(nil), data...)
	return nil
}
func (m *InMemory) Get(name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.m[name]
	if !ok {
		return nil, fmt.Errorf("blob 不存在: %s", name)
	}
	return b, nil
}
func (m *InMemory) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.m, name)
	return nil
}
func (m *InMemory) List() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.m))
	for k := range m.m {
		out = append(out, k)
	}
	return out, nil
}
func (m *InMemory) Ping() error { return nil }

// backend.go 中补 Dir 类型声明，dir.go 提供实现
type Dir struct {
	path        string
	bucketDepth int
}

func (d *Dir) Path() string { return d.path }
