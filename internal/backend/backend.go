package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
)

type Backend interface {
	Put(name string, data []byte) error
	Get(name string) ([]byte, error)
	Delete(name string) error
	List() ([]string, error)
	Ping() error
	PutMetaContext(context.Context, string, io.ReadSeeker, int64) error
	GetMetaContext(context.Context, string) (io.ReadCloser, error)
	ListMetaContext(context.Context) ([]string, error)
	DeleteMetaContext(context.Context, string) error
}

// ContextBackend 是可取消的数据 blob 写扩展接口。
type ContextBackend interface {
	Backend
	PutContext(context.Context, string, []byte) error
}

// InMemory：单元测试用的假后端。并发安全（v0.5 起 blob 上传锁外并发，
// 测试用例会多 goroutine 同时写同一测试后端）。
type InMemory struct {
	mu sync.Mutex
	m  map[string][]byte
	mm map[string][]byte
}

func NewInMemory() *InMemory { return &InMemory{m: map[string][]byte{}, mm: map[string][]byte{}} }

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

func (m *InMemory) PutMetaContext(ctx context.Context, name string, src io.ReadSeeker, size int64) error {
	if err := validateBlobName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return err
	}
	b, err := io.ReadAll(io.LimitReader(src, size+1))
	if err != nil {
		return err
	}
	if int64(len(b)) != size {
		return fmt.Errorf("Meta 对象大小不符: 读取 %d，期望 %d", len(b), size)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mm[name] = append([]byte(nil), b...)
	return nil
}

func (m *InMemory) GetMetaContext(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.mm[name]
	if !ok {
		return nil, fmt.Errorf("Meta 备份不存在: %s", name)
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), b...))), nil
}

func (m *InMemory) ListMetaContext(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.mm))
	for name := range m.mm {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func (m *InMemory) DeleteMetaContext(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mm, name)
	return nil
}

// backend.go 中补 Dir 类型声明，dir.go 提供实现
type Dir struct {
	path        string
	bucketDepth int
}

func (d *Dir) Path() string { return d.path }
