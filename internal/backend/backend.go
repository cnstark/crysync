package backend

import "fmt"

type Backend interface {
	Put(name string, data []byte) error
	Get(name string) ([]byte, error)
	Delete(name string) error
	List() ([]string, error)
	Ping() error
}

// InMemory：单元测试用的假后端
type InMemory struct{ m map[string][]byte }

func NewInMemory() *InMemory { return &InMemory{m: map[string][]byte{}} }

func (m *InMemory) Put(name string, data []byte) error {
	m.m[name] = append([]byte(nil), data...)
	return nil
}
func (m *InMemory) Get(name string) ([]byte, error) {
	b, ok := m.m[name]
	if !ok {
		return nil, fmt.Errorf("blob 不存在: %s", name)
	}
	return b, nil
}
func (m *InMemory) Delete(name string) error { delete(m.m, name); return nil }
func (m *InMemory) List() ([]string, error) {
	out := make([]string, 0, len(m.m))
	for k := range m.m {
		out = append(out, k)
	}
	return out, nil
}
func (m *InMemory) Ping() error { return nil }

// backend.go 中补 Dir 类型声明，dir.go 提供实现
type Dir struct{ path string }

func (d *Dir) Path() string { return d.path }
