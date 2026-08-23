// internal/backend/dir.go
package backend

import (
	"fmt"
	"os"
	"path/filepath"
)

func NewDir(path string) (*Dir, error) {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, fmt.Errorf("创建后端目录: %w", err)
	}
	return &Dir{path: path}, nil
}

func (d *Dir) Put(name string, data []byte) error {
	final := filepath.Join(d.path, name)
	tmp, err := os.CreateTemp(d.path, "tmp-*")
	if err != nil {
		return fmt.Errorf("创建临时文件: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return fmt.Errorf("写入临时文件: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("关闭临时文件: %w", err)
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("重命名: %w", err)
	}
	return nil
}

func (d *Dir) Get(name string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(d.path, name))
	if err != nil {
		return nil, fmt.Errorf("读取 blob %s: %w", name, err)
	}
	return b, nil
}

func (d *Dir) Delete(name string) error {
	if err := os.Remove(filepath.Join(d.path, name)); err != nil {
		return fmt.Errorf("删除 blob %s: %w", name, err)
	}
	return nil
}

func (d *Dir) List() ([]string, error) {
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		out = append(out, e.Name())
	}
	return out, nil
}

func (d *Dir) Ping() error {
	st, err := os.Stat(d.path)
	if err != nil || !st.IsDir() {
		return fmt.Errorf("后端目录不可用: %v", err)
	}
	return nil
}
