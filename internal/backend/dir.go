// internal/backend/dir.go
package backend

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func NewDir(path string, bucketDepth int) (*Dir, error) {
	depth, err := normalizeBucketDepth(bucketDepth)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, fmt.Errorf("创建后端目录: %w", err)
	}
	return &Dir{path: path, bucketDepth: depth}, nil
}

func (d *Dir) Put(name string, data []byte) error {
	return d.PutContext(context.Background(), name, data)
}

func (d *Dir) PutContext(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	parts, err := bucketParts(name, d.bucketDepth)
	if err != nil {
		return err
	}
	bucketDir := filepath.Join(append([]string{d.path}, parts...)...)
	if err := os.MkdirAll(bucketDir, 0o700); err != nil {
		return fmt.Errorf("创建分桶目录: %w", err)
	}
	final := filepath.Join(bucketDir, name)
	tmp, err := os.CreateTemp(bucketDir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("创建临时文件: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return fmt.Errorf("写入临时文件: %w", err)
	}
	if err := ctx.Err(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
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
	parts, err := bucketParts(name, d.bucketDepth)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(append([]string{d.path}, append(parts, name)...)...))
	if err != nil {
		return nil, fmt.Errorf("读取 blob %s: %w", name, err)
	}
	return b, nil
}

func (d *Dir) Delete(name string) error {
	parts, err := bucketParts(name, d.bucketDepth)
	if err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(append([]string{d.path}, append(parts, name)...)...)); err != nil {
		return fmt.Errorf("删除 blob %s: %w", name, err)
	}
	return nil
}

func (d *Dir) List() ([]string, error) {
	out := make([]string, 0)
	var visit func(string, []string) error
	visit = func(dir string, parts []string) error {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if len(parts) < d.bucketDepth {
				if e.IsDir() && isBucketPart(e.Name()) {
					next := append(append([]string(nil), parts...), e.Name())
					if err := visit(filepath.Join(dir, e.Name()), next); err != nil {
						return err
					}
				}
				continue
			}
			if isTemporaryBlob(e.Name()) || !e.Type().IsRegular() ||
				!blobBelongsToBucket(e.Name(), parts, d.bucketDepth) {
				continue
			}
			out = append(out, e.Name())
		}
		return nil
	}
	if err := visit(d.path, nil); err != nil {
		return nil, err
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
