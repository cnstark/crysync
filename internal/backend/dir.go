// internal/backend/dir.go
package backend

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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

func (d *Dir) metaDir() string { return filepath.Join(d.path, "meta") }

func (d *Dir) PutMetaContext(ctx context.Context, name string, src io.ReadSeeker, size int64) error {
	if err := validateBlobName(name); err != nil {
		return err
	}
	if err := os.MkdirAll(d.metaDir(), 0o700); err != nil {
		return fmt.Errorf("创建 Meta 备份目录: %w", err)
	}
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(d.metaDir(), ".tmp-meta-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	written, copyErr := io.Copy(tmp, &contextReader{ctx: ctx, r: io.LimitReader(src, size+1)})
	if copyErr != nil {
		tmp.Close()
		return copyErr
	}
	if written != size {
		tmp.Close()
		return fmt.Errorf("Meta 对象大小不符: 读取 %d，期望 %d", written, size)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(d.metaDir(), name)); err != nil {
		return err
	}
	dir, err := os.Open(d.metaDir())
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (d *Dir) GetMetaContext(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := validateBlobName(name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.Open(filepath.Join(d.metaDir(), name))
}

func (d *Dir) ListMetaContext(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(d.metaDir())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.Type().IsRegular() && !isTemporaryBlob(entry.Name()) {
			out = append(out, entry.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

func (d *Dir) DeleteMetaContext(ctx context.Context, name string) error {
	if err := validateBlobName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := os.Remove(filepath.Join(d.metaDir(), name))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
