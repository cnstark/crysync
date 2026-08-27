// internal/front/webdav/write.go
// 写路径：PUT 请求体缓冲到临时文件，Close 时以"写即快照"语义落库。
package webdav

import (
	"context"
	"os"
	pathpkg "path"
	"time"

	"golang.org/x/net/webdav"

	"crysync/internal/core"
)

// openWrite 打开写句柄：read_only 拒绝；父目录缺失返回 os.ErrNotExist
// （x/net/webdav PUT 对 os.IsNotExist 映射 409 Conflict）。
func (m *moduleFS) openWrite(ctx context.Context, path string) (webdav.File, error) {
	if m.readOnly || m.writer == nil {
		return nil, os.ErrPermission
	}
	if path == "" {
		return nil, os.ErrInvalid // 根不可写
	}
	parent := pathpkg.Dir(path)
	if parent != "." {
		sid, err := m.store.ActiveSnapshotID()
		if err != nil {
			return nil, err
		}
		row, ok, err := m.store.GetFileRow(sid, parent)
		if err != nil {
			return nil, err
		}
		if !ok || !row.IsDir {
			return nil, os.ErrNotExist
		}
	}
	tmp, err := os.CreateTemp("", "crysync-webdav-put-*")
	if err != nil {
		return nil, err
	}
	return &writeFile{writer: m.writer, path: path, tmp: tmp}, nil
}

// writeFile 缓冲写句柄：Write 收集到临时文件，Close 时 PutFile（写即快照）。
type writeFile struct {
	writer core.FileWriter
	path   string
	tmp    *os.File
	done   bool
}

func (w *writeFile) Write(p []byte) (int, error) { return w.tmp.Write(p) }

func (w *writeFile) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	tmpPath := w.tmp.Name()
	defer os.Remove(tmpPath)
	if err := w.tmp.Close(); err != nil {
		return err
	}
	f, err := os.Open(tmpPath)
	if err != nil {
		return err
	}
	defer f.Close()
	return w.writer.PutFile(w.path, 0o100644, time.Now().UnixNano(), f)
}

func (w *writeFile) Read(p []byte) (int, error) { return 0, os.ErrInvalid }
func (w *writeFile) Seek(offset int64, whence int) (int64, error) {
	return 0, os.ErrInvalid
}
func (w *writeFile) Readdir(count int) ([]os.FileInfo, error) { return nil, os.ErrInvalid }
func (w *writeFile) Stat() (os.FileInfo, error) {
	fi, err := w.tmp.Stat()
	if err != nil {
		return nil, err
	}
	return &writeFileInfo{size: fi.Size(), modTime: fi.ModTime()}, nil
}

// writeFileInfo PUT 期间的虚拟文件信息（x/net/webdav 用于 ETag 计算）。
type writeFileInfo struct {
	size    int64
	modTime time.Time
}

func (i *writeFileInfo) Name() string       { return "uploading" }
func (i *writeFileInfo) Size() int64        { return i.size }
func (i *writeFileInfo) Mode() os.FileMode  { return 0o644 }
func (i *writeFileInfo) ModTime() time.Time { return i.modTime }
func (i *writeFileInfo) IsDir() bool        { return false }
func (i *writeFileInfo) Sys() any           { return nil }
