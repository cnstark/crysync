// internal/front/webdav/fs.go
// moduleFS 把中端 FileStore/FileWriter 适配成 x/net/webdav 的虚拟文件系统。
package webdav

import (
	"context"
	"io"
	"os"
	pathpkg "path"
	"strings"
	"time"

	"golang.org/x/net/webdav"

	"crysync/internal/core"
	"crysync/internal/core/meta"
)

// normalize 把 WebDAV 请求路径转成仓库内路径（去前导/尾随斜杠；空 = 模块根）。
func normalize(name string) string { return strings.Trim(name, "/") }

type moduleFS struct {
	store    core.FileStore
	writer   core.FileWriter
	readOnly bool
	// urlRoot is the module's external URL path (for example "/quark").
	// Keeping this path in the FileSystem, instead of Handler.Prefix, means
	// x/net/webdav sees /quark/ as a named collection. Some NAS clients reject
	// a collection whose DAV:displayname is empty (the value x/net emits for
	// handler root "/").
	urlRoot string
	// rootName/rootModTime describe the virtual module root. x/net/webdav
	// derives DAV:displayname and DAV:getlastmodified from os.FileInfo; an
	// unnamed zero-time root is accepted by Go clients but rejected by some
	// NAS WebDAV clients as an inaccessible collection.
	rootName    string
	rootModTime time.Time
}

// repoPath converts an external DAV path to a repository-relative path. An
// empty urlRoot is useful for direct FileSystem unit tests.
func (m *moduleFS) repoPath(name string) (string, error) {
	clean := pathpkg.Clean("/" + name)
	if m.urlRoot == "" {
		return normalize(clean), nil
	}
	root := pathpkg.Clean(m.urlRoot)
	if clean == root {
		return "", nil
	}
	if !strings.HasPrefix(clean, root+"/") {
		return "", os.ErrNotExist
	}
	return strings.TrimPrefix(clean, root+"/"), nil
}

func (m *moduleFS) rootInfo() *fileInfo {
	return &fileInfo{
		row:     meta.FileRow{IsDir: true, Mode: 0o40755},
		name:    m.rootName,
		modTime: m.rootModTime,
	}
}

func (m *moduleFS) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	path, err := m.repoPath(name)
	if err != nil {
		return nil, err
	}
	if path == "" {
		return m.rootInfo(), nil // 模块根
	}
	row, ok, err := m.store.GetFileRow(path)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	return &fileInfo{row: row}, nil
}

func (m *moduleFS) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	path, err := m.repoPath(name)
	if err != nil {
		return nil, err
	}
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		return m.openWrite(ctx, path)
	}
	if path == "" {
		// 模块根：与 Stat 一致的虚拟目录，可打开枚举（PROPFIND 遍历用）
		return &dirFile{fs: m, path: path, fi: m.rootInfo()}, nil
	}
	row, ok, err := m.store.GetFileRow(path)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	switch {
	case row.IsDir:
		return &dirFile{fs: m, path: path, fi: &fileInfo{row: row}}, nil
	case row.IsSymlink:
		// WebDAV 无 symlink 语义：GET 返回链接目标文本（设计：不跟随、不创建）
		return &symlinkFile{fi: &fileInfo{row: row}, target: row.LinkTarget}, nil
	}
	fr, _, err := m.store.OpenFile(path)
	if err != nil {
		return nil, err
	}
	return &readFile{fr: fr, fi: &fileInfo{row: row}}, nil
}

func (m *moduleFS) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	path, err := m.repoPath(name)
	if err != nil {
		return err
	}
	return m.writer.Mkcol(path)
}

func (m *moduleFS) RemoveAll(ctx context.Context, name string) error {
	path, err := m.repoPath(name)
	if err != nil {
		return err
	}
	return m.writer.DeletePath(path)
}

func (m *moduleFS) Rename(ctx context.Context, oldName, newName string) error {
	oldPath, err := m.repoPath(oldName)
	if err != nil {
		return err
	}
	newPath, err := m.repoPath(newName)
	if err != nil {
		return err
	}
	return m.writer.MovePath(oldPath, newPath)
}

// fileInfo 快照文件行的 os.FileInfo 实现。
type fileInfo struct {
	row     meta.FileRow
	name    string
	modTime time.Time
}

func (i *fileInfo) Name() string {
	if i.name != "" {
		return i.name
	}
	if i.row.Path == "" {
		return ""
	}
	return pathpkg.Base(i.row.Path)
}
func (i *fileInfo) Size() int64 { return i.row.Size }
func (i *fileInfo) ModTime() time.Time {
	if !i.modTime.IsZero() {
		return i.modTime
	}
	return time.Unix(0, i.row.MTimeNs)
}
func (i *fileInfo) IsDir() bool { return i.row.IsDir }
func (i *fileInfo) Sys() any    { return nil }
func (i *fileInfo) Mode() os.FileMode {
	if i.row.IsDir {
		return 0o755 | os.ModeDir
	}
	if i.row.IsSymlink {
		return 0o644 // 呈现为普通文件
	}
	return os.FileMode(i.row.Mode)
}

// readFile 普通文件读句柄（webdav.File 实现）。
type readFile struct {
	fr io.ReadSeekCloser
	fi os.FileInfo
}

func (f *readFile) Read(p []byte) (int, error)         { return f.fr.Read(p) }
func (f *readFile) Seek(o int64, w int) (int64, error) { return f.fr.Seek(o, w) }
func (f *readFile) Close() error                       { return f.fr.Close() }
func (f *readFile) Stat() (os.FileInfo, error)         { return f.fi, nil }
func (f *readFile) Write(p []byte) (int, error)        { return 0, os.ErrInvalid }
func (f *readFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, os.ErrInvalid
}

// dirFile 目录句柄：Readdir 列当前清单的直接子项（PROPFIND 枚举用）。
// x/net/webdav 的目录遍历循环调用 Readdir 直到 io.EOF——消费完必须返回
// io.EOF，否则死循环。
type dirFile struct {
	fs   *moduleFS
	path string
	fi   os.FileInfo
	off  int // 枚举游标
}

// Readdir 枚举目录子项。count<=0 时返回全部剩余且耗尽不是错误
// （(nil, nil)）——对齐 x/net/webdav 内 memFile 与 os.File 语义：walkFS
// 用 Readdir(0) 且把任何非 nil err 当目录遍历失败，空目录返回 io.EOF
// 会令 PROPFIND 得 500。count>0 时耗尽返回 io.EOF（分页读取惯例）。
func (d *dirFile) Readdir(count int) ([]os.FileInfo, error) {
	rows, err := d.fs.store.ListDir(d.path)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(rows))
	for _, row := range rows {
		infos = append(infos, &fileInfo{row: row})
	}
	if d.off >= len(infos) {
		if count > 0 {
			return nil, io.EOF
		}
		return nil, nil
	}
	end := len(infos)
	if count > 0 && d.off+count < end {
		end = d.off + count
	}
	out := infos[d.off:end]
	d.off = end
	return out, nil
}
func (d *dirFile) Read(p []byte) (int, error)         { return 0, os.ErrInvalid }
func (d *dirFile) Seek(o int64, w int) (int64, error) { return 0, os.ErrInvalid }
func (d *dirFile) Write(p []byte) (int, error)        { return 0, os.ErrInvalid }
func (d *dirFile) Close() error                       { return nil }
func (d *dirFile) Stat() (os.FileInfo, error)         { return d.fi, nil }

// symlinkFile 符号链接句柄：内容 = 链接目标文本。
type symlinkFile struct {
	fi     os.FileInfo
	target string
	off    int
}

func (s *symlinkFile) Read(p []byte) (int, error) {
	if s.off >= len(s.target) {
		return 0, io.EOF
	}
	n := copy(p, s.target[s.off:])
	s.off += n
	return n, nil
}
func (s *symlinkFile) Seek(o int64, w int) (int64, error) {
	n := int64(len(s.target))
	var base int64
	switch w {
	case io.SeekStart:
		// base = 0
	case io.SeekCurrent:
		base = int64(s.off)
	case io.SeekEnd:
		base = n
	default:
		return 0, os.ErrInvalid
	}
	abs := base + o
	if abs < 0 {
		return 0, os.ErrInvalid
	}
	// 超出末尾封顶到末尾（io.ReadSeeker 契约）；ServeContent 依赖
	// Seek(0, io.SeekEnd) 返回大小再 Seek(0, io.SeekStart) 读内容。
	if abs > n {
		abs = n
	}
	s.off = int(abs)
	return abs, nil
}
func (s *symlinkFile) Write(p []byte) (int, error) { return 0, os.ErrInvalid }
func (s *symlinkFile) Readdir(count int) ([]os.FileInfo, error) {
	return nil, os.ErrInvalid
}
func (s *symlinkFile) Close() error               { return nil }
func (s *symlinkFile) Stat() (os.FileInfo, error) { return s.fi, nil }
