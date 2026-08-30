// internal/front/webdav/write.go
// 写路径：PUT 请求体缓冲到临时文件，Close 时以"写即快照"语义落库。
package webdav

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/net/webdav"

	"crysync/internal/core"
)

// openWrite 打开写句柄：read_only 拒绝。父目录存在性不在前置检查--
// PutFile 在写锁内的 checkParentDir 才是权威判定（锁外预检查曾读 ActiveSnapshotID
// 到一个已被并发单份模式 trim 删除的快照，GET 旧快照清单必然落空 -> PUT 409
// 重试风暴；405/409 映射仍由 PutFile 返回的 os.ErrNotExist 驱动）。
func (m *moduleFS) openWrite(ctx context.Context, path string) (webdav.File, error) {
	if m.readOnly || m.writer == nil {
		return nil, os.ErrPermission
	}
	if path == "" {
		return nil, os.ErrInvalid // 根不可写
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

// idempotentDelete 幂等 DELETE 中间件（绿联 NAS 定制 restic fork 兼容）：
// x/net/webdav 的 handleDelete 在 RemoveAll 前强制 Stat（webdav.go:261-266），
// 路径不存在即 404——repo 层 DeletePath 的幂等化覆盖不到库层，故在 Handler
// 前短路 DELETE：writer.DeletePath 锁内判定（不存在返回 nil），统一 204
// No Content（目标状态已达成即成功）。存在性判定与删除在同一写锁内，
// 无锁外预检竞态（409 风暴教训）。模块根删除顺带拒绝（原库行为会
// RemoveAll 模块根清空整仓）。
func idempotentDelete(next http.Handler, writer core.FileWriter, prefix string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			next.ServeHTTP(w, r)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, prefix)
		if path = strings.Trim(path, "/"); path == "" {
			http.Error(w, "cannot delete module root", http.StatusMethodNotAllowed)
			return
		}
		if err := writer.DeletePath(path); err != nil {
			// 与 x/net/webdav RemoveAll 错误映射一致（405）；DeletePath 失败
			// 一般是 DB/后端错误，原库同样 405
			http.Error(w, err.Error(), http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
