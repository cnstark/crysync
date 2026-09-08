// internal/front/webdav/write.go
// 写路径：PUT 由 streamingPut 直接流式落库；COPY 等 FileSystem 写操作仍使用临时文件。
package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/webdav"

	"crysync/internal/backend"
	"crysync/internal/core"
)

// streamingPut 直接把请求体交给核心流式上传，避免 PUT 先完整写入临时文件。
// COPY 仍通过 moduleFS.openWrite 使用临时文件，因此这里只拦截 PUT。
func streamingPut(next http.Handler, writer core.FileWriter, prefix string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			next.ServeHTTP(w, r)
			return
		}
		path := normalize(strings.TrimPrefix(r.URL.Path, prefix))
		if path == "" {
			http.Error(w, "cannot write module root", http.StatusMethodNotAllowed)
			return
		}
		mtime := time.Now().UnixNano()
		if cw, ok := writer.(core.ContextFileWriter); ok {
			result, err := cw.PutFileContext(r.Context(), path, 0o100644, mtime, r.Body)
			if err != nil {
				writePutError(w, err)
				return
			}
			w.Header().Set("ETag", fmt.Sprintf(`"%x%x"`, result.MTimeNs, result.Size))
			w.WriteHeader(http.StatusCreated)
			return
		}
		// 兼容只实现旧 FileWriter 的嵌入者；正式 Repo 实现 ContextFileWriter。
		cr := &countingReader{r: r.Body}
		if err := writer.PutFile(path, 0o100644, mtime, cr); err != nil {
			writePutError(w, err)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"%x%x"`, mtime, cr.n))
		w.WriteHeader(http.StatusCreated)
	})
}

type countingReader struct {
	r io.Reader
	n int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func writePutError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var backendErr *backend.WebDAVError
	switch {
	case errors.As(err, &backendErr):
		// An upstream 404 does not mean that the path requested from CrySync is
		// absent. Cloud-WebDAV bridges can return 404 for a stale directory ID,
		// so expose this as a retryable dependency failure instead.
		if backendErr.StatusCode == http.StatusUnauthorized || backendErr.StatusCode == http.StatusForbidden || !backendErr.Retryable() {
			status = http.StatusBadGateway
		} else {
			status = http.StatusServiceUnavailable
			w.Header().Set("Retry-After", backend.RetryAfterSeconds(backendErr.RetryAfter))
			if w.Header().Get("Retry-After") == "" {
				w.Header().Set("Retry-After", "30")
			}
		}
		w.Header().Set("X-CrySync-Backend-Operation", backendErr.Method)
		if backendErr.StatusCode != 0 {
			w.Header().Set("X-CrySync-Backend-Status", strconv.Itoa(backendErr.StatusCode))
		} else {
			w.Header().Set("X-CrySync-Backend-Status", "transport")
		}
		if backendErr.Retryable() {
			w.Header().Set("X-CrySync-Backend-Retryable", "true")
		}
		http.Error(w, "backend storage unavailable", status)
		return
	case errors.Is(err, os.ErrNotExist):
		status = http.StatusConflict
	case errors.Is(err, os.ErrPermission):
		status = http.StatusForbidden
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// 请求已取消时响应可能已无法发送；若仍可写，使用客户端取消语义。
		status = 499
	}
	http.Error(w, err.Error(), status)
}

// openWrite 打开 COPY 等写句柄：read_only 拒绝。父目录存在性由最终写入阶段
// 的 checkParentDir 判定，避免锁外检查与提交之间出现竞态。
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

// writeFile 缓冲写句柄：COPY 收集到临时文件，Close 时 PutFile。
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
