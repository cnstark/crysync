// internal/logging/rotate.go
// 按大小轮转的日志 Writer：超过软上限时重命名链轮转（log → log.1 → log.2 …），
// 保留 maxFiles 个历史文件；maxFiles=0 表示不轮转。
// 软限制语义：写入前检查，当前行将超限时先轮转，行内容完整进入新文件（不截断）。
package logging

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type RotateWriter struct {
	path     string
	maxSize  int64 // 单文件软上限（字节）
	maxFiles int   // 保留历史文件数；0 = 不轮转

	mu   sync.Mutex
	f    *os.File
	size int64 // 当前文件已写字节
}

// NewRotateWriter 打开（必要时创建）path。启动时文件已超限则先轮转一次。
func NewRotateWriter(path string, maxSize int64, maxFiles int) (*RotateWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("创建日志目录: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("打开日志文件: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &RotateWriter{path: path, maxSize: maxSize, maxFiles: maxFiles, f: f, size: st.Size()}
	if maxFiles > 0 && st.Size() > 0 && st.Size() >= maxSize {
		w.rotateLocked() // 启动轮转失败降级继续写旧文件
	}
	return w, nil
}

// Write 并发安全；轮转失败时 stderr 告警并降级继续写当前文件（日志绝不阻断业务）。
func (w *RotateWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		// 轮转中断后的兜底重开
		f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return 0, err
		}
		w.f = f
		st, _ := f.Stat()
		if st != nil {
			w.size = st.Size()
		}
	}
	if w.maxFiles > 0 && w.size > 0 && w.size+int64(len(p)) > w.maxSize {
		w.rotateLocked()
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	return n, err
}

// Close 关闭当前文件（daemon 退出时调用）。
func (w *RotateWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// rotateLocked 执行重命名链轮转（调用方持锁）。失败时 stderr 告警并尽力保证 f 可写。
func (w *RotateWriter) rotateLocked() {
	if w.f != nil {
		w.f.Close()
		w.f = nil
	}
	// 删除最旧，链式前移：log.N-1→log.N … log→log.1
	oldest := fmt.Sprintf("%s.%d", w.path, w.maxFiles)
	if err := os.Remove(oldest); err != nil && !os.IsNotExist(err) {
		log.Printf("日志轮转: 删除 %s 失败: %v（继续使用当前文件）", oldest, err)
	}
	for i := w.maxFiles - 1; i >= 1; i-- {
		from := fmt.Sprintf("%s.%d", w.path, i)
		if _, err := os.Stat(from); err != nil {
			continue
		}
		if err := os.Rename(from, fmt.Sprintf("%s.%d", w.path, i+1)); err != nil {
			log.Printf("日志轮转: %s 前移失败: %v", from, err)
		}
	}
	if err := os.Rename(w.path, w.path+".1"); err != nil && !os.IsNotExist(err) {
		log.Printf("日志轮转: %s 重命名失败: %v（继续写当前文件）", w.path, err)
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		log.Printf("日志轮转: 重开 %s 失败: %v（写入将失败重试）", w.path, err)
		return
	}
	w.f = f
	w.size = 0
}
