// internal/logging/logging.go
// daemon 主 logger 构造：slog TextHandler（key=value 文本行）写 stderr + 可选轮转文件。
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
)

// New 构造 daemon 主 logger。file 为空 = 仅 stderr（向后兼容缺省行为）。
// level: debug/info/warn/error；maxSizeMB 单文件软上限；maxFiles 保留轮转数（0 = 不轮转）。
func New(file, level string, maxSizeMB, maxFiles int) (*slog.Logger, error) {
	return newWithStderr(os.Stderr, file, level, int64(maxSizeMB)<<20, maxFiles)
}

// newWithStderr 允许注入 stderr（测试用 io.Discard 屏蔽噪音）。
func newWithStderr(stderr io.Writer, file, level string, maxSizeBytes int64, maxFiles int) (*slog.Logger, error) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return nil, fmt.Errorf("日志级别非法: %q（debug/info/warn/error）", level)
	}

	writers := []io.Writer{stderr}
	if file != "" {
		rw, err := NewRotateWriter(file, maxSizeBytes, maxFiles)
		if err != nil {
			return nil, err
		}
		writers = append(writers, rw)
	}
	h := slog.NewTextHandler(io.MultiWriter(writers...), &slog.HandlerOptions{Level: lvl})
	return slog.New(h), nil
}
