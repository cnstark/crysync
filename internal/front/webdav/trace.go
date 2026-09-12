package webdav

import (
	"log/slog"
	"net/http"
	"time"

	"crysync/internal/trace"
)

type traceResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *traceResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *traceResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

// requestTrace records the outer lifecycle of every WebDAV request and passes
// its opaque ID into the core upload path.
func requestTrace(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := trace.NewID()
		started := time.Now()
		logger.Debug("webdav_request_start", "op", id, "method", r.Method, "path", r.URL.Path, "client", r.RemoteAddr)
		tw := &traceResponseWriter{ResponseWriter: w}
		next.ServeHTTP(tw, r.WithContext(trace.WithID(r.Context(), id)))
		status := tw.status
		if status == 0 {
			status = http.StatusOK
		}
		logger.Debug("webdav_request_end", "op", id, "method", r.Method, "path", r.URL.Path,
			"status", status, "response_bytes", tw.bytes, "elapsed_ms", time.Since(started).Milliseconds())
	})
}
