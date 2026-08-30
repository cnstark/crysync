// internal/front/webdav/browse.go
// 目录 HTML 浏览中间件：GET/HEAD 模块目录返回 HTML 文件列表（浏览器直接
// 打开 http://host:port/<模块>/ 查看备份内容、点击下载）。
// 标准 WebDAV 浏览走 PROPFIND；x/net/webdav 对目录 GET 固定 405，无法表达
// "目录的表示"，故在库外拦截渲染——对标 rclone/wsgidav 的 dir_browser
// 惯例，对标准客户端（文件管理器/restic）完全透明。
package webdav

import (
	"html"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"crysync/internal/core"
)

// dirBrowse 包装模块 handler：目录的 GET/HEAD 渲染 HTML 列表，其余透传。
func dirBrowse(next http.Handler, store core.FileStore, prefix string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		path := normalize(strings.TrimPrefix(r.URL.Path, prefix))
		sid, err := store.ActiveSnapshotID()
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if path == "" {
			// 模块根是虚拟目录（无 FileRow 行）：直接渲染
			renderDirList(w, r, store, sid, "")
			return
		}
		row, ok, err := store.GetFileRow(sid, path)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if !ok || !row.IsDir {
			// 文件 GET（下载）或不存在：交给 webdav.Handler 处理
			next.ServeHTTP(w, r)
			return
		}
		renderDirList(w, r, store, sid, path)
	})
}

// renderDirList 渲染目录 HTML 列表：目录项（带尾斜杠）在前、文件项在后，
// 均按名排序；非模块根时含父目录链接。
func renderDirList(w http.ResponseWriter, r *http.Request, store core.FileStore, sid int64, path string) {
	rows, err := store.ListDir(sid, path)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	sort.Slice(rows, func(i, j int) bool {
		di, dj := rows[i].IsDir, rows[j].IsDir
		if di != dj {
			return di // 目录优先
		}
		return rows[i].Path < rows[j].Path
	})

	var b strings.Builder
	b.WriteString("<!DOCTYPE html><html><head><meta charset=\"utf-8\">")
	b.WriteString("<title>crysync: /" + html.EscapeString(path) + "/</title>")
	b.WriteString("<style>body{font-family:monospace;margin:2em}ul{list-style:none;padding:0}")
	b.WriteString("li{padding:2px 0}a{text-decoration:none}.muted{color:#888;margin-left:1em}</style></head><body>")
	// 标题 = 面包屑（模块根 -> 逐层路径）
	b.WriteString("<h1>" + html.EscapeString("/"+path) + "/</h1><ul>")
	if path != "" {
		// 父目录链接：去掉最后一段（r.URL.Path 必以 / 结尾——ServeMux 301 保证）
		parent := r.URL.Path[:strings.LastIndex(strings.TrimSuffix(r.URL.Path, "/"), "/")+1]
		if parent != "" {
			b.WriteString(`<li><a href="` + html.EscapeString(parent) + `">../</a></li>`)
		}
	}
	for _, row := range rows {
		name := pathpkgBase(row.Path)
		href := url.PathEscape(name)
		if row.IsDir {
			b.WriteString(`<li><a href="` + html.EscapeString(href) + `/">` + html.EscapeString(name) + `/</a></li>`)
			continue
		}
		modTime := time.Unix(0, row.MTimeNs).Format("2006-01-02 15:04:05")
		b.WriteString(`<li><a href="` + html.EscapeString(href) + `">` + html.EscapeString(name) + `</a>`)
		b.WriteString(`<span class="muted">` + humanSize(row.Size) + " " + html.EscapeString(modTime) + `</span></li>`)
	}
	b.WriteString("</ul></body></html>")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String()) //nolint:errcheck // 响应体写入失败无补救动作
}

// pathpkgBase 返回路径末段（列表项显示名）。
func pathpkgBase(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// humanSize 人类可读大小。
func humanSize(n int64) string {
	switch {
	case n >= 1<<30:
		return trimFloat(float64(n)/float64(1<<30)) + " GiB"
	case n >= 1<<20:
		return trimFloat(float64(n)/float64(1<<20)) + " MiB"
	case n >= 1<<10:
		return trimFloat(float64(n)/float64(1<<10)) + " KiB"
	default:
		return strconv.FormatInt(n, 10) + " B"
	}
}

// trimFloat 保留一位小数的数字文本（如 "1.5"、"2"）。
func trimFloat(f float64) string {
	s := strings.TrimSuffix(strings.TrimSuffix(strconv.FormatFloat(f, 'f', 1, 64), "0"), ".")
	if s == "" {
		return "0"
	}
	return s
}
