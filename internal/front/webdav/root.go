// internal/front/webdav/root.go
// 虚拟根：WebDAV 客户端挂载服务器根（http://host:port/）时，把已就绪的
// 模块呈现为集合列表。根自身只读（OPTIONS/GET/HEAD/PROPFIND），写方法
// 405；根下不匹配任何模块子树的路径 404。
package webdav

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type rootHandler struct {
	names func() []string // 就绪模块名（按配置声明顺序，动态读取模块缓存）
}

func (h *rootHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	switch r.Method {
	case "OPTIONS":
		// 与模块 handler 的响应头对齐（x/net/webdav handleOptions 同款）
		w.Header().Set("Allow", "OPTIONS, GET, HEAD, PROPFIND")
		w.Header().Set("DAV", "1, 2")
		w.Header().Set("MS-Author-Via", "DAV")
		w.WriteHeader(http.StatusOK)
	case "PROPFIND":
		h.propfind(w, r)
	case http.MethodGet, http.MethodHead:
		h.getIndex(w)
	default:
		// 根是虚拟集合：不可写（PUT/DELETE/MKCOL/COPY/MOVE/PROPPATCH/LOCK/UNLOCK）
		http.Error(w, "root is read-only", http.StatusMethodNotAllowed)
	}
}

// propfind 根集合 PROPFIND：Depth 0 只返回根；其余 Depth 枚举模块集合。
func (h *rootHandler) propfind(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	b.WriteString(`<D:multistatus xmlns:D="DAV:">` + "\n")
	writeRootResponse(&b, "")
	if r.Header.Get("Depth") != "0" {
		for _, name := range h.names() {
			writeRootResponse(&b, name)
		}
	}
	b.WriteString(`</D:multistatus>`)
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	io.WriteString(w, b.String()) //nolint:errcheck // 响应体写入失败无补救动作
}

// writeRootResponse 写一条根/模块集合的 response。name 为空 = 根自身。
func writeRootResponse(b *strings.Builder, name string) {
	href := "/"
	display := "crysync"
	if name != "" {
		href = "/" + name + "/"
		display = name
	}
	b.WriteString(" <D:response>\n")
	fmt.Fprintf(b, "  <D:href>%s</D:href>\n", xmlEscapeText(href))
	b.WriteString("  <D:propstat>\n   <D:prop>\n")
	fmt.Fprintf(b, "    <D:displayname>%s</D:displayname>\n", xmlEscapeText(display))
	b.WriteString("    <D:resourcetype><D:collection/></D:resourcetype>\n")
	// Keep the virtual root's collection properties aligned with the
	// x/net/webdav handler used for /<module>/. Some WebDAV clients treat a
	// collection with only resourcetype as inaccessible and report a
	// misleading "permission denied" error.
	fmt.Fprintf(b, "    <D:getlastmodified>%s</D:getlastmodified>\n", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	b.WriteString("    <D:supportedlock><D:lockentry xmlns:D=\"DAV:\"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockentry></D:supportedlock>\n")
	b.WriteString("   </D:prop>\n   <D:status>HTTP/1.1 200 OK</D:status>\n")
	b.WriteString("  </D:propstat>\n </D:response>\n")
}

// getIndex 浏览器友好的 HTML 索引（浏览器直接访问根时可见模块链接）。
func (h *rootHandler) getIndex(w http.ResponseWriter) {
	var b strings.Builder
	b.WriteString("<!DOCTYPE html><html><head><title>crysync</title></head><body><ul>")
	for _, name := range h.names() {
		href := xmlEscapeText("/" + name + "/")
		fmt.Fprintf(&b, `<li><a href="%s">%s/</a></li>`, href, xmlEscapeText(name))
	}
	b.WriteString("</ul></body></html>")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, b.String()) //nolint:errcheck // 响应体写入失败无补救动作
}

// xmlEscapeText 转义 XML 文本（模块名来自配置，防御性转义）。
func xmlEscapeText(s string) string {
	var b strings.Builder
	if err := xml.EscapeText(&b, []byte(s)); err != nil {
		return s
	}
	return b.String()
}
