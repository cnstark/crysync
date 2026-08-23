// internal/backend/webdav.go
package backend

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// WebDAV：基于 WebDAV 协议的 Backend 实现（blob 名 = 64 hex，无路径分隔符，
// 直接作为资源名）。方法映射：
//
//	Put=PUT、Get=GET、Delete=DELETE、List=PROPFIND(depth=1)、Ping=PROPFIND(根)。
//
// 写失败（网络/超时）按设计文档 §7 指数退避重试 3 次再报错；读失败不重试。
type WebDAV struct {
	baseURL string
	user    string
	pass    string
	client  *http.Client
}

// NewWebDAV 构造 WebDAV 后端。url 为后端根（blob 存放在其下）。
func NewWebDAV(rawURL, user, pass string) (*WebDAV, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("解析 webdav URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("webdav URL 必须为 http/https: %q", rawURL)
	}
	base := strings.TrimSuffix(u.String(), "/")
	return &WebDAV{
		baseURL: base,
		user:    user,
		pass:    pass,
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

// blobURL 返回 blob 资源的完整 URL。
func (w *WebDAV) blobURL(name string) string {
	return w.baseURL + "/" + url.PathEscape(name)
}

// do 发送请求：带 Basic 认证、统一状态码处理。
// wantStatus 为空时接受 2xx；非 2xx 返回错误。
func (w *WebDAV) do(method, url string, body []byte, want ...int) (*http.Response, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	if w.user != "" {
		req.SetBasicAuth(w.user, w.pass)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	if len(want) > 0 {
		ok = false
		for _, c := range want {
			if resp.StatusCode == c {
				ok = true
				break
			}
		}
	}
	if !ok {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("webdav %s %s: %s", method, url, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

// putOnce 单次 PUT。
func (w *WebDAV) putOnce(name string, data []byte) error {
	resp, err := w.do(http.MethodPut, w.blobURL(name), data)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (w *WebDAV) Put(name string, data []byte) error {
	// 指数退避重试（设计文档 §7：后端写失败重试 3 次再中止）
	var err error
	delay := 200 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		if err = w.putOnce(name, data); err == nil {
			return nil
		}
		if attempt < 2 {
			time.Sleep(delay)
			delay *= 2
		}
	}
	return fmt.Errorf("写入后端 %s: %w", name, err)
}

func (w *WebDAV) Get(name string) ([]byte, error) {
	resp, err := w.do(http.MethodGet, w.blobURL(name), nil)
	if err != nil {
		return nil, fmt.Errorf("读取 blob %s: %w", name, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 blob %s: %w", name, err)
	}
	return b, nil
}

func (w *WebDAV) Delete(name string) error {
	// 404 视为已删除（幂等）
	resp, err := w.do(http.MethodDelete, w.blobURL(name), nil, http.StatusNoContent, http.StatusNotFound, http.StatusOK)
	if err != nil {
		return fmt.Errorf("删除 blob %s: %w", name, err)
	}
	resp.Body.Close()
	return nil
}

// multistatus PROPFIND 响应（只解析 blob 名需要的 href 字段）。
type multistatus struct {
	Responses []struct {
		Href string `xml:"href"`
	} `xml:"response"`
}

// List 用 PROPFIND depth=1 列出后端根下的资源名。
func (w *WebDAV) List() ([]string, error) {
	req, err := http.NewRequest("PROPFIND", w.baseURL+"/", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Depth", "1")
	if w.user != "" {
		req.SetBasicAuth(w.user, w.pass)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("webdav list: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMultiStatus {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("webdav list: %s", strings.TrimSpace(string(b)))
	}
	var ms multistatus
	if err := xml.NewDecoder(resp.Body).Decode(&ms); err != nil {
		return nil, fmt.Errorf("解析 PROPFIND 响应: %w", err)
	}
	// href 可能是绝对 URL 或根相对路径；blob 名取最后一段且必须为 64 hex
	seen := map[string]bool{}
	var out []string
	for _, r := range ms.Responses {
		u, err := url.Parse(r.Href)
		if err != nil {
			continue
		}
		name := path.Base(u.Path)
		if name == "." || name == "/" || name == "" {
			continue // 根自身
		}
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out, nil
}

// Ping 用 PROPFIND 探测根可达性。
func (w *WebDAV) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "PROPFIND", w.baseURL+"/", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Depth", "0")
	if w.user != "" {
		req.SetBasicAuth(w.user, w.pass)
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("webdav 后端不可达: %w", err)
	}
	defer resp.Body.Close()
	// 服务器实现可能不支持 PROPFIND 返回 207；200（旧实现）也视为可达
	if resp.StatusCode != http.StatusMultiStatus && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webdav 后端不可达: HTTP %d", resp.StatusCode)
	}
	return nil
}
