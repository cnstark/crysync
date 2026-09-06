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
	"sort"
	"strings"
	"sync"
	"time"
)

// WebDAV：基于 WebDAV 协议的 Backend 实现。逻辑 blob 名通过哈希前缀映射到
// 多级 collection，完整逻辑名作为叶子资源名。方法映射：
//
//	Put=MKCOL+PUT、Get=GET、Delete=DELETE、List=逐层 PROPFIND(depth=1)、Ping=PROPFIND(根)。
//
// 写失败（网络/超时）按设计文档 §7 指数退避重试 3 次再报错；读失败不重试。
type WebDAV struct {
	baseURL       string
	user          string
	pass          string
	client        *http.Client
	bucketDepth   int
	collectionsMu sync.Mutex
	collections   map[string]bool
}

// NewWebDAV 构造 WebDAV 后端。url 为后端根（blob 存放在其下）。
func NewWebDAV(rawURL, user, pass string, bucketDepth int) (*WebDAV, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("解析 webdav URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("webdav URL 必须为 http/https: %q", rawURL)
	}
	depth, err := normalizeBucketDepth(bucketDepth)
	if err != nil {
		return nil, err
	}
	base := strings.TrimSuffix(u.String(), "/")
	return &WebDAV{
		baseURL:     base,
		user:        user,
		pass:        pass,
		bucketDepth: depth,
		collections: make(map[string]bool),
		client: &http.Client{
			// 300s：公网 WebDAV 后端（如夸克云盘）上行带宽可低至 ~0.1MiB/s，
			// 大块 blob（chunk_size 配置 32MiB 时）传输需要数百秒；60s 会在
			// 带宽波动期误杀慢速 PUT 触发整块重传放大。写失败仍有 3 次
			// 指数退避重试兜底（Put）。
			Timeout: 300 * time.Second,
		},
	}, nil
}

func (w *WebDAV) collectionURL(parts []string) string {
	u := w.baseURL
	for _, part := range parts {
		u += "/" + url.PathEscape(part)
	}
	return u
}

// blobURL 返回 blob 资源的完整 URL。
func (w *WebDAV) blobURL(name string) (string, error) {
	parts, err := bucketParts(name, w.bucketDepth)
	if err != nil {
		return "", err
	}
	return w.collectionURL(parts) + "/" + url.PathEscape(name), nil
}

// do 发送请求：带 Basic 认证、统一状态码处理。
// wantStatus 为空时接受 2xx；非 2xx 返回错误。
func (w *WebDAV) do(method, url string, body []byte, want ...int) (*http.Response, error) {
	return w.doContext(context.Background(), method, url, body, want...)
}

func (w *WebDAV) doContext(ctx context.Context, method, url string, body []byte, want ...int) (*http.Response, error) {
	var rdr io.Reader
	var size int64 = -1
	if body != nil {
		rdr = bytes.NewReader(body)
		size = int64(len(body))
	}
	return w.doReaderContext(ctx, method, url, rdr, size, want...)
}

func (w *WebDAV) doReaderContext(ctx context.Context, method, rawURL string, body io.Reader, size int64, want ...int) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
		if size >= 0 {
			req.ContentLength = size
		}
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
		return nil, fmt.Errorf("webdav %s %s: %s", method, rawURL, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

func (w *WebDAV) metaURL() string { return w.baseURL + "/meta" }

func (w *WebDAV) ensureMetaCollection(ctx context.Context) error {
	w.collectionsMu.Lock()
	known := w.collections["@meta"]
	w.collectionsMu.Unlock()
	if known {
		return nil
	}
	resp, err := w.doContext(ctx, "MKCOL", w.metaURL(), nil, http.StatusCreated, http.StatusMethodNotAllowed)
	if err != nil {
		return fmt.Errorf("创建 WebDAV Meta 目录: %w", err)
	}
	resp.Body.Close()
	w.collectionsMu.Lock()
	w.collections["@meta"] = true
	w.collectionsMu.Unlock()
	return nil
}

func (w *WebDAV) PutMetaContext(ctx context.Context, name string, src io.ReadSeeker, size int64) error {
	if err := validateBlobName(name); err != nil {
		return err
	}
	var err error
	delay := 200 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		if err = w.ensureMetaCollection(ctx); err != nil {
			// 与 PUT 一起重试，临时的 MKCOL/网络失败不应直接终止。
		} else if _, err = src.Seek(0, io.SeekStart); err != nil {
			return err
		} else {
			var resp *http.Response
			resp, err = w.doReaderContext(ctx, http.MethodPut, w.metaURL()+"/"+url.PathEscape(name), src, size)
			if err == nil {
				resp.Body.Close()
				return nil
			}
		}
		w.collectionsMu.Lock()
		delete(w.collections, "@meta")
		w.collectionsMu.Unlock()
		if attempt < 2 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			delay *= 2
		}
	}
	return fmt.Errorf("上传 Meta 备份 %s: %w", name, err)
}

func (w *WebDAV) GetMetaContext(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := validateBlobName(name); err != nil {
		return nil, err
	}
	resp, err := w.doReaderContext(ctx, http.MethodGet, w.metaURL()+"/"+url.PathEscape(name), nil, -1)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (w *WebDAV) ListMetaContext(ctx context.Context) ([]string, error) {
	if err := w.ensureMetaCollection(ctx); err != nil {
		return nil, err
	}
	responses, err := w.propfindContext(ctx, w.metaURL())
	if err != nil {
		return nil, err
	}
	var out []string
	for _, response := range responses {
		name, ok, err := hrefChildName(response.Href, w.metaURL())
		if err != nil {
			return nil, err
		}
		if ok && !collectionResponse(response) && strings.HasSuffix(name, ".cmeta") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (w *WebDAV) DeleteMetaContext(ctx context.Context, name string) error {
	if err := validateBlobName(name); err != nil {
		return err
	}
	resp, err := w.doReaderContext(ctx, http.MethodDelete, w.metaURL()+"/"+url.PathEscape(name), nil, -1,
		http.StatusNoContent, http.StatusNotFound, http.StatusOK)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// putOnce 单次 PUT。
func (w *WebDAV) putOnce(name string, data []byte) error {
	return w.putOnceContext(context.Background(), name, data)
}

func (w *WebDAV) putOnceContext(ctx context.Context, name string, data []byte) error {
	parts, err := bucketParts(name, w.bucketDepth)
	if err != nil {
		return err
	}
	if err := w.ensureCollections(ctx, parts); err != nil {
		return err
	}
	blobURL := w.collectionURL(parts) + "/" + url.PathEscape(name)
	resp, err := w.doContext(ctx, http.MethodPut, blobURL, data)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (w *WebDAV) Put(name string, data []byte) error {
	return w.PutContext(context.Background(), name, data)
}

func (w *WebDAV) PutContext(ctx context.Context, name string, data []byte) error {
	if _, err := bucketParts(name, w.bucketDepth); err != nil {
		return err
	}
	// 指数退避重试（设计文档 §7：后端写失败重试 3 次再中止）
	var err error
	delay := 200 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		if err = w.putOnceContext(ctx, name, data); err == nil {
			return nil
		}
		if attempt < 2 {
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
			delay *= 2
		}
	}
	return fmt.Errorf("写入后端 %s: %w", name, err)
}

func (w *WebDAV) Get(name string) ([]byte, error) {
	blobURL, err := w.blobURL(name)
	if err != nil {
		return nil, err
	}
	resp, err := w.do(http.MethodGet, blobURL, nil)
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
	blobURL, err := w.blobURL(name)
	if err != nil {
		return err
	}
	// 404 视为已删除（幂等）
	resp, err := w.do(http.MethodDelete, blobURL, nil, http.StatusNoContent, http.StatusNotFound, http.StatusOK)
	if err != nil {
		return fmt.Errorf("删除 blob %s: %w", name, err)
	}
	resp.Body.Close()
	return nil
}

// multistatus PROPFIND 响应，同时保留 resource type 以区分 collection。
type multistatus struct {
	Responses []struct {
		Href     string `xml:"href"`
		Propstat []struct {
			Prop struct {
				ResourceType struct {
					Collection *struct{} `xml:"collection"`
				} `xml:"resourcetype"`
			} `xml:"prop"`
		} `xml:"propstat"`
	} `xml:"response"`
}

func (w *WebDAV) ensureCollections(ctx context.Context, parts []string) error {
	for i := 1; i <= len(parts); i++ {
		key := strings.Join(parts[:i], "/")
		w.collectionsMu.Lock()
		known := w.collections[key]
		w.collectionsMu.Unlock()
		if known {
			continue
		}
		resp, err := w.doContext(ctx, "MKCOL", w.collectionURL(parts[:i]), nil, http.StatusCreated, http.StatusMethodNotAllowed)
		if err != nil {
			return fmt.Errorf("创建 WebDAV 分桶 %s: %w", key, err)
		}
		resp.Body.Close()
		w.collectionsMu.Lock()
		w.collections[key] = true
		w.collectionsMu.Unlock()
	}
	return nil
}

func collectionResponse(r struct {
	Href     string `xml:"href"`
	Propstat []struct {
		Prop struct {
			ResourceType struct {
				Collection *struct{} `xml:"collection"`
			} `xml:"resourcetype"`
		} `xml:"prop"`
	} `xml:"propstat"`
}) bool {
	for _, ps := range r.Propstat {
		if ps.Prop.ResourceType.Collection != nil {
			return true
		}
	}
	return false
}

// hrefChildName extracts exactly one child segment relative to currentURL.
func hrefChildName(href, currentURL string) (string, bool, error) {
	h, err := url.Parse(href)
	if err != nil {
		return "", false, err
	}
	c, err := url.Parse(currentURL)
	if err != nil {
		return "", false, err
	}
	hp := strings.TrimSuffix(h.EscapedPath(), "/")
	cp := strings.TrimSuffix(c.EscapedPath(), "/")
	if hp == cp {
		return "", false, nil
	}
	prefix := cp + "/"
	if !strings.HasPrefix(hp, prefix) {
		return "", false, nil
	}
	rel := strings.TrimPrefix(hp, prefix)
	if rel == "" || strings.Contains(rel, "/") {
		return "", false, nil
	}
	name, err := url.PathUnescape(rel)
	if err != nil {
		return "", false, err
	}
	return name, true, nil
}

func (w *WebDAV) propfind(currentURL string) ([]struct {
	Href     string `xml:"href"`
	Propstat []struct {
		Prop struct {
			ResourceType struct {
				Collection *struct{} `xml:"collection"`
			} `xml:"resourcetype"`
		} `xml:"prop"`
	} `xml:"propstat"`
}, error) {
	return w.propfindContext(context.Background(), currentURL)
}

func (w *WebDAV) propfindContext(ctx context.Context, currentURL string) ([]struct {
	Href     string `xml:"href"`
	Propstat []struct {
		Prop struct {
			ResourceType struct {
				Collection *struct{} `xml:"collection"`
			} `xml:"resourcetype"`
		} `xml:"prop"`
	} `xml:"propstat"`
}, error) {
	req, err := http.NewRequestWithContext(ctx, "PROPFIND", strings.TrimSuffix(currentURL, "/")+"/", nil)
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
	return ms.Responses, nil
}

// List 逐层使用 PROPFIND depth=1，返回叶子文件的逻辑 blob 名。
func (w *WebDAV) List() ([]string, error) {
	seen := map[string]bool{}
	var out []string
	var walk func([]string) error
	walk = func(parts []string) error {
		currentURL := w.collectionURL(parts)
		responses, err := w.propfind(currentURL)
		if err != nil {
			return err
		}
		level := len(parts)
		for _, r := range responses {
			name, ok, err := hrefChildName(r.Href, currentURL)
			if err != nil {
				return fmt.Errorf("解析 PROPFIND href: %w", err)
			}
			if !ok {
				continue
			}
			if level < w.bucketDepth {
				if collectionResponse(r) && isBucketPart(name) {
					if err := walk(append(parts, name)); err != nil {
						return err
					}
				}
				continue
			}
			if collectionResponse(r) || isTemporaryBlob(name) {
				continue
			}
			if !blobBelongsToBucket(name, parts, w.bucketDepth) {
				continue
			}
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
		return nil
	}
	if err := walk(nil); err != nil {
		return nil, err
	}
	sort.Strings(out)
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
