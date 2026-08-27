// internal/front/webdav/auth.go
// Basic 认证与 read_only 拦截中间件。
package webdav

import "net/http"

// basicAuth HTTP Basic 认证中间件：users 为空（未配置认证）时匿名放行。
func basicAuth(users map[string]string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if len(users) == 0 {
				next.ServeHTTP(w, r)
				return
			}
			user, pass, ok := r.BasicAuth()
			if !ok || users[user] != pass {
				w.Header().Set("WWW-Authenticate", `Basic realm="crysync"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// writeMethods WebDAV 写方法集（read_only 模块拦截用）。
var writeMethods = map[string]bool{
	http.MethodPut: true, http.MethodDelete: true,
	"MKCOL": true, "COPY": true, "MOVE": true,
	"PROPPATCH": true, "LOCK": true, "UNLOCK": true,
}

// readOnlyGuard 拦截只读模块的写方法（403)。x/net/webdav 的 PUT/DELETE/MKCOL
// 对普通错误映射 404/405，无法表达 403，故在中间件层拦截。
func readOnlyGuard(readOnly bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if readOnly && writeMethods[r.Method] {
			http.Error(w, "read-only module", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
