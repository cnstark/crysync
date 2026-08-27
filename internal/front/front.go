// internal/front/front.go
// front 包定义前端层接口：协议无关的服务生命周期。
package front

import "context"

// Front 前端接口：Name 标识协议，Serve 监听并服务直到 ctx 取消。
type Front interface {
	Name() string
	Serve(ctx context.Context) error
}