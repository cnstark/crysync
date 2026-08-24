# 多阶段构建：golang 构建 -> alpine 运行（保留 shell 便于调试与健康检查）
# CGO_ENABLED=0 静态编译（modernc.org/sqlite 纯 Go 驱动的前提）
FROM golang:1.26-alpine AS build
# VERSION 由 CI 通过 --build-arg 注入（如 v1.2.3），本地构建缺省 dev
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/crysyncd ./cmd/crysyncd \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/crysync ./cmd/crysync

FROM alpine:3.22
# root 入口：entrypoint 探测挂载卷属主（或 PUID/PGID 环境变量）后 su-exec 降权运行，
# bind mount 任意属主目录零配置（设计：docs/superpowers/specs/2026-08-24-docker-volume-permissions-design.md）
RUN apk add --no-cache su-exec
# 默认运行身份 1000:1000（无数据卷挂载且 PUID 未设时回退；named volume 首挂以镜像目录属主初始化）
RUN addgroup -g 1000 crysync && adduser -D -u 1000 -G crysync crysync
COPY --from=build /out/crysyncd /out/crysync /usr/local/bin/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
# /keys /meta 为必挂卷；/data 仅 Dir 后端需要——不声明 VOLUME（声明会强制
# 匿名卷，WebDAV 后端场景凭空多一个卷），需要时由 compose/docker run 显式挂载
RUN mkdir -p /keys /meta /data && chown -R crysync:crysync /keys /meta /data
EXPOSE 873
VOLUME ["/keys", "/meta"]
ENTRYPOINT ["entrypoint.sh"]
