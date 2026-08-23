# 多阶段构建：golang 构建 -> alpine 运行（保留 shell 便于调试与健康检查）
# CGO_ENABLED=0 静态编译（modernc.org/sqlite 纯 Go 驱动的前提）
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/crysyncd ./cmd/crysyncd \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/crysync ./cmd/crysync

FROM alpine:3.22
# 非 root 运行，UID/GID 默认 1000 可配置
ARG UID=1000
ARG GID=1000
RUN addgroup -g ${GID} crysync && adduser -D -u ${UID} -G crysync crysync
COPY --from=build /out/crysyncd /out/crysync /usr/local/bin/
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
# 三类数据分离，对应三个挂载卷（见 docker-compose.yml）
RUN mkdir -p /keys /meta /data && chown -R crysync:crysync /keys /meta /data
USER crysync
EXPOSE 873
VOLUME ["/keys", "/meta", "/data"]
ENTRYPOINT ["entrypoint.sh"]
