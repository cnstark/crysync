#!/bin/sh
# 子命令（crysync CLI）转发；其余参数启动 daemon（设计文档 §10 初始化流程：
# docker compose run --rm crysync init --config /conf/crysync.yaml）
case "$1" in
  init|snapshots|prune)
    exec crysync "$@"
    ;;
  *)
    exec crysyncd --config /conf/crysync.yaml "$@"
    ;;
esac
