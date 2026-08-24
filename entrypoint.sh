#!/bin/sh
# Docker 入口。root 启动时先确定运行身份，su-exec 降权后重入自身；非 root
# （用户显式 user: 指定）直接执行。设计文档：
# docs/superpowers/specs/2026-08-24-docker-volume-permissions-design.md
#
# 身份确定优先级：PUID/PGID 环境变量 > 挂载目录属主探测 > 默认 1000:1000。
# 纯适配：不做任何 chown/chmod；身份对挂载目录无写权限时报错退出并给出修复命令。
#
# 子命令（crysync CLI）转发；其余参数启动 daemon（daemon 启动时逐模块自动初始化，
# init 仅为提前显式初始化的可选入口：docker compose run --rm crysync init --config /conf/crysync.yaml）

DATA_DIRS="/keys /meta /data"

die() {
  echo "[entrypoint] 错误: $1" >&2
  exit 1
}

if [ "$(id -u)" = "0" ]; then
  # 挂载点判断（/proc/mounts 第 2 列精确匹配；路径含空格时转义为 \040，容器内路径无空格）
  is_mountpoint() {
    awk -v p="$1" '$2 == p { found = 1 } END { exit !found }' /proc/mounts
  }

  # 收集实际挂载的数据目录（/data 未挂载的 WebDAV 场景自动跳过）
  mounted=""
  for d in $DATA_DIRS; do
    is_mountpoint "$d" && mounted="$mounted $d"
  done

  if [ -n "$PUID" ]; then
    # 显式指定（PGID 缺省 = PUID）
    case "$PUID" in *[!0-9]*) die "PUID 必须是数字: '$PUID'" ;; esac
    [ -n "$PGID" ] || PGID="$PUID"
    case "$PGID" in *[!0-9]*) die "PGID 必须是数字: '$PGID'" ;; esac
    uid="$PUID"; gid="$PGID"
  elif [ -n "$mounted" ]; then
    # 探测：所有挂载目录属主须一致
    uid=""; gid=""
    for d in $mounted; do
      st=$(stat -c '%u %g' "$d") || die "无法读取目录属主: $d"
      u=${st%% *}; g=${st#* }
      if [ -z "$uid" ]; then
        uid="$u"; gid="$g"
      elif [ "$u" != "$uid" ] || [ "$g" != "$gid" ]; then
        die "挂载目录属主不一致: $d 属主 $u:$g，先前目录属主 $uid:$gid。请在宿主机统一属主后重启，或设置 PUID/PGID 环境变量"
      fi
    done
  else
    # 未挂载任何数据卷，回退镜像默认
    uid=1000; gid=1000
  fi

  # 属主为 root（dockerd 自动创建的 bind mount 目录等）：警告后以 root 运行
  if [ "$uid" = "0" ]; then
    echo "[entrypoint] 警告: 挂载目录属主为 root，daemon 将以 root 运行。" >&2
    echo "[entrypoint] 建议在宿主机执行 chown -R <uid>:<gid> <挂载目录> 后重启以启用非 root 运行" >&2
  else
    # 权限校验：运行身份对每个挂载的数据目录须有写权限（u+w / g+w / o+w 任一满足）
    for d in $mounted; do
      st=$(stat -c '%u %g %a' "$d") || die "无法读取目录权限: $d"
      owner=${st%% *}
      rest=${st#* }
      group=${rest%% *}
      mode=${rest#* }
      perm=$((0$mode & 0777))
      if { [ "$uid" = "$owner" ] && [ $((perm & 0200)) -ne 0 ]; } ||
         { [ "$gid" = "$group" ] && [ $((perm & 0020)) -ne 0 ]; } ||
         [ $((perm & 0002)) -ne 0 ]; then
        :
      else
        die "运行身份 $uid:$gid 对目录 $d 无写权限。修复：宿主机执行 chown -R $uid:$gid <对应宿主目录>，或设置 PUID/PGID 环境变量"
      fi
    done
    # 降权重入自身：第二遍走非 root 分支，子命令分发逻辑零改动
    exec su-exec "$uid:$gid" /usr/local/bin/entrypoint.sh "$@"
  fi
fi

case "$1" in
  init|snapshots|prune|version|--version|-v)
    exec crysync "$@"
    ;;
  *)
    exec crysyncd --config /conf/crysync.yaml "$@"
    ;;
esac
