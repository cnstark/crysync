# CrySync

加密备份工具：对外提供 rsyncd 风格的 rsync 协议服务（兼容 rsync 3.x 客户端，protocol 30/31）。服务端将文件内容 **AES-256-GCM 加密**后存入文件后端，目录树与属性明文存本地 SQLite，每次会话提交一个**多版本快照**，可按需恢复到任意时间点。

## 特性

- ✅ 备份（客户端推送）：真实 rsync 3.4.1 首次/二次/深层目录备份验证通过
- ✅ delta 增量传输：未变化文件 quick check（mtime+size）零交互跳过；变化文件按块校验和只传差异部分（match+literal 重组），传输后整文件 MD5 校验
- ✅ 恢复（客户端拉取）：内容/mode/mtime（纳秒）/符号链接/空目录/中文文件名一致；支持子目录与单文件拉取、`--delete`、`--numeric-ids`
- ✅ 只读模块：可拉不可推
- ✅ 后端：本地目录 + WebDAV（v1）
- ✅ 去重：4 MiB 分块 + SHA-256 明文哈希（增量重组后统一切块，内容寻址天然去重）
- ✅ 快照：多版本 + 活跃快照切换（恢复到任意时间点）
- ✅ prune：restic 保留规则（keep_last/daily/weekly/monthly）+ 孤儿 blob 回收，每日调度
- ✅ 认证：rsyncd 风格 challenge-response（MD5）
- 🚧 未实现（v2）：xattr/硬链接/稀疏文件

## 快速开始（本地）

```bash
# 1. 准备配置
cat > crysync.yaml <<'EOF'
listen: "0.0.0.0:873"
auth:
  users: { backup: "changeme" }   # 明文密码，配置文件权限收紧到 0600
modules:
  - name: home
    path: "/"
    backend: { type: dir, path: /var/lib/crysync/data/home }
    keyfile: /var/lib/crysync/keys/home.key
    meta: /var/lib/crysync/meta/home.db
    prune: { keep_last: 7, keep_daily: 14, keep_weekly: 8, keep_monthly: 6 }
EOF

# 2. 启动 daemon（密钥与元数据库缺失时自动初始化）
crysyncd --config crysync.yaml
```

备份与恢复（标准 rsync 客户端）：

```bash
# 备份（首次全量；再次执行自动增量：未变化文件零传输，变化文件只传差异部分）
rsync -a --password-file=pw.txt /path/to/src/ backup@host::home/

# 恢复（拉回）
rsync -a --password-file=pw.txt backup@host::home/ /path/to/dest/
```

## Docker 部署

```bash
# 1. 准备文件（复制品含本地密码/环境信息，已被 gitignore）
cp docker-compose.example.yml docker-compose.yml
cp conf/crysync.yaml.example conf/crysync.yaml   # 编辑密码与模块，权限 0600

# 2. 直接启动（daemon 启动时逐模块自动初始化，无需手动 init）
docker compose up -d
```

> 迁移部署务必把 `crysync-keys` 与 `crysync-meta` 两个卷（或对应宿主机目录）一并迁走：
> 快照清单存在本地 SQLite，密钥文件是 blob 解密的前提。若 daemon 发现**元数据库已存在
> 而密钥文件缺失**（密钥卷丢失/未挂载），会拒绝自动初始化以防静默换钥导致旧快照永久
> 无法解密--恢复密钥文件，或确认放弃旧数据后删除该元数据库。

目录布局（三类数据分离，对应三个卷）：

| 路径 | 内容 | 挂载 |
|---|---|---|
| `/conf/crysync.yaml` | 配置（只读，0600） | `./conf` |
| `/keys/*.key` | 密钥文件（独立卷——与元数据、数据物理分离） | `crysync-keys` |
| `/meta/*.db` | SQLite 元数据 | `crysync-meta` |
| `/data/` | Dir 后端数据（WebDAV 后端不需要此卷） | `crysync-data` |

### 卷权限

镜像以 root 入口启动，entrypoint 按以下优先级确定运行身份，`su-exec` 降权后运行：

1. 环境变量 `PUID`/`PGID`（`PGID` 缺省 = `PUID`）
2. 自动探测 `/keys` `/meta` `/data` 挂载目录的属主（须一致；`/data` 未挂载则跳过）
3. 都未挂载时回退镜像默认 `1000:1000`

行为说明：

- **bind mount 任意属主的宿主目录零配置**：探测属主后以该身份运行，不修改宿主文件
- 目录属主为 root（dockerd 自动创建目录等场景）：打印警告后以 root 运行，建议宿主机 `chown` 后重启
- 运行身份对挂载目录无写权限：启动报错并给出修复命令（`chown` 或设置 `PUID`/`PGID`）
- compose 显式指定 `user:` 时跳过探测，直接以该身份运行（需自行保证权限）
- named volume 首挂以镜像目录属主 `1000:1000` 初始化，行为与旧版一致

## 配置

```yaml
listen: "0.0.0.0:873"
auth:
  users: { backup: "<密码>" }   # rsyncd 协议限制：明文，配置 0600
modules:
  - name: home
    path: "/"                            # 客户端看到的模块根
    read_only: false                     # true = 只读模块（可拉不可推）
    backend: { type: dir, path: /var/lib/crysync/data/home }
    # 或 WebDAV：backend: { type: webdav, url: https://dav.example.com/crysync, username: ..., password: ... }
    keyfile: /var/lib/crysync/keys/home.key
    meta: /var/lib/crysync/meta/home.db
    prune:                               # 缺省不自动清理；配置后每日执行
      keep_last: 7
      keep_daily: 14
      keep_weekly: 8
      keep_monthly: 6
      schedule: "03:00"                  # 每日执行时刻 HH:MM，缺省 03:00
```

## CLI

```bash
crysync init --config crysync.yaml                    # 显式初始化（可选，幂等；daemon 启动会自动执行）
crysync snapshots --config crysync.yaml               # 列出全部模块快照
crysync snapshots --config crysync.yaml --module home # 指定模块
crysync snapshots --config crysync.yaml --module home --set-active 3   # 切换到快照 3（恢复时间点）
crysync prune --config crysync.yaml --module home     # 手动执行保留策略 + 孤儿 blob 回收
```

`set-active` 切换后，`rsync` 拉取即恢复该时间点的目录树。

## 存储架构

- **文件后端**：只存随机命名的加密 blob（64 hex，无任何目录结构/属性信息）
- **元数据**：目录树 + 属性明文存 SQLite（WAL，每模块一个库）
- **密钥**：独立文件（0600），每模块一个
- **加密**：AES-256-GCM，每块随机 nonce，AAD 绑定 blob 名（防后端 blob 互换攻击）
- **一致性**：blob 先写后端再提交快照；失败/中断会话不产生快照，孤儿 blob 由 GC/prune 回收

## 开发

```bash
go build ./...
go vet ./...
go test ./...          # 全部测试（集成测试依赖 PATH 中的真实 rsync 二进制）
```
