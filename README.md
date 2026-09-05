# CrySync

CrySync 是一个自有协议、内容定址的加密备份与同步服务。它以**两个并列的协议前端**对外提供服务：**rsync 前端**用于服务器/备份软件的增量备份与恢复，**WebDAV 前端**用于通用客户端（文件管理器、restic、浏览器）的浏览、上传与下载。两个前端共享同一套核心：加密内容定址存储——明文被分块、按内容去重、逐块加密后存放于任意文件后端，属性和目录树保存在 SQLite 元数据库中。v0.5 起为**单一当前状态模型**：清单随每次写入原地更新（无快照历史），语义等同镜像备份。

```
         ┌────────────────── CrySync 服务进程 ──────────────────┐
 rsync ──┤  rsync 前端         ┌───────┐     ┌───────────────┐   │
 客户端   │  (TCP 873)          │  核心  │     │   文件后端     │   │
         │  增量备份/恢复       │ 加密/  │────▶│ Dir / WebDAV  │   │
 WebDAV ─┤  WebDAV 前端        │ 加密/  │     │（随机命名 blob）│  │
 客户端   │  (HTTP 8080)        │ 去重   │     └───────────────┘   │
         │  浏览/上传/下载      └───────┘                          │
         └─────────────────────────────────────────────────────────┘
```

## 特性

### rsync 前端

- **rsync daemon 协议**：greeting 宣告协议 31.0（接受客户端协议 ≥ 30），完整的握手 / 模块选择 / **MD5 challenge-response 认证** / argv 协商 / flist 编解码 / mux 帧流 / goodbye 序列，与真实 rsync 3.4.1 客户端互通（已通过差分与集成测试验证）。
- **增量传输**：二次备份对未变化文件走 **quick check**（mtime + size 一致即跳过，零字节传输）；变化文件基于库中当前内容作为 basis 发**块校验和 delta**（滚动校验和 + MD5 强校验和，客户端只回传变化块），客户端校验整文件 MD5 通过后才落库。
- **恢复方向**：支持拉取模块全量或子路径；恢复路径不存在时对齐真实 rsyncd 报错（客户端 rc=23），不静默返回空。
- **只读模块**：配置 `read_only: true` 的模块拒绝推送（push），对齐「可拉不可推」。
- **会话控制**：`--delete`（源侧删除同步，io_error 时禁用）、`--dry-run`（协议走完不落库）、`--checksum`（flist 附内容校验和）、`--timeout=N`（会话空闲超时）、递归 / 非递归（`--list-only`）、`-l` 符号链接、`-o/-g` 保留 uid/gid、`-p` 权限位。
- **明确拒绝不支持选项**：压缩（`-z/--compress*`）、`--append/--append-verify`、恢复方向 `--atimes`，经 mux 错误帧告知客户端并以正确退出码结束，不做静默错误数据。
- **设备/特殊文件**（CHR/BLK/FIFO/SOCK）：协议交互正常走完，跳过落库并日志警告（会话不因此失败）。

### WebDAV 前端

- **文件服务型 WebDAV**：基于 `x/net/webdav`，支持 PROPFIND / GET（含 Range 下载）/ PUT / MKCOL / DELETE / COPY / MOVE。
- **写即可见**：每个写请求（PUT / MKCOL / DELETE / MOVE / COPY）完成后立即原地更新清单并立即可见，行为等价「改完立即可见的工作树」。
- **Basic 认证**：`front.webdav.auth` 独立配置用户集；未配置用户时匿名放行。
- **只读模块**：`read_only: true` 的模块对全部写方法（PUT/DELETE/MKCOL/COPY/MOVE/PROPPATCH/LOCK/UNLOCK）返回 403。
- **no-op 锁**：接受 LOCK/UNLOCK 但不维护状态（Windows/macOS 客户端偶发 LOCK 不报错）；写冲突真正由模块级写互斥保障（行更新段串行化，blob 上传并发）。
- **并发写不丢数据**：blob 上传在锁外并发（内容定址天然安全，4 worker 池 + in-flight 去重），行更新按路径原地 upsert——并发写不同路径互不覆盖，同路径后写者胜。
- **HTML 目录浏览**：浏览器直接 GET 目录返回 HTML 文件列表（目录在前、文件名排序、人类可读大小与修改时间），文件可直接点击下载。
- **服务器根虚拟目录**：挂载服务器根（`http://host:port/`）时 PROPFIND 列出全部已就绪模块（集合），浏览器根路径显示模块链接；根自身只读（写方法 405）。
- **restic 系备份软件可作对端存储**：NAS 备份任务实测（绿联 NAS 的 restic 式仓库布局经 WebDAV init/上传/快照全流程通过，其 fork 的 4 并发 PUT 上传由并发写路径直接受益）；restic 工具本身可经 rclone WebDAV 桥接使用，每次 PUT 即落库。

### 核心能力（两前端共享）

- **单一当前状态（v0.5）**：无快照历史——`files` 表即完整清单，每次 rsync 会话 / 每次 WebDAV 写在 SQLite 单事务内**原地替换**（upsert）受影响路径的行，删除同步（`--delete`）原地删行。语义等同镜像备份：修改即覆盖、删除即消失，只反映当前状态。
- **会话失败 = 半更新镜像**：rsync 会话中途失败时已落库的部分保留（无回滚），客户端重试时 quick check 跳过已正确文件，幂等收敛到一致状态；`--delete` 在客户端 io_error 时自动禁用，不误删。
- **内容定址去重**：文件按 4 MiB 固定分块（`chunk_size` 可配置），对明文块做 SHA-256 哈希，重复内容（同一文件多次写入、跨文件相同块）零额外存储。
- **加密存储**：每块用 **AES-256-GCM** 加密；GCM 的 AAD 绑定 blob 名，防止把 blob 从一个文件换到另一个（篡改重放攻击）。密钥文件独立存放、0600 权限，与元数据、数据物理分离。
- **属性与内容物理分离**：内容只以随机命名（64 hex）的加密 blob 存在文件后端，目录树/文件名/权限/uid/gid/mtime 等全部明文存 SQLite，后端目录里看不出任何目录结构与原文件信息。
- **SQLite 元数据**：WAL 模式、每模块一个库；`files/chunks/file_chunks/meta` 四表；chunk 跨文件共享——覆盖/删除时递减引用，归零的 chunk 行在 upsert 事务内删除、对应 blob 成为孤儿。
- **一致性写序**：blob 先写后端，再原地更新清单行；失败/中断的写不落行，其残留 blob 由 **GC 孤儿回收**清理（以清单真引用为准——失败残留的零引用 chunk 行连同 blob 一并回收）。
- **并发上传**：blob 上传相互并行（4 worker 池 + 跨请求 in-flight 去重防同内容双写后端），适合高固定开销的 WebDAV 后端（实测夸克网盘并发吞吐 ~1.4× 单流）。
- **模块 = 仓库**：每个 rsync 模块是一个独立仓库（自己的 SQLite + keyfile + 后端目录）；`max_connections` 限制单模块并发连接（0 无限制 / 负值禁用 / 正数上限）。
- **持久化日志**：slog TextHandler 同时写 stderr 与轮转文件（`log` 节可配置路径/级别/大小/保留份数），rsync 会话逐文件埋点（path/size/mtime/mode/uid/gid/method，delta 含 matched/literal/blength 等）。
- **静态单二进制**：纯 Go（`modernc.org/sqlite`），`CGO_ENABLED=0`，无运行时依赖，linux/amd64 + arm64。

## 快速开始

### 1. 配置文件

创建 `crysync.yaml`（结构见下文「配置说明」，此处为最小双前端示例）：

```yaml
front:
  rsync:
    listen: "0.0.0.0:873"
    auth:
      users: { backup: "changeme" }   # rsync 协议限制：口令明文，配置文件权限收紧（0600）
  webdav:
    listen: "0.0.0.0:8080"
    auth:
      users: { user: "passwd" }       # Basic 认证（明文 base64），生产建议 TLS 反代
log:
  file: /var/log/crysync/crysyncd.log
  level: info                         # debug/info/warn/error，缺省 info
modules:
  - name: home                        # 模块名 = rsync 模块，也是 WebDAV 挂载路径
    path: "/"                         # 只读展示用途（rsyncd 模块列表/WebDAV 根）
    backend: { type: dir, path: /data/home }   # 或 type: webdav（任意 WebDAV 存储作后端）
    keyfile: /keys/home.key           # 密钥文件（独立卷，0600，与数据/元数据隔离）
    meta: /meta/home.db               # SQLite 元数据库（每模块一个）
```

配置中还支持 `${VAR}` / `$VAR` 环境变量展开（加载时先展开再解析 YAML），口令、后端 URL 等敏感项可用环境变量注入。

### 2. 启动

二进制方式：

```bash
crysyncd --config crysync.yaml
```

daemon 启动时自动完成逐模块初始化：密钥缺失自动生成（密钥与元数据库均不存在的首次自举），打开后端并 Ping 探活，随后同时监听 rsync 与 WebDAV 两个端口。也可以先用 `crysync init --config crysync.yaml` 显式初始化（幂等，可提前完成部署）。

Docker 方式见「部署」一节。

### 3. rsync 客户端用法

```bash
# 备份：本地目录 → CrySync 模块（增量：quick check + delta）
rsync -a --port=873 --password-file=pw.txt src/ backup@127.0.0.1::home/

# 恢复：模块/子路径 → 本地
rsync -a --port=873 --password-file=pw.txt backup@127.0.0.1::home/ dst/

# 列模块
rsync --port=873 backup@127.0.0.1::

# 子路径备份/恢复（模块内任意目录前缀）
rsync -a --port=873 --password-file=pw.txt backup@127.0.0.1::home/documents/ dst/

# 常用选项：-n 试跑不落库、-c 内容校验、--delete 同步删除、-l 符号链接、-o/-g 保留 uid/gid、
#           -p 权限、--timeout=1800 会话空闲超时
```

### 4. WebDAV 客户端用法

```bash
# 系统文件管理器（Nautilus/GNOME Files，默认 DAVS/HTTP 都支持）
# 地址：http://127.0.0.1:8080/home/

# restic 系备份软件可把 WebDAV 当作对端存储（restic 工具本身经 rclone
# WebDAV 桥接：rclone config 建 webdav remote，restic -r rclone:remote:path）
# NAS 备份任务实测：绿联 NAS 的 restic 式仓库布局（.ubk，config/keys/
# locks/snapshots/index/data）经 WebDAV init/上传全流程通过

# 浏览器直接浏览/下载：http://127.0.0.1:8080/home/（HTML 目录列表）
# 挂载服务器根查看全部模块列表：http://127.0.0.1:8080/
```

## 配置说明

顶层结构：

```yaml
front:      # 前端层：每种协议前端一节（至少启用一个）
  rsync:    #   可选，启则监听
  webdav:   #   可选，启则监听
log:        # 持久化日志（可选）
modules:    # 至少一个模块
```

### front 节

| 字段 | 说明 |
|---|---|
| `front.rsync.listen` | rsync daemon 监听地址（如 `0.0.0.0:873`），必填 |
| `front.rsync.auth.users` | 用户 → 口令映射（MD5 challenge-response 认证）；缺省不认证 |
| `front.webdav.listen` | WebDAV HTTP 监听地址（如 `0.0.0.0:8080`），必填 |
| `front.webdav.auth.users` | 用户 → 口令映射（Basic 认证）；缺省匿名放行 |

### modules 节

| 字段 | 缺省 | 说明 |
|---|---|---|
| `name` | — | 模块名：rsync 模块选择名，也是 WebDAV 路径前缀 |
| `path` | — | 模块根路径（rsyncd 模块列表展示 / WebDAV 根虚拟目录） |
| `read_only` | `false` | 只读模块：rsync 拒绝推送；WebDAV 写方法一律 403 |
| `max_connections` | `0`（无限制） | 连接上限：`0` 无限制，`负数` 禁用模块，`正数` 并发上限 |
| `backend.type` | — | `dir`（本地目录）或 `webdav`（任意 WebDAV 存储） |
| `backend.path` | — | dir 后端的数据目录 |
| `backend.url` / `username` / `password` | — | webdav 后端的地址与 Basic 凭据 |
| `keyfile` | — | 密钥文件路径（缺失首启时自动生成，0600） |
| `meta` | — | SQLite 元数据库路径（每模块一个库） |
| `chunk_size` | `4194304`（4 MiB） | 内容分块大小（字节） |

校验规则：front 至少一个；模块名唯一非空；`backend.type`/`keyfile`/`meta` 必填；`log.level` 限 `debug/info/warn/error`；`log.max_files` 显式 `0` = 不轮转（区别于缺省 5）。

### 后端存储

`Backend` 抽象只暴露 `Put/Get/Delete/List/Ping`：

- **dir**：本地目录，blob 以文件存放（写入为临时文件 + rename 原子落盘）。
- **webdav**：任何 HTTP/HTTPS WebDAV 存储（私有云盘、NAS、对象存储网关等）。Put=PUT、Get=GET、Delete=DELETE、List=PROPFIND(depth=1)、Ping=PROPFIND；写失败指数退避重试 3 次，DELETE 对 404 幂等。blob 名恒为 64 hex，无目录分隔符，任意 WebDAV 目录根均可直接使用。

## 数据模型（v0.5：单一当前状态）

v0.5 起删除快照功能，仓库 = **一份始终反映当前状态的镜像**：

- `files` 表即完整清单，无 `snapshots` 表与版本列；每次写入 = 该路径行的**原子原地替换**（单事务内完成旧引用递减、新引用递增、行替换与块关联，相同内容覆盖不会产生孤儿引用）。
- **rsync 会话**：整个会话持有模块写互斥（同模块并发写排队），逐文件落库；`--delete` 在传输根前缀内同步删除（客户端 io_error 时自动禁用）；dry-run 零持久化。会话中途失败 = 半更新镜像——已落库部分保留，客户端重试以 quick check 幂等收敛。
- **WebDAV 写**：单请求短持写互斥；blob 上传在锁外并发（多请求并行 PUT 互不阻塞），行更新段串行。
- 无历史回溯能力：如需时间点恢复请改用支持版本化的备份工具链或对 WebDAV 后端另做快照。

> v0.4.x 升级说明：旧版元数据库（含快照表）会被明确拒绝打开（错误提示删除重建）。v0.5 不提供数据迁移——删除旧库与旧 blob 后对新架构重新备份。

## 部署

### Docker（推荐）

镜像位于 GHCR：`ghcr.io/cnstark/crysync`（linux/amd64 + arm64，带 `latest` 与版本 tag）。提供 `docker-compose.example.yml`：

```yaml
services:
  crysync:
    image: ghcr.io/cnstark/crysync:latest
    ports:
      - "873:873"        # rsync；改宿主端口时同步改 rsync --port
      - "8080:8080"      # WebDAV（按需开放）
    volumes:
      - ./conf:/conf:ro        # 配置文件（crysync.yaml，0600）
      - crysync-keys:/keys     # 密钥卷（与元数据物理分离）
      - crysync-meta:/meta     # SQLite 元数据卷
      - /srv/crysync/data:/data  # Dir 后端 blob 数据：bind mount（任意属主零配置）
volumes:
  crysync-keys:
  crysync-meta:
```

- **数据分离**：`/keys`（密钥）与 `/meta`（元数据）为独立命名卷，互为备份可独立挂载/恢复；`/data`（blob）用 bind mount 宿主目录（全部用 WebDAV 后端时去掉该行）。
- **权限自举**：`entrypoint.sh` 以 root 进入后确定运行身份再 `su-exec` 降权——优先级 `PUID/PGID` 环境变量 > 挂载目录属主探测 > 默认 `1000:1000`；挂载目录属主不一致或身份无写权限时报错并给出修复命令。bind mount 任意属主目录零配置。
- **密钥保护**：密钥只应存在于 `/keys` 卷。密钥文件缺失但元数据库已存在时**拒绝自动生成新密钥**（防止密钥卷丢失时静默换钥导致旧数据永久不可解密），报错交由人工决策。
- **配置保护**：`conf/crysync.yaml` 含口令，建议权限 0600（`.gitignore` 已排除本地 `crysync.yaml`；`docker-compose.yml` 亦排除）。
- **容器内 CLI**：`docker compose run --rm crysync init --config /conf/crysync.yaml` 等。

### 二进制

发布产物为两平台（linux/amd64、arm64）的静态可执行文件 tar.gz（含 `SHA256SUMS`）：`crysyncd`（daemon）与 `crysync`（CLI）。静态二进制可放在任意目录直接运行；安装为系统服务时配置 `log.file` 持久化日志、systemd 管理生命周期。

## CLI

| 命令 | 说明 |
|---|---|
| `crysyncd --config <path>` | 启动 daemon：加载配置、逐模块自动初始化（密钥+元数据）、按 `front` 节启动 rsync/WebDAV 前端；SIGINT/SIGTERM 优雅退出 |
| `crysyncd --version` | 打印版本（构建时 ldflags 注入） |
| `crysync init --config <path>` | 显式初始化全部模块（幂等；密钥缺失自动生成、元数据库建库，与 daemon 启动共用逻辑，可选 `--module` 过滤） |
| `crysync version` | 打印版本 |

## 架构

三层分层（对应包名 `internal/front/`、`internal/core/`、`internal/backend/`）：

```
前端层 front                    核心层 core                    后端层 backend
rsync 前端（TCP）  ────────▶  Session / FileStore / FileWriter  ───▶  Dir / WebDAV
WebDAV 前端（HTTP）─────────┘         │                              (随机命名加密 blob)
                                      │
                                      ├── repo：清单原地更新 / 模块写锁 / 并发上传
                                      │        4MiB 分块 SHA-256 去重
                                      ├── crypto：AES-256-GCM + AAD 绑定 blob 名
                                      └── meta：SQLite（WAL）files/chunks/file_chunks/meta
```

- **front**：协议前端，只依赖核心层的窄接口（`Session` 备份方向 / `FileStore` 读 / `FileWriter` 写），看不到任何存储细节。
- **core**：核心加密备份。模块打开即自动初始化密钥与元数据库；对前端暴露 `OpenModule` 工厂与三类窄接口，实现封装在 `repo/meta/crypto`。
- **backend**：存储抽象，三种实现互不感知内容与元数据。

数据流：

```
备份:  rsync 客户端 → front/rsync(receiver，会话持模块写锁) → 分块/去重/加密
       → backend.Put（blob 并发上传） → 逐文件原地 upsert（会话内）
恢复:  backend.Get → crypto 解密 → repo 按块重组（含整文件 MD5 校验）
       → core.FileStore → front/rsync(sender) → rsync 客户端
浏览:  WebDAV PROPFIND/GET → core.FileStore（当前清单，SQLite 零物理 IO）
上传:  WebDAV PUT/MKCOL/DELETE/MOVE/COPY → core.FileWriter（blob 锁外并发
       → 短锁内原地 upsert）→ 立即可见
```

模块生命周期：`crysyncd` 启动时逐个 `core.OpenModule`（密钥校验、meta 打开、后端构造与 Ping 探活）；rsync 连接在握手选定模块后打开仓库；WebDAV 前端启动时预打开全部模块并缓存。

## 已知限制

- **rsync 会话失败 = 半更新镜像**：会话中途失败不回滚——失败前已落库的文件保留当前状态（内容均为客户端校验通过后的完整文件，无半截文件），未传输的文件保持原状；客户端重试即幂等收敛。极端情况下 `--delete` 与后续传输之间的中断会留下混合状态，重试同样收敛。
- **PUT 断流语义**：WebDAV PUT 上传中途断流时不落行（已上传的新 blob 成孤儿由 GC 回收），文件保留旧内容或不存在；客户端重试即可。可靠的大数据上传请走 rsync 会话（quick check + delta 增量）。
- **设备/特殊文件**：rsync 备份中 CHR/BLK/FIFO/SOCK 条目跳过不落库（协议正确走完 + 警告日志）；恢复方向不产出设备文件。
- **符号链接**：rsync 需 `-l`（preserve links）才会备份并恢复 symlink；WebDAV 无链接语义——PROPFIND 呈现为普通文件、GET 返回链接目标文本，不跟随也不创建。
- **不支持压缩/追加**：`-z/--compress*`、`--append/--append-verify`、恢复方向 `--atimes` 会被明确拒绝（错误帧 + 退出码），不会产生错误数据。
- **口令明文**：rsync 认证口令与 WebDAV Basic 凭据为明文配置（协议约束），请收紧配置权限（0600）并用环境变量注入；网络侧生产建议 TLS 反代（WebDAV 亦未内置 TLS）。
- **未实现（v2 规划）**：xattr、硬链接、稀疏文件传输，rsync 协议后端（模块后端目前仅 `dir`/`webdav`），多用户，元数据加密（内容已加密，目录树/属性明文存 SQLite）。

## 开发与验证

- Go 1.26，`CGO_ENABLED=0`，依赖：`x/net`（WebDAV）、`yaml.v3`（配置）、`modernc.org/sqlite`（纯 Go SQLite）。
- `go build ./...` / `go test ./...`：单元测试无外部依赖；集成测试依赖 PATH 中的真实 `rsync` 3.4.1 客户端（备份/恢复/增量/子路径/原地收敛/跨前端一致性）。
- 架构与协议设计文档位于主仓库 `docs/superpowers/`（`2026-08-22-crysync-design.md` 权威设计、`2026-08-28-trilayer-architecture-design.md` 三层架构、协议研究笔记两篇）。