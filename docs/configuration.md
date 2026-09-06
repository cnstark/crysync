# 配置参考

配置为 YAML。完整可复制模板见 [conf/crysync.yaml.example](../conf/crysync.yaml.example)，本地路径示例见[使用与部署](usage.md#本地快速开始)。本页描述当前代码。

## 前端与认证

| 字段 | 含义 |
|---|---|
| `front.rsync.listen` | rsync TCP 监听地址，如 `0.0.0.0:873` |
| `front.rsync.auth.users` | 用户名到口令的映射；使用 MD5 challenge-response |
| `front.webdav.listen` | WebDAV HTTP 监听地址，如 `0.0.0.0:8080` |
| `front.webdav.auth.users` | 用户名到口令的映射；使用 HTTP Basic 认证 |

只配置需要的前端，至少保留一个，并填写监听地址。省略某前端的用户表或使用空表时，该前端匿名放行。用户表按前端独立配置；所有该前端用户共享模块权限，没有每用户模块 ACL。WebDAV 前端未内置 TLS。

## 模块

`modules` 为模块列表，每个模块配置独立的密钥、元数据库和后端目录。名称应唯一，使用适合作为 URL 路径段的简单名称。

| 字段 | 缺省 / 要求 | 含义 |
|---|---|---|
| `name` | 必填 | rsync 模块名、WebDAV URL 第一段 |
| `path` | 可省略 | rsync 模块列表展示值；不指定本地源目录或存储位置 |
| `read_only` | `false` | 禁止两前端写入该模块 |
| `max_connections` | `0` | **仅 rsync**：0 不限，正数限制连接数，负数禁用模块访问；不限制 WebDAV |
| `max_upload_concurrency` | `0` | 同进程内跨连接/前端的模块 blob 上传上限；正数限流，非正数不限；读和删除不占槽位 |
| `backend.type` | 必填 | `dir` 或 `webdav` |
| `backend.path` | dir 使用 | 本地 blob 目录 |
| `backend.url` | webdav 使用 | HTTP/HTTPS 后端目录 URL，须已存在且可访问 |
| `backend.username` / `password` | 可省略 | WebDAV 后端 Basic 凭据，与前端认证独立 |
| `backend.bucket_depth` | `2`（填 `0` 也为 `2`） | Dir 与 WebDAV 共用的哈希分桶级数，只能为 `1`～`4`；每级取 SHA-256(blob 名) 的两个小写十六进制字符 |
| `keyfile` | 必填 | 密钥文件路径；首次创建权限为 `0600` |
| `meta` | 必填 | SQLite 文件路径 |
| `chunk_size` | `4194304` | 分块字节数；非正数回退 4 MiB；仓库使用期间保持固定 |
| `meta_backup.interval` | `1h` | 加密 Meta 完整快照周期，必须为正的 Go duration（如 `30m`、`2h`） |
| `meta_backup.retain` | `24` | 后端 `meta/` 中保留的快照版本数，必须为正数 |

`max_upload_concurrency` 限制的是同时进行的后端 PUT 数，不是带宽或 HTTP 客户端数。WebDAV 文件写入内部有 4 个上传 worker；多个请求共用模块上传上限。没有配置热重载，修改后重启生效。进程内锁和闸门不提供多进程共享仓库协调能力。

两种后端都将 blob 保存到分桶路径，例如 `ab/cd/<blobName>`，其中 `ab/cd` 来自逻辑 blob 名的 SHA-256 前缀，文件名仍是完整逻辑名。分桶不能关闭，深度在仓库建立后保持不变；深度越大，WebDAV 的 MKCOL 和 PROPFIND 请求越多。此次布局变化不读取或迁移旧的根目录平铺 blob。

Meta 仍从本地 SQLite 提供在线服务。daemon 启动后立即生成一份 SQLite Online Backup 一致快照，随后按 `interval` 定时生成；快照使用独立的分片 AES-GCM 格式加密并保存到后端根的 `meta/`，成功回读验证后才淘汰旧版本。备份失败会记录错误并保留已有版本，不中断在线读写。周期就是最大恢复点间隔；一小时周期意味着本地 Meta 丢失时最多丢失约一小时的清单更新。

## 进程级上传内存

| 字段 | 缺省 / 要求 | 含义 |
|---|---|---|
| `upload.max_inflight_chunks` | `8`，必须为正整数 | 所有模块、前端和连接共享的在途块数；槽位耗尽时 WebDAV PUT 停止读取请求体 |

每个在途块覆盖读取、查重、加密和后端上传全过程。默认分块 4 MiB、8 个槽位时，受控块数据约占 64 MiB；还需为 Go runtime、HTTP/TLS、SQLite 和文件块引用预留至少同等额外空间。反向代理可能自行缓存请求体，部署时需按代理设置评估客户端背压效果。

WebDAV 后端示例：

```yaml
modules:
  - name: archive
    path: "/"
    max_upload_concurrency: 4
    backend:
      type: webdav
      url: "${STORAGE_URL}"
      username: "${STORAGE_USER}"
      password: "${STORAGE_PASSWORD}"
      bucket_depth: 2
    keyfile: /keys/archive.key
    meta: /meta/archive.db
    meta_backup:
      interval: 1h
      retain: 24
```

后端需要支持 PUT、GET、DELETE、PROPFIND。当前 PUT 失败最多尝试 **3 次（含首次）**，间隔 200 ms、400 ms；GET 不重试。HTTP 客户端超时 300 秒，Ping 探测超时 10 秒。

## 日志

| 字段 | 默认值 | 含义 |
|---|---|---|
| `log.file` | 空 | 空时仅 stderr；指定文件后同时写文件和 stderr |
| `log.level` | `info` | `debug` / `info` / `warn` / `error` |
| `log.max_size_mb` | `16` | 文件大小软限制（MiB）；启用文件日志时必须为正数 |
| `log.max_files` | `5` | 轮转文件保留数，显式 0 不轮转 |

文件日志路径须对运行身份可写；容器持久化日志需挂载相应目录。debug 日志适合排查协议，长期启用会增加日志量。

## 环境变量与校验边界

加载顺序为读取文件 → `os.ExpandEnv` 展开 `${VAR}` / `$VAR` → YAML 解析。变量必须存在于服务进程环境中；Docker Compose 的环境文件不会自动成为容器环境，需通过 `environment` 或 `env_file` 显式传入。

未定义变量展开为空串；含 `$` 的字面口令也会被尝试展开。展开在 YAML 解析前发生，值中的引号、反斜杠、换行等可能改变 YAML 语义，不能把简单加引号当成任意凭据的安全转义。

当前加载器检查模块必需字段和日志参数，但使用非严格 YAML 解析，未知字段会被忽略；更完整的 `Config.Validate()` 尚未接入 daemon/CLI 加载路径。因此不要依赖启动来检测所有拼写错误、重复模块名或缺失前端。

旧顶层 `listen` / `auth` 结构不再配置前端；旧 `snapshot` / `prune` 字段没有效果，应从配置删除。含 `snapshots` 表的旧元数据库会被拒绝打开，不提供原地迁移。
