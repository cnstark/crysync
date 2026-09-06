# 架构与开发

本文描述当前实现。旧快照设计及历史测试结果不代表当前功能或本次验证结果。

## 代码地图

| 包 / 入口 | 职责 |
|---|---|
| `cmd/crysyncd` | 配置和日志装配、启动所选协议前端、处理退出信号 |
| `cmd/crysync` | `init` / `version` CLI |
| `internal/front/rsync` | TCP 服务、模块选择、连接限制 |
| `internal/front/rsync/protocol` | 握手、flist、mux、校验和、备份 receiver 与恢复 sender |
| `internal/front/webdav` | HTTP 认证、模块路由、文件系统适配、根目录与 HTML 浏览 |
| `internal/core/types` | `Session`、`FileStore`、`FileWriter`、`RsyncService` 接口 |
| `internal/core` | 门面及 `OpenModule` / `EnsureModuleInit` 工厂 |
| `internal/core/repo` | 文件访问、分块、去重、流式重组、写锁、上传闸门、进程级在途限流、GC 方法 |
| `internal/core/meta` | SQLite 清单、块引用更新、在线一致快照与结构校验 |
| `internal/core/crypto` | 密钥格式、随机 blob 名、blob 加密与 CRYM 分片 Meta 加密 |
| `internal/backend` | blob 的 `Put/Get/Delete/List/Ping` 及 Meta 快照流式存取接口，Dir / WebDAV 实现与测试用 InMemory |
| `internal/config`、`internal/logging` | YAML 加载与日志轮转 |

前端通过核心接口访问文件；存储实现位于 core 和 backend。接口复用 `meta.FileRow` 等类型，前端不负责加密和后端操作。

## 数据模型

每模块包含一份 SQLite（WAL 模式）、一份密钥和一个独立 blob 目录。两种后端都按 `SHA-256(逻辑 blob 名)` 的前缀使用默认两级分桶，每级两个十六进制字符（可配置为 1～4 级），例如 `ab/cd/<blobName>`。数据库四张表：

| 表 | 内容 |
|---|---|
| `files` | 当前路径、类型、权限、uid/gid、大小、mtime、链接目标 |
| `chunks` | 明文 SHA-256、随机 blob 名、大小和引用计数 |
| `file_chunks` | 文件到有序块的关联 |
| `meta` | 键值元数据 |

文件固定分块后以 SHA-256 查重，命中复用同模块已有块。新块使用 AES-256-GCM 和随机 nonce 加密；AAD 绑定逻辑 blob 名，使不同名字下的密文互换不能通过认证。后端内部才把逻辑名映射为物理分桶路径，数据库和加密层不保存物理路径。该机制不对明文元数据库提供认证，也不提供历史回滚检测。旧平铺布局不读取、不迁移。

blob 先写后端，再写块记录，最后更新文件清单。`meta.UpsertFile` 将单个文件替换、旧/新引用计数维护和块关联放在一个事务内。覆盖/删除让引用归零时移除 chunk 行，后端 blob 成为孤儿。`Repo.GC()` 按 `file_chunks JOIN files` 的实际引用回收，但没有 CLI 或定时调用，也不应假设它能与在途上传安全并行。

本地 Meta 是在线数据库；远端 `meta/` 只保存不可变灾难恢复快照。快照由 SQLite Online Backup 生成，再使用从主 key 域分离派生的密钥按帧 AES-GCM 加密。GC 删除 blob 前会合并当前清单与所有保留 Meta 快照的引用集合；任一快照无法验证时停止删除。

## 写入与并发

```text
rsync push → quick check / delta → 分块、去重、加密、写 blob → 单文件 upsert
             └──────────── 模块写锁覆盖整个写会话 ────────────┘

WebDAV PUT → 进程级 inflight 槽位 → 4 worker 分块查重/加密/上传 → 模块短写锁 → 单文件 upsert
                                  （锁外并发，槽位耗尽形成背压）

读取 → 当前清单与块映射 → 后端 GET → 解密 → 按序重组
```

模块写锁按元数据库路径在进程内共享。rsync 会话调用 `WriteSessionLock`，会话内部不能再调用会二次加锁的 `PutFile`。WebDAV 上传阶段在锁外执行，更新行时才取锁；同路径后提交的写入覆盖先前结果。目录 MOVE/DELETE 是多次行操作，不保证整个目录事务性。

上传闸门也按元数据库路径在进程内共享，只覆盖 `backend.Put`。in-flight 同内容去重表则属于 **Repo 实例**：WebDAV 缓存实例内的并发请求可共享它；其他实例可能先上传重复 blob，再在数据库唯一性冲突后尝试删除自己的副本。不要将其描述成全进程上传去重。

`Runtime` 在 daemon 启动时只创建一个，所有前端打开模块时注入同一个 `InflightLimiter`。WebDAV 在读取每个块之前取得槽位，因此上限约束受控块数据并向请求体施加背压；默认 4 MiB 分块、8 个槽位约 64 MiB。rsync 的 `StoreChunk` 也使用该限制器，但协议层已持有重组块。

当前读取不具备快照隔离，不承诺并发覆盖期间的稳定历史视图。部署时每个仓库由单个 daemon 管理；现有进程内锁不能协调多个 daemon。

## 初始化与生命周期

- `EnsureModuleInit` 会探测数据后端并执行初始化状态机。key 与 Meta 都缺失且远端无备份时创建新仓库；只有 key 时必须从远端恢复，远端无有效备份则报错；Meta 存在但 key 缺失时拒绝生成新密钥。
- daemon 在前端监听前准备模块；恢复候选通过 CRYM 认证、SQLite/关系语义检查以及所有引用 blob 的解密、hash 和大小校验后才原子发布。
- 每模块只有一个 Meta 备份调度器：启动后立即执行，随后默认每小时执行并保留 24 个版本。
- daemon 为每个模块启动 Meta 备份自愈调度器：初始化失败时按 1 秒起、30 秒封顶的指数退避重试，成功后记录 `module_recovered` 并进入备份循环；rsync 连接选择模块时仍通过 `OpenModule` 做幂等状态检查并 Ping 后端。
- WebDAV 前端启动时预打开并缓存模块，然后监听；打开失败的模块通过请求路径懒打开，未就绪返回 503 和 `Retry-After`，单飞成功后加入根列表，无需重启前端。
- daemon 在退出信号或首个前端退出时结束等待，尚无等待全部在途传输完成的停机屏障。

## 开发与验证

所有命令在产品仓库根目录执行，需要 Go 1.26：

```bash
go build ./...
go vet ./...
go test ./...
```

核心、元数据、加密、后端、配置、日志及协议编码有单元测试。集成测试会在本机启动服务；真实 rsync 相关测试依赖 PATH 中的 `rsync`，缺失时部分测试会跳过，不能将这种通过视为客户端互通验证。项目已有 rsync 3.4.1 互通测试记录。

```bash
go test ./internal/front/rsync/...
go test ./internal/front/webdav/...
go test ./internal/core/...
```

保留 `CGO_ENABLED=0` 的静态构建能力，SQLite 使用 `modernc.org/sqlite`。测试临时路径、监听权限、Go 缓存和依赖可用性由运行环境决定；不要因环境失败而改动产品行为。

## 发布

[release.yml](../.github/workflows/release.yml) 在 `v*` tag 推送时构建 Linux amd64/arm64 的 `crysyncd`、`crysync`，打包 tar.gz 和 `SHA256SUMS`，发布 GitHub Release，并构建 GHCR 多架构镜像。二进制版本由 ldflags 注入；镜像版本标签去掉前缀 `v`，另更新 `latest`。

当前发布工作流没有测试步骤，发布前需自行完成相应验证。

## 维护文档

产品首页负责用途与能力边界；使用文档负责可执行操作；配置参考负责字段语义；本文负责实现和开发约束。行为改变时同时检查四处和 example 文件，避免把实现计划写成已发布能力。

开发工作区另有 `docs/superpowers/` 的历史设计与协议研究；这些文件不随独立产品仓库分发。涉及 rsync wire 行为时，使用工作区的上游 rsync 参考源码及协议研究核对。
