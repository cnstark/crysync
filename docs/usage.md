# 使用与部署

## 本地快速开始

以下命令在产品仓库根目录执行。需要 Go 1.26，备份和恢复示例需要 `rsync`。示例监听回环地址，使用当前用户可写的独立目录。

```bash
mkdir -p bin
CGO_ENABLED=0 go build -o bin/crysyncd ./cmd/crysyncd
CGO_ENABLED=0 go build -o bin/crysync ./cmd/crysync
mkdir -p /tmp/crysync-demo/src /tmp/crysync-demo/dst
printf 'hello CrySync\n' > /tmp/crysync-demo/src/hello.txt
```

将以下内容保存为 `/tmp/crysync-demo/crysync.yaml`：

```yaml
front:
  rsync:
    listen: "127.0.0.1:8873"
    auth:
      users: { backup: "demo-password" }
  webdav:
    listen: "127.0.0.1:8875"
    auth:
      users: { backup: "demo-password" }
modules:
  - name: home
    path: "/"
    backend:
      type: dir
      path: /tmp/crysync-demo/data
      bucket_depth: 2
    keyfile: /tmp/crysync-demo/keys/home.key
    meta: /tmp/crysync-demo/meta/home.db
    meta_backup:
      interval: 1h
      retain: 24
```

```bash
chmod 600 /tmp/crysync-demo/crysync.yaml
./bin/crysyncd --config /tmp/crysync-demo/crysync.yaml
```

日志默认输出到 stderr。首次运行会生成密钥和元数据库；在另一终端完成备份和恢复：

```bash
printf '%s\n' 'demo-password' > /tmp/crysync-demo/pw.txt
chmod 600 /tmp/crysync-demo/pw.txt
rsync -a --port=8873 --password-file=/tmp/crysync-demo/pw.txt   /tmp/crysync-demo/src/ backup@127.0.0.1::home/
rsync -a --port=8873 --password-file=/tmp/crysync-demo/pw.txt   backup@127.0.0.1::home/ /tmp/crysync-demo/dst/
diff -r /tmp/crysync-demo/src /tmp/crysync-demo/dst
```

再次运行推送命令时，mtime 和大小相同的文件走 quick check；变化文件可以利用当前内容进行 delta 传输。添加 `--delete` 会删除传输范围内源侧已不存在的文件；可先加 `--dry-run` 检查。

## WebDAV 访问

沿用本地示例配置：

```bash
# 列出已就绪模块
curl -u backup:demo-password -X PROPFIND -H 'Depth: 1' http://127.0.0.1:8875/
# 上传与下载（父目录须存在）
curl -u backup:demo-password -T /tmp/crysync-demo/src/hello.txt   http://127.0.0.1:8875/home/from-webdav.txt
curl -u backup:demo-password http://127.0.0.1:8875/home/from-webdav.txt
```

浏览器访问 `http://127.0.0.1:8875/` 可查看已就绪模块，进入 `/home/` 浏览文件。后端尚未就绪的模块请求会返回 503 和 `Retry-After`，后续请求会懒打开并在成功后加入根列表。文件管理器也可挂载服务器根或模块 URL。WebDAV 上传的文件可以通过 rsync 恢复。

restic 本身可经 rclone 的 WebDAV remote 使用此存储；历史 NAS 联调记录中的定制 restic 客户端不代表所有备份客户端均兼容。CrySync 看到的是这些工具写入的仓库文件，备份版本由上层工具维护。

## Docker 部署

仓库提供 [Compose 示例](../docker-compose.example.yml) 和[配置示例](../conf/crysync.yaml.example)：

```bash
cp docker-compose.example.yml docker-compose.yml
cp conf/crysync.yaml.example conf/crysync.yaml
```

启动前编辑两个文件：

1. 替换前端认证示例密码；按需启用 rsync、WebDAV 及端口映射。
2. 将 `/srv/crysync/data` 替换为实际数据目录；全部模块采用 WebDAV 后端时移除此挂载。
3. 确保 `/keys`、`/meta`、`/data` 的挂载目录属主一致，或通过 `PUID`/`PGID` 指定有权限的运行身份。新命名卷通常继承镜像中的 `1000:1000`；宿主机自动创建的目录可能属于 root，两者混用会导致入口检查失败。
4. 配置文件设为 `0600`，属主需允许容器运行身份读取。入口脚本不会自动修改属主或权限。

```bash
chmod 600 conf/crysync.yaml
docker compose up -d
docker compose logs -f crysync
```

示例使用 `ghcr.io/cnstark/crysync:latest`；实际部署可改为所选发布版本的标签。Docker 默认日志写 stderr，由容器日志系统接收。需要文件日志时，另配 `log.file` 和对应可写持久卷。

入口身份选择为 `PUID`（`PGID` 缺省同值）→ 挂载目录属主 → `1000:1000`。若选择的 UID 为 0，会警告后以 root 运行。显式设置 Compose 的 `user:` 为非 root 时，需自行保证全部路径可读写。

Compose 健康检查只检测任一配置示例端口是否监听，不能证明所有模块或后端可用；修改容器内监听端口时同步修改检查命令。

## 命令参考

| 命令 | 行为 |
|---|---|
| `crysyncd --config <path>` | 加载配置并启动所配置前端；缺省路径 `/etc/crysync/crysync.yaml` |
| `crysyncd --version` | 打印构建版本，本地构建缺省 `dev` |
| `crysync init --config <path>` | 初始化或恢复全部模块；会探测后端，只有 key 时从远端 Meta 恢复 |
| `crysync version` | 打印构建版本；也接受 `--version`、`-v` |

CLI 没有 `--module`、`snapshots`、`prune` 或 `gc`。容器中可执行 `docker compose run --rm crysync init --config /conf/crysync.yaml`；初始化是可选步骤。

## 运行与排障

| 现象 | 当前行为与排查方向 |
|---|---|
| WebDAV 模块 503 | 模块或后端尚未就绪；检查 `module_open_error`、`module_init_retry`、后端 URL 和权限，等待冷却后重试请求 |
| rsync 模块不可用 | 初始化失败由后台调度器退避重试；检查 `module_init_retry`/`module_recovered`，连接选择模块时也会重新打开并探测后端 |
| 数据库存在但密钥缺失 | 拒绝自动换钥；恢复对应密钥及挂载，避免丢失现有数据的解密能力 |
| PUT 返回 409 | 检查父目录是否存在，以及后端目标目录是否有效 |
| 只读模块写入失败 | rsync 拒绝推送，WebDAV 写方法返回 403 |
| 删除文件后后端空间未释放 | 清单引用已移除，但没有自动 GC；不要按文件名猜测并手工删除 blob |
| 上传慢或后端限流 | 通过 `max_upload_concurrency` 限制模块上传并发；`chunk_size` 会影响请求数和内存使用 |

WebDAV PUT 按块流式处理，收到块后立即查重、加密并上传；进程级 `upload.max_inflight_chunks` 默认 8，槽位耗尽时暂停读取客户端。默认 4 MiB 分块、8 个槽位约占 64 MiB 受控块内存，仍需为 Go runtime、网络和元数据库预留空间。写入成功后才更新清单；上传报错或断流不会把部分请求视为成功。反向代理可能按自身配置缓存请求体。

Dir 与 WebDAV 后端都按 `backend.bucket_depth` 创建 `ab/cd/<blobName>` 形式的多级分桶，默认深度为 2，可设置为 1～4。目录片段来自逻辑 blob 名的 SHA-256；旧平铺布局不读取或迁移，升级时请在新布局仓库重新备份。

服务收到 SIGINT/SIGTERM 后开始退出，但主进程不保证等待所有正在进行的请求或 rsync 会话完成。维护和备份前先停止客户端写入并等待传输结束。

daemon 默认每小时把 SQLite 一致快照加密到数据后端的 `meta/` 目录，并保留 24 个版本。只有 key 而本地 Meta 缺失时，启动会从最新版本向前尝试，校验数据库和全部引用 blob 后再开放模块；没有有效备份时拒绝创建空仓库。全量数据校验需要下载所有引用 blob，大仓库的首次灾难恢复可能耗时较长。

key 不会随 Meta 上传，必须在独立位置备份。定时备份的周期就是最大恢复点间隔；后端与本地 Meta 位于同一磁盘时不能抵御整盘损坏。手工复制运行中的 SQLite `.db` 仍不安全，因为数据库使用 WAL；需要离线复制时先停止服务。每个模块使用独立后端目录，避免混入其他应用文件。

## 协议兼容边界

rsync 支持 `-l` 符号链接、`-p` 权限、`-o/-g` uid/gid、`--checksum`、`--timeout=N`、非递归列表及子路径恢复。恢复不存在路径返回客户端错误，不能据此建立空仓库。设备/特殊文件跳过落库并记录警告。

压缩（`-z/--compress*`）、追加（`--append/--append-verify`）及恢复方向 `--atimes` 被明确拒绝。xattr、硬链接、稀疏文件传输尚未实现。

WebDAV 根目录只读；LOCK/UNLOCK 接受请求但不维护锁状态。符号链接在 WebDAV 中作为目标文本读取，不跟随链接。已有目录 MKCOL 和不存在路径 DELETE 做幂等处理。COPY 通过读写文件完成并利用分块去重；MOVE/DELETE 目录操作可能在失败后留下部分更新。
