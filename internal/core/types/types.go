// internal/core/types/types.go
// types 包是三层的叶子节点：只定义面向前端的窄接口，不依赖任何实现。
// 依赖方向（单向无环）：types ← repo ← core ← front/rsync。
package types

import (
	"io"
	"time"

	"crysync/internal/core/meta"
)

// SessionTxn 备份会话事务：rsync 备份方向（receiver）使用。
// 方法集合 = receiver.go 全部落库调用面（UpsertFile/DeleteFile/ApplyDelete/Commit/Rollback
// 加 SnapshotID 取快照 ID），与 repo.SnapshotTxn 一一对应。
type SessionTxn interface {
	// SnapshotID 返回本次事务创建的快照 ID（BeginSnapshot 复制清单后即可用）。
	SnapshotID() int64
	// UpsertFile 落库文件行并附加块关联（refcount 递增由实现负责）。
	UpsertFile(f meta.FileRow, chunks []meta.ChunkRef) error
	// DeleteFile 删除事务快照中指定路径的行。
	DeleteFile(path string) error
	// ApplyDelete 执行 rsync --delete 语义：删除 prefix 子树内、不在 covered 中的行。
	ApplyDelete(prefix string, covered map[string]bool) (int, error)
	// Commit 提交快照，返回快照 ID。
	Commit() (int64, error)
	// Rollback 回滚快照（删除预建的快照行）。
	Rollback() error
}

// Session 备份方向服务：rsync receiver 使用。
// 方法集合 = rsyncproto 当前实际调用的 repo 方法全集（Task 2 Step 6 逐一核对），
// 不增不减。含 KeepOnlySnapshot（receiver 会话收尾单份模式裁剪用）。
type Session interface {
	// BeginSnapshot 创建新快照并复制上一快照的完整文件清单，返回会话事务。
	BeginSnapshot(now time.Time) (SessionTxn, error)
	// StoreChunk 将明文块去重存储（哈希命中复用），返回 chunk ID 与是否复用。
	StoreChunk(data []byte) (chunkID int64, reused bool, err error)
	// ChunkSizeBytes 返回分块大小（字节），receiver 用它确定内容缓冲/分块边界。
	ChunkSizeBytes() int
	// GetFileRow 按快照路径查文件元数据（delta 的 quick check 基准）。
	GetFileRow(snapshotID int64, path string) (meta.FileRow, bool, error)
	// ReadChunkAt 读取快照文件第 idx 个存储块的明文（basisReader 逐块顺序读取）。
	ReadChunkAt(snapshotID int64, path string, idx int) ([]byte, error)
	// ActiveSnapshotID 返回活跃快照 ID（会话开始取一次，之后不随 CLI 切换）。
	ActiveSnapshotID() (int64, error)
	// StreamFile 流式读取快照文件内容（整文件 MD5 强校验和），返回字节数与校验和。
	StreamFile(snapshotID int64, path string, seed int32, w io.Writer) (int64, [16]byte, error)
	// KeepOnlySnapshot 单份模式会话收尾：删除 keepID 之前的全部快照。
	KeepOnlySnapshot(snapshotID int64) (removed int, blobs int, err error)
}

// FileStore 读路径服务：rsync 恢复方向（sender）+ 后续 WebDAV 读使用。
// 方法集合 = sender.go processSendSession 的 repo 调用面（Step 6 核对），
// 不增不减；不含任何写方法。
type FileStore interface {
	// ActiveSnapshotID 返回活跃快照 ID（恢复以 CLI 切换的活跃快照为时间点）。
	ActiveSnapshotID() (int64, error)
	// SnapshotFileRows 返回快照文件清单，按子路径 prefix 过滤（sender 构造 flist）。
	SnapshotFileRows(snapshotID int64, prefix string) ([]meta.FileRow, error)
	// GetFileRow 按快照路径查文件元数据（恢复方向错误形态判定用）。
	GetFileRow(snapshotID int64, path string) (meta.FileRow, bool, error)
	// StreamFile 流式读取快照文件内容（整文件 MD5 强校验和），返回字节数与校验和。
	StreamFile(snapshotID int64, path string, seed int32, w io.Writer) (int64, [16]byte, error)
	// ChunkSizeBytes 返回分块大小（字节），sender 用它计算块数与日志统计。
	ChunkSizeBytes() int
}

// RsyncService rsync 前端会话所需完整能力（Session ∪ FileStore）：
// 方向在 argv 协商后才确定，入口（RunSession/RunSender）需要两方向全部方法，
// 会话内部再按方向收窄为 Session（备份）或 FileStore（恢复）。
type RsyncService interface {
	Session
	FileStore
}