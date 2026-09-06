// internal/core/types/types.go
// types 包是三层的叶子节点：只定义面向前端的窄接口，不依赖任何实现。
// 依赖方向（单向无环）：types ← repo ← core ← front/rsync。
package types

import (
	"context"
	"io"

	"crysync/internal/core/meta"
)

// Session 备份方向服务：rsync receiver 使用。
// v0.5 单一当前状态模型：无快照事务，会话持写互斥锁（WriteSessionLock）
// 后直接原地落库；失败 = 半更新镜像（客户端重试幂等收敛）。
type Session interface {
	// WriteSessionLock 获取模块写互斥（会话全程持有，结束释放）：
	// 同模块并发写会话与 WebDAV 写在此排队。
	WriteSessionLock() func()
	// StoreChunk 将明文块去重存储（哈希命中复用），返回 chunk ID 与是否复用。
	StoreChunk(data []byte) (chunkID int64, reused bool, err error)
	// ChunkSizeBytes 返回分块大小（字节），receiver 用它确定内容缓冲/分块边界。
	ChunkSizeBytes() int
	// UpsertFile 原地落库文件行并附加块关联（refcount 递增）。
	UpsertFile(f meta.FileRow, chunks []meta.ChunkRef) error
	// DeleteFile 原地删除路径行（--delete 语义）。
	DeleteFile(path string) error
	// FileRows 返回清单（--delete 枚举与 quick check 用），prefix 子树过滤。
	FileRows(prefix string) ([]meta.FileRow, error)
	// GetFileRow 按路径查文件元数据（delta 的 quick check 基准）。
	GetFileRow(path string) (meta.FileRow, bool, error)
	// ReadChunkAt 读取文件第 idx 个存储块的明文（basisReader 逐块顺序读取）。
	ReadChunkAt(path string, idx int) ([]byte, error)
	// StreamFile 流式读取文件内容（整文件 MD5 强校验和），返回字节数与校验和。
	StreamFile(path string, seed int32, w io.Writer) (int64, [16]byte, error)
}

// FileStore 读路径服务：rsync 恢复方向（sender）+ WebDAV 读使用。
type FileStore interface {
	// FileRows 返回文件清单，按子路径 prefix 过滤（sender 构造 flist）。
	FileRows(prefix string) ([]meta.FileRow, error)
	// GetFileRow 按路径查文件元数据（恢复方向错误形态判定用）。
	GetFileRow(path string) (meta.FileRow, bool, error)
	// StreamFile 流式读取文件内容（整文件 MD5 强校验和），返回字节数与校验和。
	StreamFile(path string, seed int32, w io.Writer) (int64, [16]byte, error)
	// ChunkSizeBytes 返回分块大小（字节），sender 用它计算块数与日志统计。
	ChunkSizeBytes() int
	// ListDir 返回 path 的直接子项（非递归，不含自身）。
	ListDir(path string) ([]meta.FileRow, error)
	// OpenFile 按路径打开文件：返回流式块重组 reader（Read/Seek/Close）与元数据。
	OpenFile(path string) (io.ReadSeekCloser, meta.FileRow, error)
}

// FileWriter 写路径服务：WebDAV 写使用（v0.5：原地更新，无快照语义）。
type FileWriter interface {
	// PutFile 写入（或覆盖）path 文件内容（原地 upsert 当前状态）。
	PutFile(path string, mode uint32, mtimeNs int64, src io.Reader) error
	// Mkcol 创建目录条目；已存在时幂等返回 nil。
	Mkcol(path string) error
	// DeletePath 删除 path（文件或目录，目录递归删整棵子树）；不存在幂等。
	DeletePath(path string) error
	// MovePath 移动/改名 src 至 dst（目录移动递归整棵子树）。
	MovePath(src, dst string) error
}

// PutResult 是一次成功流式上传的结果。
type PutResult struct {
	Size    int64
	MTimeNs int64
}

// ContextFileWriter 是支持请求取消和结果返回的写入扩展接口。
type ContextFileWriter interface {
	PutFileContext(context.Context, string, uint32, int64, io.Reader) (PutResult, error)
}

// ContextSession 是支持请求取消的 rsync 写入扩展接口。
type ContextSession interface {
	StoreChunkContext(context.Context, []byte) (chunkID int64, reused bool, err error)
}

// RsyncService rsync 前端会话所需完整能力（Session ∪ FileStore）：
// 方向在 argv 协商后才确定，入口（RunSession/RunSender）需要两方向全部方法，
// 会话内部再按方向收窄为 Session（备份）或 FileStore（恢复）。
type RsyncService interface {
	Session
	FileStore
}
