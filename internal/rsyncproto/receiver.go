// internal/rsyncproto/receiver.go
package rsyncproto

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"crysync/internal/config"
	"crysync/internal/meta"
	"crysync/internal/repo"
)

// RunReceiver 处理一次备份方向会话（客户端推）：argv/二进制协商 -> mux 会话。
// 调用方需先完成：HandleModuleRequest（greeting/模块选择/认证）。
func RunReceiver(ctx context.Context, br *bufio.Reader, w io.Writer, module *config.ModuleConfig, r *repo.Repo) error {
	neg, err := NegotiateBinary(br, w)
	if err != nil {
		return fmt.Errorf("协商: %w", err)
	}
	mr, err := NewMuxReader(br)
	if err != nil {
		return err
	}
	mw := NewMuxWriter(w)
	return processSession(ctx, mr, mw, module, r, neg)
}

// RunReceiverWithReader：与已进行握手/认证的 bufio.Reader 继续协议（避免预读丢失）。
// handleConn 已用同一个 br 读过文本行阶段，br 的 buffer 可能预读后续协议字节，
// 必须原样传给 RunReceiver，否则预读字节丢失（RunReceiver 内部完成 NegotiateBinary + mux 会话）。
func RunReceiverWithReader(ctx context.Context, br *bufio.Reader, conn net.Conn, module *config.ModuleConfig, r *repo.Repo) error {
	return RunReceiver(ctx, br, conn, module, r)
}

// ITEM_* 标志（rsync.h:205-235，iflags 以 shortint 小端 2 字节发送）
const (
	itemTransfer = uint16(1 << 15) // ITEM_TRANSFER：需内容传输
	itemIsNew    = uint16(1 << 13) // ITEM_IS_NEW：新条目
)

// xferSumLen 整文件强校验和长度：compat_flags=0 时客户端 fallback md5（compat.c:552），
// xfer_sum_len = MD5_SUM_LENGTH = 16。
const xferSumLen = 16

// processSession 完整 mux 会话：
// [条件]filter -> flist（逐条到哨兵）-> [条件]id list -> 传输阶段（generator 角色
// 逐条 ndx/iflags/sums 驱动 + receiver 角色读回显与 token 流）-> goodbye -> 提交快照。
func processSession(ctx context.Context, in *MuxReader, out *MuxWriter, module *config.ModuleConfig, r *repo.Repo, neg *Negotiation) error {
	stream := NewMuxStream(in)
	// ndx 差分编码的读/写方向各自独立维护 prev 状态（io.c write_ndx/read_ndx 分方向）
	ndxOut := newNdxCodec() // S->C：条目 ndx 与 NDX_DONE
	ndxIn := newNdxCodec()  // C->S：客户端回显与 ACK

	// filter 列表：仅 delete/prune 时出现在网络上（exclude.c:1643-1678--
	// sender 且 receiver_wants_list=0 时客户端 f_out=-1 连 int32 0 都不发）
	if neg.DeleteMode || neg.PruneEmptyDirs {
		if err := recvFilterList(stream); err != nil {
			return fmt.Errorf("filter: %w", err)
		}
	}

	// flist：无 count 头，逐条读到单字节 0 哨兵（write_end_of_flist 旧路径无错变体）
	parser := NewFlistParser()
	var entries []FileEntry
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		e, err := parser.Parse(stream)
		if err == ErrFlistEnd {
			break
		}
		if err != nil {
			return fmt.Errorf("解析 flist: %w", err)
		}
		entries = append(entries, e)
	}
	// 按 f_name_cmp 排序（flist.c:3217）：generator 遍历 sorted 数组发 ndx
	// （generator.c:2316-2322），传输阶段索引必须与真实 rsync 的 sorted 顺序一致
	entries = SortFlistEntries(entries)

	// id list：非增量模式下紧跟 flist 哨兵（uidlist.c:460-479；按 preserve 条件）
	if err := ReadIdList(stream, neg.PreserveUID, neg.PreserveGID); err != nil {
		return fmt.Errorf("id list: %w", err)
	}

	txn, err := r.BeginSnapshot(time.Now())
	if err != nil {
		return err
	}
	rollback := func() { txn.Rollback() }

	// 传输阶段：对 flist 中每个条目发 ndx+iflags（目录/链接无 sums），
	// 客户端 sender 对每条回显 ndx+iflags（文件另有 sum_head 回显与 token 流）。
	for i, e := range entries {
		if err := ctx.Err(); err != nil {
			rollback()
			return err
		}
		if e.Path == "" {
			// 顶层目录 "."（cleanPath 拒绝）：不落库，但仍需走 ndx 协议交互
			if !e.IsDir {
				rollback()
				return fmt.Errorf("flist 条目 %d: 非法路径", i)
			}
			if err := driveEntry(out, stream, ndxOut, ndxIn, i, itemIsNew); err != nil {
				rollback()
				return fmt.Errorf("条目 %d: %w", i, err)
			}
			continue
		}
		if e.IsDir || e.IsSymlink {
			if err := driveEntry(out, stream, ndxOut, ndxIn, i, itemIsNew); err != nil {
				rollback()
				return fmt.Errorf("条目 %d: %w", i, err)
			}
			if err := applyStaticEntry(txn, e); err != nil {
				rollback()
				return fmt.Errorf("落库 %s: %w", e.Path, err)
			}
			continue
		}
		// 普通文件：ITEM_TRANSFER | ITEM_IS_NEW + 空校验和请求（write_sum_head(NULL) 16 字节全 0）
		refs, err := receiveFile(ctx, stream, out, ndxOut, ndxIn, r, i)
		if err != nil {
			rollback()
			return fmt.Errorf("文件 %s: %w", e.Path, err)
		}
		if err := txn.UpsertFile(meta.FileRow{
			Path: e.Path, Mode: e.Mode, UID: e.UID, GID: e.GID,
			Size: e.Size, MTimeNs: e.MTimeNs,
		}, refs); err != nil {
			rollback()
			return fmt.Errorf("落库 %s: %w", e.Path, err)
		}
	}

	// goodbye（generator.c:2336/2368-2376 + main.c:1119，无 delete/delay 的典型序列）：
	// DONE#1 -> ACK；DONE#2 -> ACK；DONE#3（客户端 break，无 ACK）；DONE#4 -> ACK；DONE#5。
	// 注意 #3 后客户端不回 ACK，等 ACK 会死锁。
	for _, step := range []struct {
		writeDone int // 连续发出的 DONE 数
		readAck   int // 随后读取的 ACK 数
	}{
		{1, 1}, {1, 1}, {2, 1}, {1, 0},
	} {
		for k := 0; k < step.writeDone; k++ {
			if err := writeNdxDone(out, ndxOut); err != nil {
				rollback()
				return err
			}
		}
		for k := 0; k < step.readAck; k++ {
			if err := readNdxDone(stream, ndxIn); err != nil {
				rollback()
				return err
			}
		}
	}

	// 协议全部成功，才提交快照
	if _, err := txn.Commit(); err != nil {
		rollback()
		return err
	}
	return nil
}

// driveEntry 发送目录/链接条目的 ndx+iflags 并读客户端回显（sender.c:285-289
// 非 ITEM_TRANSFER 分支：回显 ndx+iflags 后不读 sums）。
func driveEntry(out *MuxWriter, stream *MuxStream, ndxOut, ndxIn *ndxCodec, index int, iflags uint16) error {
	if err := sendNdxIflags(out, ndxOut, index, iflags); err != nil {
		return err
	}
	return recvNdxEcho(stream, ndxIn)
}

// recvNdxEcho 读客户端回显的 ndx+iflags（write_ndx_and_attrs，无 BASIS/XNAME 跟随位）。
func recvNdxEcho(stream *MuxStream, ndx *ndxCodec) error {
	if _, err := ndx.Read(stream); err != nil {
		return fmt.Errorf("回显 ndx: %w", err)
	}
	if _, err := ReadShortint(stream); err != nil {
		return fmt.Errorf("回显 iflags: %w", err)
	}
	return nil
}

// recvFilterList 读客户端 filter 列表（exclude.c:1667-1690）：
// while ((len = read_int32()) != 0) { 读 len 字节规则 }。
func recvFilterList(stream *MuxStream) error {
	for {
		n, err := ReadInt32(stream)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		if n < 0 || n > 1<<20 {
			return fmt.Errorf("非法 filter 规则长度: %d", n)
		}
		if _, err := io.CopyN(io.Discard, stream, int64(n)); err != nil {
			return err
		}
	}
}

// sendNdxIflags 发送 ndx（差分编码）+ iflags（shortint）。
func sendNdxIflags(out *MuxWriter, ndx *ndxCodec, index int, iflags uint16) error {
	var buf bytesBuffer
	if err := ndx.Write(&buf, int32(index)); err != nil {
		return err
	}
	if err := WriteShortint(&buf, iflags); err != nil {
		return err
	}
	return out.WriteData(buf.Bytes())
}

// writeNdxDone 发送一个 NDX_DONE（write_ndx 协议 30+ 编码 = 单字节 0x00）。
func writeNdxDone(out *MuxWriter, ndx *ndxCodec) error {
	var buf bytesBuffer
	if err := ndx.Write(&buf, ndxDone); err != nil {
		return err
	}
	return out.WriteData(buf.Bytes())
}

// readNdxDone 读取一个 NDX_DONE（单字节 0x00）。
func readNdxDone(stream *MuxStream, ndx *ndxCodec) error {
	v, err := ndx.Read(stream)
	if err != nil {
		return err
	}
	if v != ndxDone {
		return fmt.Errorf("goodbye 期待 NDX_DONE，收到 %d", v)
	}
	return nil
}

// bytesBuffer 内存缓冲。
type bytesBuffer struct{ b []byte }

func (w *bytesBuffer) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

func (w *bytesBuffer) Bytes() []byte { return w.b }

// applyStaticEntry：目录/符号链接条目（无内容传输）。
func applyStaticEntry(txn *repo.SnapshotTxn, e FileEntry) error {
	if e.IsSymlink {
		return txn.UpsertFile(meta.FileRow{
			Path: e.Path, IsSymlink: true, Mode: e.Mode,
			UID: e.UID, GID: e.GID, MTimeNs: e.MTimeNs, LinkTarget: e.LinkTarget,
		}, nil)
	}
	if e.IsDir {
		// 目录路径统一去尾斜杠后落库
		return txn.UpsertFile(meta.FileRow{
			Path: strings.TrimSuffix(e.Path, "/"), IsDir: true, Mode: e.Mode,
			UID: e.UID, GID: e.GID, MTimeNs: e.MTimeNs,
		}, nil)
	}
	return nil
}

// receiveFile 单个普通文件的完整传输（generator 请求 + receiver 收数据）：
// 1. 发 ndx + iflags(ITEM_TRANSFER|ITEM_IS_NEW) + write_sum_head(NULL)（16 字节全 0）
// 2. 读客户端回显 ndx+iflags 与回显 sum_head（sender.c:409/410）
// 3. token 流：正 int=literal（读 N 字节）；负=match（v1 无 basis，报错）；0=结束（token.c:304-319）
// 4. 读 xferSumLen 字节整文件强校验和（md5，丢弃）
func receiveFile(ctx context.Context, stream *MuxStream, out *MuxWriter, ndxOut, ndxIn *ndxCodec, r *repo.Repo, index int) ([]meta.ChunkRef, error) {
	// ndx + iflags + 空校验和请求（count/blength/s2length/remainder 各 int32 0 = 16 字节）
	var buf bytesBuffer
	if err := ndxOut.Write(&buf, int32(index)); err != nil {
		return nil, err
	}
	if err := WriteShortint(&buf, itemTransfer|itemIsNew); err != nil {
		return nil, err
	}
	if err := writeNullSumHead(&buf); err != nil {
		return nil, err
	}
	if err := out.WriteData(buf.Bytes()); err != nil {
		return nil, err
	}

	// 客户端回显：ndx + iflags，然后回显 sum_head（16 字节）
	if err := recvNdxEcho(stream, ndxIn); err != nil {
		return nil, err
	}
	var echo [4]int32
	for i := range echo {
		v, err := ReadInt32(stream)
		if err != nil {
			return nil, fmt.Errorf("回显 sum_head: %w", err)
		}
		echo[i] = v
	}

	chunkSize := r.ChunkSizeBytes()
	data := make([]byte, 0, chunkSize)
	var refs []meta.ChunkRef
	var idx int
	flush := func() error {
		if len(data) == 0 {
			return nil
		}
		id, _, err := r.StoreChunk(data)
		if err != nil {
			return err
		}
		refs = append(refs, meta.ChunkRef{ChunkID: id, IDX: idx})
		idx++
		data = data[:0]
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		token, err := ReadInt32(stream)
		if err != nil {
			return nil, err
		}
		if token == 0 {
			break // 文件数据结束
		}
		if token < 0 {
			// match token：v1 不请求块校验和（sums 为空），客户端不应发出
			return nil, fmt.Errorf("不应收到匹配块 token: %d", token)
		}
		n := int(token)
		if n > 32<<20 {
			return nil, fmt.Errorf("字面量块过大: %d", n)
		}
		remaining := n
		for remaining > 0 {
			take := min(remaining, chunkSize-len(data))
			tmp := data[len(data) : len(data)+take]
			if _, err := io.ReadFull(stream, tmp); err != nil {
				return nil, err
			}
			data = data[:len(data)+take]
			remaining -= take
			if len(data) == chunkSize {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	// 整文件强校验和（v1 不校验，读掉）
	if _, err := io.ReadFull(stream, make([]byte, xferSumLen)); err != nil {
		return nil, err
	}
	return refs, nil
}

// writeNullSumHead 写 write_sum_head(NULL)：count/blength/s2length/remainder 各 int32 0，
// 共 16 字节（io.c:1996-2008，null_sum 静态全零）。
func writeNullSumHead(w io.Writer) error {
	for i := 0; i < 4; i++ {
		if err := WriteInt32(w, 0); err != nil {
			return err
		}
	}
	return nil
}
