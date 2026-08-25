// internal/rsyncproto/receiver.go
package rsyncproto

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"crysync/internal/config"
	"crysync/internal/meta"
	"crysync/internal/repo"
)

// nopLogger 丢弃全部输出（调用方未提供 logger 时兜底）。
var nopLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// logNegotiated 记录协商结果（debug 级，协议排查用）。
func logNegotiated(logger *slog.Logger, neg *Negotiation) {
	logger.Debug("negotiated",
		"protocol", 31,
		"compat_flags", 0,
		"checksum_seed", neg.ChecksumSeed,
		"sender_mode", neg.SenderMode,
		"preserve_uid", neg.PreserveUID,
		"preserve_gid", neg.PreserveGID,
		"preserve_devices", neg.PreserveDevices,
		"delete_mode", neg.DeleteMode,
		"numeric_ids", neg.NumericIDs,
	)
}

// RunReceiver 处理一次备份方向会话（客户端推）：argv/二进制协商 -> mux 会话。
// 调用方需先完成：HandleModuleRequest（greeting/模块选择/认证）。
func RunReceiver(ctx context.Context, br *bufio.Reader, w io.Writer, module *config.ModuleConfig, r *repo.Repo, logger *slog.Logger) error {
	if logger == nil {
		logger = nopLogger
	}
	neg, err := NegotiateBinary(br, w)
	if err != nil {
		return fmt.Errorf("协商: %w", err)
	}
	logNegotiated(logger, neg)
	mr, err := NewMuxReader(br)
	if err != nil {
		return err
	}
	mw := NewMuxWriter(w)
	if err := rejectCompression(mw, neg); err != nil {
		return err
	}
	return processSession(ctx, mr, mw, module, r, neg, logger)
}

// RunReceiverWithReader：与已进行握手/认证的 bufio.Reader 继续协议（避免预读丢失）。
// handleConn 已用同一个 br 读过文本行阶段，br 的 buffer 可能预读后续协议字节，
// 必须原样传给 RunReceiver，否则预读字节丢失（RunReceiver 内部完成 NegotiateBinary + mux 会话）。
func RunReceiverWithReader(ctx context.Context, br *bufio.Reader, conn net.Conn, module *config.ModuleConfig, r *repo.Repo, logger *slog.Logger) error {
	return RunReceiver(ctx, br, conn, module, r, logger)
}

// ITEM_* 标志（rsync.h:205-235，iflags 以 shortint 小端 2 字节发送）
const (
	itemTransfer = uint16(1 << 15) // ITEM_TRANSFER：需内容传输
	itemIsNew    = uint16(1 << 13) // ITEM_IS_NEW：新条目
)

// xferSumLen 整文件强校验和长度：compat_flags=0 时客户端 fallback md5（compat.c:552），
// xfer_sum_len = MD5_SUM_LENGTH = 16。
const xferSumLen = 16

// fileStats 单文件处理统计（file 日志行与会话汇总用）。
type fileStats struct {
	method  string // quick_check / full / delta
	matched int64  // delta 命中旧文件字节数
	literal int64  // 客户端直传字节数
	chunks  int    // 文件落库块数（refs）
	stored  int    // 其中新写后端的块数（去重后）
	blength int32  // delta 块结构参数
	elapsed time.Duration
}

// processSession 完整 mux 会话：
// [条件]filter -> flist（逐条到哨兵）-> [条件]id list -> 传输阶段（generator 角色
// 逐条 ndx/iflags/sums 驱动 + receiver 角色读回显与 token 流）-> goodbye -> 提交快照。
func processSession(ctx context.Context, in *MuxReader, out *MuxWriter, module *config.ModuleConfig, r *repo.Repo, neg *Negotiation, logger *slog.Logger) (err error) {
	if logger == nil {
		logger = nopLogger
	}
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
		e, err := parser.Parse(stream, neg.PreserveUID, neg.PreserveGID, neg.PreserveLinks, neg.PreserveDevices, neg.ChecksumMode)
		if err == ErrFlistEnd {
			break
		}
		if err != nil {
			return fmt.Errorf("解析 flist: %w", err)
		}
		entries = append(entries, e)
	}
	// IO_ERROR_ENDLIST 哨兵携带发送侧 io_error 位（源消失/vanished 文件等）：
	// 会话继续处理已收条目，但错误必须可见（真实 rsync 客户端此时退出码 23）；
	// P1#7 的 --delete 实现须在 io_error≠0 时禁用删除（flist.c:1402 语义）
	if parser.IoError != 0 {
		logger.Warn("flist_io_error", "io_error", parser.IoError,
			"hint", "发送侧 flist 构造时出错（源消失等），已收条目仍处理")
	}
	// 按 f_name_cmp 排序（flist.c:3217）：generator 遍历 sorted 数组发 ndx
	// （generator.c:2316-2322），传输阶段索引必须与真实 rsync 的 sorted 顺序一致
	entries = SortFlistEntries(entries)

	// 子路径前缀（与 sender 方向 modulePrefix 对称）：flist 条目名相对传输根，
	// 落库路径与 quick check 基准查询必须加模块子路径前缀，否则子路径备份
	// 落库到模块根（多客户端子路径互相覆盖、子路径恢复必然空）。
	prefix := modulePrefix(neg.ModuleArg, module.Name)
	full := func(p string) string {
		switch {
		case prefix == "":
			return p
		case p == "":
			return prefix
		default:
			return prefix + "/" + p
		}
	}

	logger.Info("session_start", "dir", "backup", "prefix", prefix,
		"argv", strings.Join(neg.Argv, " "), "entries", len(entries))

	// id list：非增量模式下紧跟 flist 哨兵（uidlist.c:460-479；按 preserve 条件）
	if err := ReadIdList(stream, neg.PreserveUID, neg.PreserveGID); err != nil {
		return fmt.Errorf("id list: %w", err)
	}

	// 会话统计与错误上下文：任何 return 前的错误都记 session_error（含当前条目）
	started := time.Now()
	var st struct {
		files      int   // flist 条目总数
		transferred int  // 实际传输文件（full+delta）
		skipped    int   // quick check 命中
		dirs       int
		links      int
		matched    int64
		literal    int64
		chunksStored int
	}
	st.files = len(entries)
	var curPath string
	defer func() {
		if err != nil {
			logger.Error("session_error", "path", curPath, "err", err.Error())
		}
	}()

	txn, err := r.BeginSnapshot(time.Now())
	if err != nil {
		return err
	}
	rollback := func() { txn.Rollback() }

	// 活跃快照（delta 的 quick check 基准）：会话开始取一次，之后不随 CLI 切换
	activeID, err := r.ActiveSnapshotID()
	if err != nil {
		rollback()
		return err
	}

	// 传输阶段：对 flist 中每个条目发 ndx+iflags（目录/链接无 sums），
	// 客户端 sender 对每条回显 ndx+iflags（文件另有 sum_head 回显与 token 流）。
	for i, e := range entries {
		if err := ctx.Err(); err != nil {
			rollback()
			return err
		}
		curPath = e.Path
		if e.Path == "" {
			// 顶层目录 "."（cleanPath 拒绝）：模块根时不落库，但仍需走 ndx 协议交互；
			// 子路径备份时 "." 是传输根目录（prefix）自身的属性，落为 prefix 目录行，
			// 恢复方向以子路径拉取时化身 "."（sender 侧 f.Path==prefix 分支）
			if !e.IsDir {
				rollback()
				return fmt.Errorf("flist 条目 %d: 非法路径", i)
			}
			if err := driveEntry(out, stream, ndxOut, ndxIn, i, itemIsNew); err != nil {
				rollback()
				return fmt.Errorf("条目 %d: %w", i, err)
			}
			if prefix != "" {
				if err := applyStaticEntry(txn, e, prefix); err != nil {
					rollback()
					return fmt.Errorf("落库 %s: %w", prefix, err)
				}
				st.dirs++
				logger.Info("entry", "path", prefix, "type", "dir",
					"mode", fmt.Sprintf("%o", e.Mode), "uid", e.UID, "gid", e.GID,
					"mtime", e.MTimeNs / 1e9)
			}
			continue
		}
		// 设备/特殊文件（CHR/BLK/FIFO/SOCK）：协议交互照常（无内容传输，回显
		// ndx+iflags，与目录/链接同路径），但 v1 快照模型无设备语义——跳过落库，
		// 警告日志 + MSG_INFO 客户端提示，会话继续（备份 /var 类含 FIFO 的目录
		// 是真实场景，此前整会话失败不可接受）
		if mt := e.Mode & sIfmt; mt == sIfChr || mt == sIfBlk || mt == sIfFifo || mt == sIfSock {
			logger.Warn("skip_special_file", "path", full(e.Path),
				"mode", fmt.Sprintf("%o", e.Mode),
				"hint", "v1 暂不支持设备/特殊文件，未备份")
			_ = out.WriteInfoMsg(fmt.Sprintf(
				"skipping non-regular file %q (device/special files are not supported by this server)\n", e.Path))
			if err := driveEntry(out, stream, ndxOut, ndxIn, i, itemIsNew); err != nil {
				rollback()
				return fmt.Errorf("条目 %d: %w", i, err)
			}
			continue
		}
		if e.IsDir || e.IsSymlink {
			// preserve off（无 -l）收到的 symlink 无 target 字段，无法恢复——
			// 跳过落库并警告（协议本身合法，客户端未发送该信息）
			if e.IsSymlink && e.LinkTarget == "" {
				logger.Warn("skip_link_no_target",
					"path", e.Path, "hint", "客户端未启用 -l（preserve links），symlink 未备份")
				if err := driveEntry(out, stream, ndxOut, ndxIn, i, itemIsNew); err != nil {
					rollback()
					return fmt.Errorf("条目 %d: %w", i, err)
				}
				continue
			}
			if err := driveEntry(out, stream, ndxOut, ndxIn, i, itemIsNew); err != nil {
				rollback()
				return fmt.Errorf("条目 %d: %w", i, err)
			}
			if err := applyStaticEntry(txn, e, full(e.Path)); err != nil {
				rollback()
				return fmt.Errorf("落库 %s: %w", full(e.Path), err)
			}
			attrs := []any{"path", full(e.Path), "mode", fmt.Sprintf("%o", e.Mode),
				"uid", e.UID, "gid", e.GID, "mtime", e.MTimeNs / 1e9}
			if e.IsSymlink {
				st.links++
				logger.Info("entry", append(attrs, "type", "link", "target", e.LinkTarget)...)
			} else {
				st.dirs++
				logger.Info("entry", append(attrs, "type", "dir")...)
			}
			continue
		}
		// 普通文件：quick check 命中不发请求（不落库，CopyFiles 已继承旧行与 chunk 关联）；
		// 变化文件发真实块校验和（delta）；新文件/空文件发空校验和请求（全量 literal）
		refs, updated, fs, err := receiveFileDelta(ctx, stream, out, ndxOut, ndxIn, r, neg, activeID, e, i, prefix)
		if err != nil {
			rollback()
			return fmt.Errorf("文件 %s: %w", e.Path, err)
		}
		switch fs.method {
		case "quick_check":
			st.skipped++
		default:
			st.transferred++
		}
		st.matched += fs.matched
		st.literal += fs.literal
		st.chunksStored += fs.stored
		fields := []any{"path", full(e.Path), "size", e.Size, "mtime", e.MTimeNs / 1e9,
			"mode", fmt.Sprintf("%o", e.Mode), "uid", e.UID, "gid", e.GID,
			"method", fs.method}
		switch fs.method {
		case "delta":
			fields = append(fields, "matched", fs.matched, "literal", fs.literal,
				"chunks", fs.chunks, "blength", fs.blength, "md5", "ok")
		case "full":
			fields = append(fields, "literal", fs.literal, "chunks", fs.chunks, "md5", "ok")
		}
		fields = append(fields, "elapsed_ms", fs.elapsed.Milliseconds())
		logger.Info("file", fields...)
		if updated { // quick check 命中不落库：CopyFiles 已继承旧行与 chunk 关联
			if err := txn.UpsertFile(meta.FileRow{
				Path: full(e.Path), Mode: e.Mode, UID: e.UID, GID: e.GID,
				Size: e.Size, MTimeNs: e.MTimeNs,
			}, refs); err != nil {
				rollback()
				return fmt.Errorf("落库 %s: %w", full(e.Path), err)
			}
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

	// 空 flist 会话（entries=0，如无 -a 的空目录推送——argv 形如
	// "--server -e.LsfxCIvu --stats . mod/"）：协议走完但不提交快照。BeginSnapshot
	// 已预建快照行（并复制上一快照清单），提交会使每次此类会话新增一个无变化
	// 快照，污染快照历史与 prune 保留计算。
	if len(entries) == 0 {
		rollback()
		logger.Info("session_done",
			"snapshot_id", int64(0),
			"files", 0,
			"empty_session", true,
			"elapsed_ms", time.Since(started).Milliseconds(),
		)
		return nil
	}

	// 协议全部成功，才提交快照
	sid, err := txn.Commit()
	if err != nil {
		rollback()
		return err
	}
	logger.Info("session_done",
		"snapshot_id", sid,
		"files", st.files,
		"transferred", st.transferred,
		"skipped", st.skipped,
		"dirs", st.dirs,
		"links", st.links,
		"bytes_matched", st.matched,
		"bytes_literal", st.literal,
		"chunks_stored", st.chunksStored,
		"elapsed_ms", time.Since(started).Milliseconds(),
	)
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

// applyStaticEntry：目录/符号链接条目（无内容传输）。path 为库内全路径
// （已含子路径前缀；调用方对子路径备份的顶层 "." 传 prefix 自身）。
func applyStaticEntry(txn *repo.SnapshotTxn, e FileEntry, path string) error {
	if e.IsSymlink {
		return txn.UpsertFile(meta.FileRow{
			Path: path, IsSymlink: true, Mode: e.Mode,
			UID: e.UID, GID: e.GID, MTimeNs: e.MTimeNs, LinkTarget: e.LinkTarget,
		}, nil)
	}
	if e.IsDir {
		// 目录路径统一去尾斜杠后落库
		return txn.UpsertFile(meta.FileRow{
			Path: strings.TrimSuffix(path, "/"), IsDir: true, Mode: e.Mode,
			UID: e.UID, GID: e.GID, MTimeNs: e.MTimeNs,
		}, nil)
	}
	return nil
}

// receiveFileLegacy 全量传输路径（v1）：发 ndx + iflags(ITEM_TRANSFER|ITEM_IS_NEW)
// + write_sum_head(NULL)（16 字节全 0）→ 客户端 count==0 全量 literal 发送。
// 用于新文件/空文件/旧行类型不一致（无可作 basis 的旧文件）场景。
func receiveFileLegacy(ctx context.Context, stream *MuxStream, out *MuxWriter, ndxOut, ndxIn *ndxCodec, r *repo.Repo, index int) ([]meta.ChunkRef, fileStats, error) {
	st := fileStats{method: "full"}
	// ndx + iflags + 空校验和请求（count/blength/s2length/remainder 各 int32 0 = 16 字节）
	var buf bytesBuffer
	if err := ndxOut.Write(&buf, int32(index)); err != nil {
		return nil, st, err
	}
	if err := WriteShortint(&buf, itemTransfer|itemIsNew); err != nil {
		return nil, st, err
	}
	if err := writeNullSumHead(&buf); err != nil {
		return nil, st, err
	}
	if err := out.WriteData(buf.Bytes()); err != nil {
		return nil, st, err
	}

	// 客户端回显：ndx + iflags，然后回显 sum_head（16 字节）
	if err := recvNdxEcho(stream, ndxIn); err != nil {
		return nil, st, err
	}
	var echo [4]int32
	for i := range echo {
		v, err := ReadInt32(stream)
		if err != nil {
			return nil, st, fmt.Errorf("回显 sum_head: %w", err)
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
		id, reused, err := r.StoreChunk(data)
		if err != nil {
			return err
		}
		if !reused {
			st.stored++
		}
		refs = append(refs, meta.ChunkRef{ChunkID: id, IDX: idx})
		idx++
		data = data[:0]
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, st, err
		}
		token, err := ReadInt32(stream)
		if err != nil {
			return nil, st, err
		}
		if token == 0 {
			break // 文件数据结束
		}
		if token < 0 {
			// match token：v1 不请求块校验和（sums 为空），客户端不应发出
			return nil, st, fmt.Errorf("不应收到匹配块 token: %d", token)
		}
		n := int(token)
		if n > 32<<20 {
			return nil, st, fmt.Errorf("字面量块过大: %d", n)
		}
		st.literal += int64(n)
		remaining := n
		for remaining > 0 {
			take := min(remaining, chunkSize-len(data))
			tmp := data[len(data) : len(data)+take]
			if _, err := io.ReadFull(stream, tmp); err != nil {
				return nil, st, err
			}
			data = data[:len(data)+take]
			remaining -= take
			if len(data) == chunkSize {
				if err := flush(); err != nil {
					return nil, st, err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return nil, st, err
	}
	// 整文件强校验和（v1 不校验，读掉）
	if _, err := io.ReadFull(stream, make([]byte, xferSumLen)); err != nil {
		return nil, st, err
	}
	st.chunks = len(refs)
	return refs, st, nil
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

// receiveFileDelta 单个普通文件的 delta 传输：
//  1. quick check：活跃快照存在同路径普通文件且 (mtime, size) 一致 → 不发出
//     任何字节（BeginSnapshot 的 CopyFiles 已继承旧行与 chunk 关联）。
//  2. 无旧行 / 空文件 / 旧行类型不一致 → 全量路径（receiveFileLegacy）。
//  3. 变化文件：第一遍读旧文件算块校验和表 → 发 ndx+iflags+sum_head+块校验和
//     → 读回显 → token 流（match 从旧文件流复制 + literal 直收）按 4MiB 重组
//     StoreChunk → 读 16B 整文件 MD5 与重组累计比对。
//
// 查询/落库均用库内全路径（prefix + e.Path），与 sender 方向子路径恢复对称。
func receiveFileDelta(ctx context.Context, stream *MuxStream, out *MuxWriter, ndxOut, ndxIn *ndxCodec, r *repo.Repo, neg *Negotiation, activeID int64, e FileEntry, index int, prefix string) ([]meta.ChunkRef, bool, fileStats, error) {
	started := time.Now()
	st := fileStats{method: "delta"}
	fullPath := e.Path
	if prefix != "" {
		fullPath = prefix + "/" + e.Path
	}
	row, ok, err := r.GetFileRow(activeID, fullPath)
	if err != nil {
		return nil, true, st, err
	}
	// quick check 命中：mtime+size 一致且旧行确为普通文件（类型变化不可跳过）。
	// 不发出任何字节，返回 updated=false（CopyFiles 已继承旧行与 chunk 关联，不落库）。
	if ok && !row.IsDir && !row.IsSymlink && row.MTimeNs == e.MTimeNs && row.Size == e.Size {
		st.method = "quick_check"
		st.elapsed = time.Since(started)
		return nil, false, st, nil
	}
	// 全量路径：无旧行 / 空文件（新旧任一为空都无 delta 基础）/ 旧行类型不一致
	if !ok || e.Size == 0 || row.Size == 0 || row.IsDir || row.IsSymlink {
		legacyRefs, lst, lerr := receiveFileLegacy(ctx, stream, out, ndxOut, ndxIn, r, index)
		lst.elapsed = time.Since(started)
		return legacyRefs, true, lst, lerr
	}

	// --- delta 路径：旧文件为 basis ---
	// 第一遍读旧文件计算块校验和表（len 取旧文件大小：sum 表描述 basis 块结构，
	// 客户端按收到的 blength 匹配自己的新文件）
	tbl, err := calcBlockSumsFromRepo(r, activeID, fullPath, row.Size, neg.ChecksumSeed)
	if err != nil {
		return nil, true, st, fmt.Errorf("计算块校验和: %w", err)
	}
	st.blength = tbl.Blength
	// 发 ndx + iflags + sum_head + 逐块校验和（协议 31 非 mux 独立帧，随 MSG_DATA 流）
	var buf bytesBuffer
	if err := ndxOut.Write(&buf, int32(index)); err != nil {
		return nil, true, st, err
	}
	if err := WriteShortint(&buf, itemTransfer|itemIsNew); err != nil {
		return nil, true, st, err
	}
	for _, v := range [...]int32{tbl.Count, tbl.Blength, tbl.S2Length, tbl.Remainder} {
		if err := WriteInt32(&buf, v); err != nil {
			return nil, true, st, err
		}
	}
	for _, s := range tbl.Sums {
		if err := WriteInt32(&buf, int32(s.Sum1)); err != nil {
			return nil, true, st, err
		}
		buf.Write(s.Sum2[:])
	}
	if err := out.WriteData(buf.Bytes()); err != nil {
		return nil, true, st, err
	}
	// 客户端回显：ndx+iflags + sum_head 镜像（与发送一致，防御协议错位）
	if err := recvNdxEcho(stream, ndxIn); err != nil {
		return nil, true, st, err
	}
	var echo [4]int32
	for i := range echo {
		if echo[i], err = ReadInt32(stream); err != nil {
			return nil, true, st, fmt.Errorf("回显 sum_head: %w", err)
		}
	}
	if echo != [4]int32{tbl.Count, tbl.Blength, tbl.S2Length, tbl.Remainder} {
		return nil, true, st, fmt.Errorf("回显 sum_head 与发送不一致: %v", echo)
	}
	// 第二遍读旧文件（basisReader 短连接逐块读）供 match 块复制
	br := &basisReader{r: r, snapshotID: activeID, path: fullPath, fileSize: row.Size}

	// token 循环 + 4MiB 重组 + MD5 累计
	chunkSize := r.ChunkSizeBytes()
	data := make([]byte, 0, chunkSize)
	var refs []meta.ChunkRef
	var idx int
	h := md5.New()
	var consumed int64 // 旧文件流已消费偏移（match 单调递增断言基准）
	flush := func() error {
		if len(data) == 0 {
			return nil
		}
		id, reused, err := r.StoreChunk(data)
		if err != nil {
			return err
		}
		if !reused {
			st.stored++
		}
		refs = append(refs, meta.ChunkRef{ChunkID: id, IDX: idx})
		idx++
		data = data[:0]
		return nil
	}
	// flushFull 将 data 中所有完整 chunk 逐段落库：匹配块（rsync blength 最大 32KB，
	// 测试配置 chunk=64）常跨多个 chunk 边界，==chunkSize 判断会漏冲刷导致 data 无限
	// 累积。逐段分存可保证每块恰为 chunkSize（末块除外），chunk 布局规整——
	// basisReader 的 pos/chunkSize 索引假设对 delta 快照同样成立。
	flushFull := func() error {
		for len(data) >= chunkSize {
			full := data[:chunkSize]
			id, reused, err := r.StoreChunk(full)
			if err != nil {
				return err
			}
			if !reused {
				st.stored++
			}
			refs = append(refs, meta.ChunkRef{ChunkID: id, IDX: idx})
			idx++
			data = data[chunkSize:]
		}
		if cap(data) < chunkSize {
			// 切空（cap=0）或残余底层容量不足时换新缓冲：literal 路径按
			// chunkSize 直接切片写入需保证容量；容量足够时保留底层零拷贝
			nd := make([]byte, len(data), chunkSize)
			copy(nd, data)
			data = nd
		}
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, true, st, err
		}
		token, err := ReadInt32(stream)
		if err != nil {
			return nil, true, st, err
		}
		if token == 0 {
			break // 文件数据流结束（token.c:319）
		}
		if token < 0 {
			// match 第 b 块：从旧文件流复制（偏移 b*blength，末块长度按表规则）
			b := int64(-token) - 1
			if b >= int64(tbl.Count) {
				return nil, true, st, fmt.Errorf("匹配块索引越界: %d (共 %d 块)", b, tbl.Count)
			}
			offset2 := b * int64(tbl.Blength)
			if offset2 < consumed {
				return nil, true, st, fmt.Errorf("匹配块偏移回退: %d < %d（rsync sender 应单调递增）", offset2, consumed)
			}
			if offset2 > consumed {
				if _, err := io.CopyN(io.Discard, br, offset2-consumed); err != nil {
					return nil, true, st, fmt.Errorf("推进旧文件流到偏移 %d: %w", offset2, err)
				}
				consumed = offset2
			}
			blen := int64(tbl.Blength)
			if b == int64(tbl.Count)-1 && tbl.Remainder != 0 {
				blen = int64(tbl.Remainder)
			}
			if consumed+blen > row.Size {
				return nil, true, st, fmt.Errorf("匹配块越界: 偏移 %d 长度 %d 超过旧文件 %d 字节", consumed, blen, row.Size)
			}
			match := make([]byte, blen)
			if _, err := io.ReadFull(br, match); err != nil {
				return nil, true, st, fmt.Errorf("读取匹配块 %d: %w", b, err)
			}
			consumed += blen
			st.matched += blen
			h.Write(match)
			data = append(data, match...)
			if err := flushFull(); err != nil { // 匹配块跨 chunk 边界，逐段落库
				return nil, true, st, err
			}
			continue
		}
		// literal：读 token 字节原始数据（客户端按 CHUNK_SIZE 分包，服务端合并）
		n := int(token)
		if n > 32<<20 {
			return nil, true, st, fmt.Errorf("字面量块过大: %d", n)
		}
		st.literal += int64(n)
		remaining := n
		for remaining > 0 {
			take := min(remaining, chunkSize-len(data))
			tmp := data[len(data) : len(data)+take]
			if _, err := io.ReadFull(stream, tmp); err != nil {
				return nil, true, st, err
			}
			data = data[:len(data)+take]
			h.Write(tmp)
			remaining -= take
			if len(data) == chunkSize {
				if err := flush(); err != nil {
					return nil, true, st, err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return nil, true, st, err
	}
	// 整文件强校验和（纯 MD5(内容)，客户端 sum_end 不混 seed）与重组累计比对
	var wantSum [xferSumLen]byte
	if _, err := io.ReadFull(stream, wantSum[:]); err != nil {
		return nil, true, st, err
	}
	gotSum := h.Sum(nil)
	if !bytes.Equal(gotSum, wantSum[:]) {
		return nil, true, st, fmt.Errorf("整文件校验和不匹配: 重组 %x vs 客户端 %x", gotSum, wantSum)
	}
	st.chunks = len(refs)
	st.elapsed = time.Since(started)
	return refs, true, st, nil
}

// basisReader 顺序读取旧文件（basis）内容供 match 块复制：按 4MiB 存储块
// 短连接读取（每块查询即查即关），避免 io.Pipe+StreamFile 长连接方案与
// meta 层 MaxOpenConns=1 互锁。
type basisReader struct {
	r          *repo.Repo
	snapshotID int64
	path       string
	fileSize   int64
	cur        []byte // 当前存储块明文
	pos        int64  // 已消费偏移
}

func (b *basisReader) Read(p []byte) (int, error) {
	for len(b.cur) == 0 {
		if b.pos >= b.fileSize {
			return 0, io.EOF
		}
		idx := int(b.pos / int64(b.r.ChunkSizeBytes()))
		blob, err := b.r.ReadChunkAt(b.snapshotID, b.path, idx)
		if err != nil {
			return 0, err
		}
		b.cur = blob
	}
	n := copy(p, b.cur)
	b.cur = b.cur[n:]
	b.pos += int64(n)
	return n, nil
}

// calcBlockSumsFromRepo 第一遍读旧文件计算块校验和表：先按 size 定块结构
// （len 取旧文件大小：sum 表描述 basis 块结构，客户端按收到的 blength 匹配
// 自己的新文件），再流式读旧文件逐块计算 sum1+sum2（StreamFile 内部已校验
// 内容与 files 表 size 一致，此处再按传入 size 复核）。
func calcBlockSumsFromRepo(r *repo.Repo, snapshotID int64, path string, size int64, seed int32) (SumTable, error) {
	count, blength, remainder, err := CalcSizes(size)
	if err != nil {
		return SumTable{}, err
	}
	w := &sumTableWriter{
		tbl:     SumTable{Count: count, Blength: blength, S2Length: strongSumLen, Remainder: remainder},
		seed:    seed,
		fileLen: size,
	}
	if _, _, err := r.StreamFile(snapshotID, path, 0, w); err != nil {
		return SumTable{}, err
	}
	return w.finish()
}

// sumTableWriter 累积流式字节，满 blength 即计算一块校验和（sum1+sum2）。
type sumTableWriter struct {
	tbl     SumTable
	buf     []byte
	seed    int32
	total   int64
	fileLen int64
}

func (w *sumTableWriter) Write(p []byte) (int, error) {
	w.total += int64(len(p))
	w.buf = append(w.buf, p...)
	bl := int(w.tbl.Blength)
	for len(w.buf) >= bl {
		block := w.buf[:bl]
		w.tbl.Sums = append(w.tbl.Sums, BlockSum{RollingSum1(block), StrongSum2(block, w.seed)})
		w.buf = w.buf[bl:]
	}
	return len(p), nil
}

func (w *sumTableWriter) finish() (SumTable, error) {
	if w.total != w.fileLen {
		return SumTable{}, fmt.Errorf("旧文件读不完整: %d/%d 字节", w.total, w.fileLen)
	}
	if len(w.buf) > 0 { // 末块（长度=remainder 或 blength）
		w.tbl.Sums = append(w.tbl.Sums, BlockSum{RollingSum1(w.buf), StrongSum2(w.buf, w.seed)})
	}
	if int32(len(w.tbl.Sums)) != w.tbl.Count {
		return SumTable{}, fmt.Errorf("块数不符: %d/%d", len(w.tbl.Sums), w.tbl.Count)
	}
	return w.tbl, nil
}
