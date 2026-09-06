// internal/front/rsync/protocol/receiver.go
package protocol

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"crysync/internal/config"
	"crysync/internal/core/meta"
	"crysync/internal/core/types"
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
		"preserve_atimes", neg.PreserveAtimes,
		"delete_mode", neg.DeleteMode,
		"numeric_ids", neg.NumericIDs,
		"io_timeout", neg.IoTimeout,
	)
}

// RunReceiver 处理一次备份方向会话（客户端推）：argv/二进制协商 -> mux 会话。
// 调用方需先完成：HandleModuleRequest（greeting/模块选择/认证）。
func RunReceiver(ctx context.Context, br *bufio.Reader, w io.Writer, module *config.ModuleConfig, s types.Session, logger *slog.Logger) error {
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
	k := applyIoTimeout(w, mr, mw, neg)
	return wrapIoTimeout(processSession(ctx, mr, mw, module, s, neg, logger, k), w, mw, neg)
}

// RunReceiverWithReader：与已进行握手/认证的 bufio.Reader 继续协议（避免预读丢失）。
// handleConn 已用同一个 br 读过文本行阶段，br 的 buffer 可能预读后续协议字节，
// 必须原样传给 RunReceiver，否则预读字节丢失（RunReceiver 内部完成 NegotiateBinary + mux 会话）。
func RunReceiverWithReader(ctx context.Context, br *bufio.Reader, conn net.Conn, module *config.ModuleConfig, s types.Session, logger *slog.Logger) error {
	return RunReceiver(ctx, br, conn, module, s, logger)
}

func storeChunk(ctx context.Context, s types.Session, data []byte) (int64, bool, error) {
	if cs, ok := s.(types.ContextSession); ok {
		return cs.StoreChunkContext(ctx, data)
	}
	return s.StoreChunk(data)
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
	// badSum：整文件校验和不匹配（客户端读源中途失败发坏校验和，receiver.c
	// sum_end 比对失败）——该文件不落库，会话继续
	badSum  bool
	elapsed time.Duration
}

// processSession 完整 mux 会话：
// [条件]filter -> flist（逐条到哨兵）-> [条件]id list -> 传输阶段（generator 角色
// 逐条 ndx/iflags/sums 驱动 + receiver 角色读回显与 token 流）-> --delete 收敛
// -> goodbye。keeper 非 nil（客户端 --timeout=N）时经 KeepAlive 在块存储、
// 批量删除等本地慢工作点续期。
func processSession(ctx context.Context, in *MuxReader, out *MuxWriter, module *config.ModuleConfig, s types.Session, neg *Negotiation, logger *slog.Logger, keeper *idleKeeper) (err error) {
	if logger == nil {
		logger = nopLogger
	}
	keepAlive := func() {}
	if keeper != nil {
		keepAlive = keeper.KeepAlive
	}
	stream := NewMuxStream(in)
	stream.Logger = logger // 跳过的 mux 消息帧记 debug（P2#11）
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
		e, err := parser.Parse(stream, neg.PreserveUID, neg.PreserveGID, neg.PreserveLinks, neg.PreserveDevices, neg.PreserveAtimes, neg.ChecksumMode)
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
		files        int // flist 条目总数
		transferred  int // 实际传输文件（full+delta）
		skipped      int // quick check 命中
		dirs         int
		links        int
		matched      int64
		literal      int64
		chunksStored int
		deleted      int // --delete 移除的行（文件+目录）
		failed       int // 失败文件（客户端 open 失败 MSG_NO_SEND / 校验和不匹配）
	}
	st.files = len(entries)
	var curPath string
	defer func() {
		if err != nil {
			logger.Error("session_error", "path", curPath, "err", err.Error())
		}
	}()

	// 模块写互斥（会话全程持有）：同模块并发写会话与 WebDAV 写在排队等待。
	// v0.5 单一当前状态模型：无快照事务——失败 = 半更新镜像（已落库文件保留，
	// 客户端重试时 quick check 跳过已正确文件，幂等收敛）。
	unlock := s.WriteSessionLock()
	defer unlock()

	// 传输阶段：对 flist 中每个条目发 ndx+iflags（目录/链接无 sums），
	// 客户端 sender 对每条回显 ndx+iflags（文件另有 sum_head 回显与 token 流）。
	for i, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		curPath = e.Path
		if e.Path == "" {
			// 顶层目录 "."（cleanPath 拒绝）：模块根时不落库，但仍需走 ndx 协议交互；
			// 子路径备份时 "." 是传输根目录（prefix）自身的属性，落为 prefix 目录行，
			// 恢复方向以子路径拉取时化身 "."（sender 侧 f.Path==prefix 分支）
			if !e.IsDir {
				return fmt.Errorf("flist 条目 %d: 非法路径", i)
			}
			if err := driveEntry(out, stream, ndxOut, ndxIn, i, itemIsNew); err != nil {
				return fmt.Errorf("条目 %d: %w", i, err)
			}
			if prefix != "" && !neg.DryRun {
				if err := applyStaticEntry(s, e, prefix); err != nil {
					return fmt.Errorf("落库 %s: %w", prefix, err)
				}
				st.dirs++
				logger.Info("entry", "path", prefix, "type", "dir",
					"mode", fmt.Sprintf("%o", e.Mode), "uid", e.UID, "gid", e.GID,
					"mtime", e.MTimeNs/1e9)
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
				return fmt.Errorf("条目 %d: %w", i, err)
			}
			continue
		}
		if e.IsDir || e.IsSymlink {
			// preserve off（无 -l）收到的 symlink 无 target 字段，无法恢复——
			// 跳过落库并警告（协议本身合法，客户端未发送该信息）。MSG_INFO 提示
			// 对齐真实 rsyncd（generator.c:2113 无 -l 时 FINFO 回显同文本）
			if e.IsSymlink && e.LinkTarget == "" {
				logger.Warn("skip_link_no_target",
					"path", e.Path, "hint", "客户端未启用 -l（preserve links），symlink 未备份")
				_ = out.WriteInfoMsg(fmt.Sprintf(
					"skipping non-regular file %q\n", e.Path))
				if err := driveEntry(out, stream, ndxOut, ndxIn, i, itemIsNew); err != nil {
					return fmt.Errorf("条目 %d: %w", i, err)
				}
				continue
			}
			if err := driveEntry(out, stream, ndxOut, ndxIn, i, itemIsNew); err != nil {
				return fmt.Errorf("条目 %d: %w", i, err)
			}
			if !neg.DryRun {
				if err := applyStaticEntry(s, e, full(e.Path)); err != nil {
					return fmt.Errorf("落库 %s: %w", full(e.Path), err)
				}
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
		refs, updated, fs, err := receiveFileDelta(ctx, stream, out, ndxOut, ndxIn, s, neg, e, i, prefix, keepAlive)
		if err != nil {
			// 客户端 MSG_NO_SEND（open 失败/vanished，sender.c:722-724）：该文件被
			// 跳过（无回显无 token 流），客户端已报错（rc=23）——不落库，会话继续
			var nse *noSendError
			if errors.As(err, &nse) {
				if nse.ndx != int32(i) {
					return fmt.Errorf("MSG_NO_SEND ndx 错位: %d（当前 %d）", nse.ndx, i)
				}
				st.failed++
				logger.Warn("file_skipped", "path", full(e.Path),
					"reason", "客户端无法读取源文件（MSG_NO_SEND）")
				continue
			}
			return fmt.Errorf("文件 %s: %w", e.Path, err)
		}
		// 整文件校验和不匹配（客户端读源中途失败发坏校验和）：不落库，发
		// MSG_ERROR_XFER 使客户端计入 io_error（rc=23），会话继续其余文件
		if fs.badSum {
			st.failed++
			logger.Error("file_verify_failed", "path", full(e.Path),
				"size", e.Size, "hint", "整文件 MD5 不匹配，客户端读源失败")
			_ = out.WriteMsg(fmt.Sprintf("file corruption in %q (checksum mismatch)\n", e.Path))
			continue
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
		if updated && !neg.DryRun { // quick check 命中不落库（旧行即当前状态）；dry-run 零持久化
			if err := s.UpsertFile(meta.FileRow{
				Path: full(e.Path), Mode: e.Mode, UID: e.UID, GID: e.GID,
				Size: e.Size, MTimeNs: e.MTimeNs,
			}, refs); err != nil {
				return fmt.Errorf("落库 %s: %w", full(e.Path), err)
			}
		}
	}

	// --delete 语义（generator.c delete_in_dir/delete_missing，flist.c:1402）：
	// 删除传输根（prefix 子树）内、本次 flist 未覆盖的文件与目录行——单一状态
	// 模型下不删除则客户端删掉的文件下次恢复时复活。io_error≠0 时禁用（发送侧
	// flist 构造出错，源清单不完整，删除会误删）。覆盖集含全部 flist 条目路径
	// （含跳过未落库的设备/special 与无 target 链接），目录路径归一化无尾斜杠。
	// dry-run 会话（argv 'n'）不执行（真实 rsync -n 同样只报告不删除）——
	// 会话期间的所有落库（静态条目/文件行）在 dry-run 下均已短路跳过。
	if neg.DeleteMode && !neg.DryRun && parser.IoError == 0 {
		covered := make(map[string]bool, len(entries))
		for _, e := range entries {
			covered[full(strings.TrimSuffix(e.Path, "/"))] = true
		}
		rows, err := s.FileRows(prefix)
		if err != nil {
			return fmt.Errorf("--delete 枚举: %w", err)
		}
		for _, row := range rows {
			if covered[row.Path] {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			keepAlive()
			if err := s.DeleteFile(row.Path); err != nil {
				return fmt.Errorf("--delete: %w", err)
			}
			st.deleted++
		}
		if st.deleted > 0 {
			logger.Info("delete_extraneous", "count", st.deleted, "prefix", prefix)
		}
	}

	// goodbye 必须在所有持久化操作完成后开始：最终 DONE 会让客户端成功退出，
	// 因而它也是服务端状态已收敛的完成边界。若先 goodbye 再删除，客户端返回
	// 后可短暂读到旧清单，删除失败也无法反馈给客户端。
	// generator.c:2336/2368-2376 + main.c:1119：
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
				return err
			}
		}
		for k := 0; k < step.readAck; k++ {
			if err := readNdxDone(stream, ndxIn); err != nil {
				return err
			}
		}
	}

	// dry-run：会话走完全部协议但零持久化（v0.5 无事务可回滚，落库调用点
	// 均已短路——见静态条目与 updated 块）。空 flist 会话天然零写入。
	logger.Info("session_done",
		"files", st.files,
		"transferred", st.transferred,
		"skipped", st.skipped,
		"dirs", st.dirs,
		"links", st.links,
		"deleted", st.deleted,
		"failed", st.failed,
		"bytes_matched", st.matched,
		"bytes_literal", st.literal,
		"chunks_stored", st.chunksStored,
		"dry_run", neg.DryRun,
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
func applyStaticEntry(s types.Session, e FileEntry, path string) error {
	if e.IsSymlink {
		return s.UpsertFile(meta.FileRow{
			Path: path, IsSymlink: true, Mode: e.Mode,
			UID: e.UID, GID: e.GID, MTimeNs: e.MTimeNs, LinkTarget: e.LinkTarget,
		}, nil)
	}
	if e.IsDir {
		// 目录路径统一去尾斜杠后落库
		return s.UpsertFile(meta.FileRow{
			Path: strings.TrimSuffix(path, "/"), IsDir: true, Mode: e.Mode,
			UID: e.UID, GID: e.GID, MTimeNs: e.MTimeNs,
		}, nil)
	}
	return nil
}

// receiveFileLegacy 全量传输路径（v1）：发 ndx + iflags(ITEM_TRANSFER|ITEM_IS_NEW)
// + write_sum_head(NULL)（16 字节全 0）→ 客户端 count==0 全量 literal 发送。
// 用于新文件/空文件/旧行类型不一致（无可作 basis 的旧文件）场景。
func receiveFileLegacy(ctx context.Context, stream *MuxStream, out *MuxWriter, ndxOut, ndxIn *ndxCodec, s types.Session, index int, keepAlive func()) ([]meta.ChunkRef, fileStats, error) {
	started := time.Now()
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

	chunkSize := s.ChunkSizeBytes()
	data := make([]byte, 0, chunkSize)
	var refs []meta.ChunkRef
	var idx int
	h := md5.New() // 整文件校验和累计（客户端 read 中途失败时发坏校验和，必须比对）
	flush := func() error {
		if len(data) == 0 {
			return nil
		}
		keepAlive() // StoreChunk 可能写慢后端（WebDAV）：续期 + 心跳防客户端超时误断
		id, reused, err := storeChunk(ctx, s, data)
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
			h.Write(tmp)
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
	// 整文件强校验和（纯 MD5(内容)，与 delta 路径同语义）：不匹配说明客户端
	// 读源中途失败（sender 发坏校验和）——不落库该文件（此前静默存坏数据），
	// 调用方记 file_verify_failed 并继续其余文件
	var wantSum [xferSumLen]byte
	if _, err := io.ReadFull(stream, wantSum[:]); err != nil {
		return nil, st, err
	}
	if !bytes.Equal(h.Sum(nil), wantSum[:]) {
		st.badSum = true
		st.elapsed = time.Since(started)
		return nil, st, nil
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
//     任何字节（旧行即当前状态，quick check 命中不落库）。
//  2. 无旧行 / 空文件 / 旧行类型不一致 → 全量路径（receiveFileLegacy）。
//  3. 变化文件：第一遍读旧文件算块校验和表 → 发 ndx+iflags+sum_head+块校验和
//     → 读回显 → token 流（match 从旧文件流复制 + literal 直收）按 4MiB 重组
//     StoreChunk → 读 16B 整文件 MD5 与重组累计比对。
//
// 查询/落库均用库内全路径（prefix + e.Path），与 sender 方向子路径恢复对称。
func receiveFileDelta(ctx context.Context, stream *MuxStream, out *MuxWriter, ndxOut, ndxIn *ndxCodec, s types.Session, neg *Negotiation, e FileEntry, index int, prefix string, keepAlive func()) ([]meta.ChunkRef, bool, fileStats, error) {
	started := time.Now()
	st := fileStats{method: "delta"}
	fullPath := e.Path
	if prefix != "" {
		fullPath = prefix + "/" + e.Path
	}
	row, ok, err := s.GetFileRow(fullPath)
	if err != nil {
		return nil, true, st, err
	}
	// quick check 命中：mtime+size 一致且旧行确为普通文件（类型变化不可跳过）。
	// 不发出任何字节，返回 updated=false（单一状态模型下旧行即当前状态，无需写）。
	if ok && !row.IsDir && !row.IsSymlink && row.MTimeNs == e.MTimeNs && row.Size == e.Size {
		st.method = "quick_check"
		st.elapsed = time.Since(started)
		return nil, false, st, nil
	}
	// dry-run（argv 'n'）：真实 generator 不发 sum_head/sums（generator.c:2390
	// !do_xfers 提前 cleanup），客户端 sender 对传输请求也只回显 ndx+iflags
	// （sender.c:638-642）——不发 token 流与校验和。此处对齐：只发 ndx+iflags
	// 并读回显，不落库（updated=false，调用方 dry-run 短路）——v0.5 无事务
	// 可回滚，持久化一律在 dry-run 下跳过。
	if neg.DryRun {
		if err := sendNdxIflags(out, ndxOut, index, itemTransfer|itemIsNew); err != nil {
			return nil, false, st, err
		}
		if err := recvNdxEcho(stream, ndxIn); err != nil {
			return nil, false, st, err
		}
		st.method = "dry_run"
		st.elapsed = time.Since(started)
		return nil, false, st, nil
	}
	// 全量路径：无旧行 / 空文件（新旧任一为空都无 delta 基础）/ 旧行类型不一致
	if !ok || e.Size == 0 || row.Size == 0 || row.IsDir || row.IsSymlink {
		legacyRefs, lst, lerr := receiveFileLegacy(ctx, stream, out, ndxOut, ndxIn, s, index, keepAlive)
		lst.elapsed = time.Since(started)
		return legacyRefs, true, lst, lerr
	}

	// --- delta 路径：旧文件为 basis ---
	// 第一遍读旧文件计算块校验和表（len 取旧文件大小：sum 表描述 basis 块结构，
	// 客户端按收到的 blength 匹配自己的新文件）
	tbl, err := calcBlockSumsFromRepo(s, fullPath, row.Size, neg.ChecksumSeed)
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
	br := &basisReader{s: s, path: fullPath, fileSize: row.Size}

	// token 循环 + 4MiB 重组 + MD5 累计
	chunkSize := s.ChunkSizeBytes()
	data := make([]byte, 0, chunkSize)
	var refs []meta.ChunkRef
	var idx int
	h := md5.New()
	var consumed int64 // 旧文件流已消费偏移（match 单调递增断言基准）
	flush := func() error {
		if len(data) == 0 {
			return nil
		}
		keepAlive() // StoreChunk 可能写慢后端（WebDAV）：续期 + 心跳防客户端超时误断
		id, reused, err := storeChunk(ctx, s, data)
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
			id, reused, err := storeChunk(ctx, s, full)
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
	// 整文件强校验和（纯 MD5(内容)，客户端 sum_end 不混 seed）与重组累计比对：
	// 不匹配说明客户端读源中途失败（发坏校验和）——不落库该文件（已存块成孤儿
	// 由 GC 回收），会话继续其余文件（rsync 同语义：单文件报错，其余正常完成）
	var wantSum [xferSumLen]byte
	if _, err := io.ReadFull(stream, wantSum[:]); err != nil {
		return nil, true, st, err
	}
	gotSum := h.Sum(nil)
	if !bytes.Equal(gotSum, wantSum[:]) {
		st.badSum = true
		st.elapsed = time.Since(started)
		return nil, true, st, nil
	}
	st.chunks = len(refs)
	st.elapsed = time.Since(started)
	return refs, true, st, nil
}

// basisReader 顺序读取旧文件（basis）内容供 match 块复制：按 4MiB 存储块
// 短连接读取（每块查询即查即关），避免 io.Pipe+StreamFile 长连接方案与
// meta 层 MaxOpenConns=1 互锁。
type basisReader struct {
	s        types.Session
	path     string
	fileSize int64
	cur      []byte // 当前存储块明文
	pos      int64  // 已消费偏移
}

func (b *basisReader) Read(p []byte) (int, error) {
	for len(b.cur) == 0 {
		if b.pos >= b.fileSize {
			return 0, io.EOF
		}
		idx := int(b.pos / int64(b.s.ChunkSizeBytes()))
		blob, err := b.s.ReadChunkAt(b.path, idx)
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
func calcBlockSumsFromRepo(s types.Session, path string, size int64, seed int32) (SumTable, error) {
	count, blength, remainder, err := CalcSizes(size)
	if err != nil {
		return SumTable{}, err
	}
	w := &sumTableWriter{
		tbl:     SumTable{Count: count, Blength: blength, S2Length: strongSumLen, Remainder: remainder},
		seed:    seed,
		fileLen: size,
	}
	if _, _, err := s.StreamFile(path, 0, w); err != nil {
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
