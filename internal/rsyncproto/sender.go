// internal/rsyncproto/sender.go
package rsyncproto

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strings"
	"time"

	"crysync/internal/config"
	"crysync/internal/repo"
)

// RunSession 完成 argv 读取与二进制协商，按方向路由：
//   - argv 含 --sender（客户端拉取）→ 服务端为 sender（恢复方向）
//   - 否则 → 服务端为 receiver（备份方向）
//
// 只读模块拒绝推送（真实 rsyncd 对只读模块 push 报错退出，do_server_recv）。
func RunSession(ctx context.Context, br *bufio.Reader, w io.Writer, module *config.ModuleConfig, r *repo.Repo, logger *slog.Logger) error {
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
		return rejectWait(err)
	}
	if module.ReadOnly && !neg.SenderMode {
		// 对齐 rsyncd do_server_recv（main.c:1193-1197）：先经 MSG_ERROR(3) 告知
		// 客户端再发 MSG_ERROR_EXIT(86, RERR_SYNTAX=1)（cleanup.c），客户端 stderr
		// 可见明确原因并以退出码 1 结束，而非 connection unexpectedly closed
		rejectWithExit(mw, "ERROR: module is read only\n", 1)
		return rejectWait(fmt.Errorf("模块 %s 只读，拒绝推送", module.Name))
	}
	if err := rejectAppend(mw, neg); err != nil {
		return rejectWait(err)
	}
	if err := rejectRestoreAtimes(mw, neg); err != nil {
		return rejectWait(err)
	}
	k := applyIoTimeout(w, mr, mw, neg)
	run := func() error {
		if neg.SenderMode {
			// 恢复方向错误路径可能已发拒绝帧，rejectWait 等客户端读到再断连
			return rejectWait(processSendSession(ctx, mr, mw, module, r, neg, logger))
		}
		return processSession(ctx, mr, mw, module, r, neg, logger, k)
	}
	return wrapIoTimeout(run(), w, mw, neg)
}

// RunSessionWithReader：与已进行握手/认证的 bufio.Reader 继续协议（避免预读丢失）。
func RunSessionWithReader(ctx context.Context, br *bufio.Reader, conn net.Conn, module *config.ModuleConfig, r *repo.Repo, logger *slog.Logger) error {
	return RunSession(ctx, br, conn, module, r, logger)
}

// RunSender 处理一次恢复方向会话（客户端拉取，服务端为 sender）：argv/二进制协商
// -> mux 会话。调用方需先完成：HandleModuleRequest（greeting/模块选择/认证）。
func RunSender(ctx context.Context, br *bufio.Reader, w io.Writer, module *config.ModuleConfig, r *repo.Repo, logger *slog.Logger) error {
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
	if err := rejectWait(rejectCompression(mw, neg)); err != nil {
		return err
	}
	if err := rejectWait(rejectAppend(mw, neg)); err != nil {
		return err
	}
	if err := rejectWait(rejectRestoreAtimes(mw, neg)); err != nil {
		return err
	}
	// 恢复方向以写为主（每帧写即重置计时），无本地慢工作点，keeper 仅用于
	// 挂载重置回调与宣告帧，返回值不需要
	applyIoTimeout(w, mr, mw, neg)
	return wrapIoTimeout(rejectWait(processSendSession(ctx, mr, mw, module, r, neg, logger)), w, mw, neg)
}

// RunSenderWithReader：与已进行握手/认证的 bufio.Reader 继续协议（避免预读丢失），
// 与 RunReceiverWithReader 对称。
func RunSenderWithReader(ctx context.Context, br *bufio.Reader, conn net.Conn, module *config.ModuleConfig, r *repo.Repo, logger *slog.Logger) error {
	return RunSender(ctx, br, conn, module, r, logger)
}

// ITEM_* 标志补充（rsync.h:205-235；传输阶段 iflags）
const (
	itemBasisTypeFollows = uint16(1 << 11) // ITEM_BASIS_TYPE_FOLLOWS：后跟 1 字节 basis 类型
	itemXnameFollows     = uint16(1 << 12) // ITEM_XNAME_FOLLOWS：后跟 vstring xname
)

const (
	ndxDelStats = int32(-3) // NDX_DEL_STATS
	ndxFlistEOF = int32(-2) // NDX_FLIST_EOF（非增量模式不应出现）
)

// literalChunkSize：字面量 token 段上限（token.c simple_send_token 的 CHUNK_SIZE）。
const literalChunkSize = 32 << 10

// processSendSession 完整 sender mux 会话（do_server_sender，main.c:908-967）：
// filter 列表 -> flist + id list -> 传输循环（响应客户端 generator 的 ndx/sums
// 请求并回送数据）-> 尾部 NDX_DONE + stats -> read_final_goodbye。
func processSendSession(ctx context.Context, in *MuxReader, out *MuxWriter, module *config.ModuleConfig, r *repo.Repo, neg *Negotiation, logger *slog.Logger) (err error) {
	if logger == nil {
		logger = nopLogger
	}
	started := time.Now()
	curPath := "" // 最近处理的条目路径（错误定位用）
	defer func() {
		if err != nil {
			logger.Error("session_error", "dir", "restore", "path", curPath, "err", err.Error())
		}
	}()

	stream := NewMuxStream(in)
	stream.Logger = logger // 跳过的 mux 消息帧记 debug（P2#11）
	// ndx 差分编码按方向独立：ndxIn 读客户端（generator 请求），ndxOut 写回复
	ndxIn := newNdxCodec()  // C->S
	ndxOut := newNdxCodec() // S->C

	// 客户端（receiver）总是发送 filter 列表（main.c:1352 send_filter_list），
	// 至少一个 int32 0——与备份方向"仅 delete 时出现"不同（那边客户端是 sender）。
	if err := recvFilterList(stream); err != nil {
		return fmt.Errorf("filter: %w", err)
	}

	// 快照清单 -> flist 条目 -> f_name_cmp 排序（ndx = 排序后索引，两端一致）。
	// 子路径拉取（prefix 非空）时条目名相对传输根：目录化身 "."（DOTDIR_NAME
	// 语义，客户端把其属性应用到目标目录），其下条目剥前缀。
	sid, err := r.ActiveSnapshotID()
	if err != nil {
		return err
	}
	if sid == 0 {
		// 明确告知客户端再断连（静默断连客户端只见 connection unexpectedly closed）
		rejectWithExit(out, "ERROR: module has no snapshot to restore\n", 1)
		return fmt.Errorf("模块 %s 没有可恢复的快照", module.Name)
	}
	prefix := modulePrefix(neg.ModuleArg, module.Name)
	rows, err := r.SnapshotFileRows(sid, prefix)
	if err != nil {
		return err
	}
	// flistItem：发送条目 + 快照内原始路径（传输阶段按 ndx 查回时用原始路径读内容）
	type flistItem struct {
		entry    FileEntry
		snapPath string
	}
	items := make([]flistItem, 0, len(rows))
	for _, f := range rows {
		name := f.Path
		if prefix != "" {
			switch {
			case f.Path == prefix && f.IsDir:
				name = "."
			case f.Path == prefix:
				name = pathBase(f.Path)
			default:
				name = strings.TrimPrefix(f.Path, prefix+"/")
			}
		}
		// 非 -r（如 --list-only 无递归）：客户端隐含过滤为单层（exclude.c:633
		// arg_len==0 时仅 "/*"），发深层条目会被拒（flist.c:1144 unrequested）。
		// 只发传输根下一层："." 与不含 '/' 的名字。
		if !neg.Recurse && name != "." && strings.Contains(name, "/") {
			continue
		}
		items = append(items, flistItem{
			entry: FileEntry{
				Path: name, IsDir: f.IsDir, IsSymlink: f.IsSymlink,
				Mode: f.Mode, UID: f.UID, GID: f.GID, Size: f.Size,
				MTimeNs: f.MTimeNs, LinkTarget: f.LinkTarget,
			},
			snapPath: f.Path,
		})
	}
	// 与 SortFlistEntries 相同的 f_name_cmp 稳定排序（ndx = 排序后索引）
	sort.SliceStable(items, func(i, j int) bool { return fNameCmp(items[i].entry, items[j].entry) < 0 })
	logger.Info("session_start", "dir", "restore", "argv", strings.Join(neg.Argv, " "), "entries", len(items))

	// 发 flist：条目 + 哨兵（旧式路径，单字节 0）+ id list（数值直通：空段 varint 0）
	fw := NewFlistWriter()
	for _, it := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		var buf bytesBuffer
		if err := fw.WriteEntry(&buf, it.entry, neg.PreserveUID, neg.PreserveGID, neg.PreserveLinks); err != nil {
			return fmt.Errorf("编码 flist 条目 %s: %w", it.entry.Path, err)
		}
		// --checksum（-c）：每条 REGULAR 条目尾部附 flist_csum_len=16 字节纯内容
		// MD5（flist.c:757-766 file_checksum，无 seed；客户端 recv_file_entry
		// 无条件读该段，缺失则整条流错位）。目录/符号链接不附。
		if neg.ChecksumMode && !it.entry.IsDir && !it.entry.IsSymlink && isRegularMode(it.entry.Mode) {
			_, fileSum, err := r.StreamFile(sid, it.snapPath, 0, io.Discard)
			if err != nil {
				return fmt.Errorf("计算 flist 校验和 %s: %w", it.entry.Path, err)
			}
			buf.Write(fileSum[:])
		}
		if err := out.WriteData(buf.Bytes()); err != nil {
			return err
		}
	}
	{
		var buf bytesBuffer
		if err := fw.WriteEndOfFlist(&buf); err != nil {
			return err
		}
		// id list：!numeric_ids 且 preserve 开启时各发一个空段（send_id_lists，
		// uidlist.c:407-429；daemon 侧 numeric_ids 调整值 ≤0 同样发送）
		if !neg.NumericIDs {
			if neg.PreserveUID {
				if err := WriteIdList(&buf); err != nil {
					return err
				}
			}
			if neg.PreserveGID {
				if err := WriteIdList(&buf); err != nil {
					return err
				}
			}
		}
		if err := out.WriteData(buf.Bytes()); err != nil {
			return err
		}
	}

	// 传输循环（send_files，sender.c:197-461）：响应客户端 generator 的每条消息。
	// DONE 计数：phase>2（收到第 3 个 DONE）跳出循环，前两个各回 ACK。
	phase := 0
	var totalSize int64
	var filesSent int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ndx, err := ndxIn.Read(stream)
		if err != nil {
			return fmt.Errorf("读 ndx: %w", err)
		}
		switch {
		case ndx == ndxDone:
			phase++
			if phase > 2 {
				goto transferDone
			}
			if err := writeNdxDone(out, ndxOut); err != nil {
				return err
			}
			continue
		case ndx == ndxDelStats:
			// 客户端 generator 发删除统计（read_del_stats/write_del_stats，
			// main.c:225-245；am_sender&&am_server 回写原值）
			if err := handleDelStats(stream, out, ndxOut); err != nil {
				return err
			}
			continue
		case ndx == ndxFlistEOF:
			return fmt.Errorf("非增量模式下不应出现 NDX_FLIST_EOF")
		case ndx < 0:
			return fmt.Errorf("非法 ndx: %d", ndx)
		}
		if int(ndx) >= len(items) {
			return fmt.Errorf("ndx %d 超出 flist 范围 %d", ndx, len(items))
		}
		it := items[ndx]
		e := it.entry
		curPath = e.Path

		// ndx 之后：iflags（shortint）+ 可选 basis 类型字节 + 可选 xname vstring
		iflags, err := ReadShortint(stream)
		if err != nil {
			return fmt.Errorf("读 iflags: %w", err)
		}
		var basis byte
		if iflags&itemBasisTypeFollows != 0 {
			if basis, err = readByte(stream); err != nil {
				return fmt.Errorf("读 basis: %w", err)
			}
		}
		var xname []byte
		if iflags&itemXnameFollows != 0 {
			if xname, err = readVstring(stream); err != nil {
				return fmt.Errorf("读 xname: %w", err)
			}
		}

		// 非传输条目（目录/符号链接/仅属性）：回显 ndx+iflags(+跟随位)（sender.c:285-289）
		if iflags&itemTransfer == 0 {
			attrs := []any{"path", e.Path}
			switch {
			case e.IsDir:
				attrs = append(attrs, "type", "dir")
			case e.IsSymlink:
				attrs = append(attrs, "type", "link", "target", e.LinkTarget)
			}
			logger.Info("entry_sent", attrs...)
			if err := writeNdxAttrs(out, ndxOut, int32(ndx), iflags, basis, xname); err != nil {
				return err
			}
			continue
		}
		if !e.IsSymlink && !e.IsDir && !isRegularMode(e.Mode) {
			return fmt.Errorf("请求传输非普通文件: %s", e.Path)
		}
		if e.IsDir {
			return fmt.Errorf("请求传输目录: %s", e.Path)
		}

		// 传输请求：读 sum_head + 块校验和（v1 全部丢弃；接收侧读掉才能保持流同步）
		sum, err := readSumHead(stream)
		if err != nil {
			return fmt.Errorf("读校验和头 %s: %w", e.Path, err)
		}
		if err := discardSums(stream, sum); err != nil {
			return fmt.Errorf("读块校验和 %s: %w", e.Path, err)
		}

		// 回显 ndx+iflags(+basis/xname)，再回显 sum_head（write_ndx_and_attrs +
		// write_sum_head，sender.c:409-410），然后 token 流 + 结束 0 + 整文件校验和
		if err := writeNdxAttrs(out, ndxOut, int32(ndx), iflags, basis, xname); err != nil {
			return err
		}
		var buf bytesBuffer
		if err := writeSumHead(&buf, sum); err != nil {
			return err
		}
		if err := out.WriteData(buf.Bytes()); err != nil {
			return err
		}

		lw := &literalWriter{out: out}
		fileStart := time.Now()
		n, fileSum, err := r.StreamFile(sid, it.snapPath, neg.ChecksumSeed, lw)
		if err != nil {
			return fmt.Errorf("读取文件 %s: %w", e.Path, err)
		}
		totalSize += n
		filesSent++
		logger.Info("file_sent", "path", e.Path, "size", n,
			"chunks", (n+int64(r.ChunkSizeBytes())-1)/int64(r.ChunkSizeBytes()),
			"elapsed_ms", time.Since(fileStart).Milliseconds())
		// 文件数据结束标记 int32 0（token.c:319），随后整文件强校验和（xfer_sum_len=16）
		if err := out.WriteData([]byte{0, 0, 0, 0}); err != nil {
			return err
		}
		if err := writeFileSum(out, fileSum); err != nil {
			return err
		}
	}

transferDone:
	// send_files 尾部：write_ndx(NDX_DONE)（sender.c:460）
	if err := writeNdxDone(out, ndxOut); err != nil {
		return err
	}
	// stats：5×varlong30(3)（handle_stats，main.c:325-380；值仅用于客户端显示）
	{
		var buf bytesBuffer
		if err := WriteVarlong(&buf, 0, 3); err != nil { // total_read（粗略）
			return err
		}
		if err := WriteVarlong(&buf, 0, 3); err != nil { // total_written（粗略）
			return err
		}
		if err := WriteVarlong(&buf, totalSize, 3); err != nil {
			return err
		}
		if err := WriteVarlong(&buf, 1, 3); err != nil { // flist_buildtime
			return err
		}
		if err := WriteVarlong(&buf, 0, 3); err != nil { // flist_xfertime
			return err
		}
		if err := out.WriteData(buf.Bytes()); err != nil {
			return err
		}
	}

	// read_final_goodbye（main.c:873-904）：读 ndx——期间可能收到 NDX_DEL_STATS
	// （read_ndx_and_attrs 的 ndx 循环内部处理：读 5 varint 并回写后继续，
	// rsync.c:338-340），直到 NDX_DONE：回写 DONE 后再读一次（协议 31
	// double-read），两次都必须是 DONE，否则非法结束包。
	for i := 0; i < 2; i++ {
		for {
			v, err := ndxIn.Read(stream)
			if err != nil {
				return fmt.Errorf("read_final_goodbye: %w", err)
			}
			if v == ndxDelStats {
				if err := handleDelStats(stream, out, ndxOut); err != nil {
					return err
				}
				continue
			}
			if v != ndxDone {
				return fmt.Errorf("会话结束期待 NDX_DONE，收到 %d", v)
			}
			break
		}
		if i == 0 {
			if err := writeNdxDone(out, ndxOut); err != nil {
				return err
			}
		}
	}
	logger.Info("session_done", "dir", "restore", "files", filesSent, "bytes", totalSize,
		"elapsed_ms", time.Since(started).Milliseconds())
	return nil
}

// handleDelStats 读删除统计（read_del_stats，main.c:240-245：5 个 varint）并回写
// 原值（write_del_stats，main.c:225-238：ndx(-3) + 5 个 varint）。
func handleDelStats(stream *MuxStream, out *MuxWriter, ndxOut *ndxCodec) error {
	var stats [5]int32
	for i := range stats {
		v, err := ReadVarint(stream)
		if err != nil {
			return fmt.Errorf("读删除统计: %w", err)
		}
		stats[i] = v
	}
	var buf bytesBuffer
	if err := ndxOut.Write(&buf, ndxDelStats); err != nil {
		return err
	}
	for _, v := range stats {
		if err := WriteVarint(&buf, v); err != nil {
			return err
		}
	}
	return out.WriteData(buf.Bytes())
}

// sumHead 一次 sum_head（io.c:1964-1991）：count/blength/s2length/remainder。
type sumHead struct {
	count, blength, s2length, remainder int32
}

// readSumHead 读 sum_head 的 4 个 int32。
func readSumHead(r io.Reader) (sumHead, error) {
	var s sumHead
	var err error
	if s.count, err = ReadInt32(r); err != nil {
		return s, err
	}
	if s.blength, err = ReadInt32(r); err != nil {
		return s, err
	}
	if s.s2length, err = ReadInt32(r); err != nil {
		return s, err
	}
	if s.remainder, err = ReadInt32(r); err != nil {
		return s, err
	}
	return s, nil
}

// writeSumHead 回显 sum_head（原值写回）。
func writeSumHead(w io.Writer, s sumHead) error {
	for _, v := range [...]int32{s.count, s.blength, s.s2length, s.remainder} {
		if err := WriteInt32(w, v); err != nil {
			return err
		}
	}
	return nil
}

// discardSums 读掉 count 块校验和（每块 sum1 int32 + sum2 s2length 字节）。
func discardSums(r io.Reader, s sumHead) error {
	if s.count <= 0 || s.count > 1<<20 {
		return nil // 无块或非法（防御）
	}
	if s.s2length < 0 || s.s2length > 1<<16 {
		return fmt.Errorf("非法 s2length: %d", s.s2length)
	}
	// 逐块读，避免超大一次性分配
	for i := int32(0); i < s.count; i++ {
		var b [4]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return err
		}
		if s.s2length > 0 {
			if _, err := io.CopyN(io.Discard, r, int64(s.s2length)); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeNdxAttrs 回显 ndx+iflags(+basis 类型字节/+xname vstring)（sender.c:180-200，
// write_ndx_and_attrs：条件位与接收时一致，原样回发）。
func writeNdxAttrs(out *MuxWriter, ndx *ndxCodec, index int32, iflags uint16, basis byte, xname []byte) error {
	var buf bytesBuffer
	if err := ndx.Write(&buf, index); err != nil {
		return err
	}
	if err := WriteShortint(&buf, iflags); err != nil {
		return err
	}
	if iflags&itemBasisTypeFollows != 0 {
		if err := writeByte(&buf, basis); err != nil {
			return err
		}
	}
	if iflags&itemXnameFollows != 0 {
		if _, err := buf.Write(xname); err != nil {
			return err
		}
	}
	return out.WriteData(buf.Bytes())
}

// readVstring 读 vstring（io.c:1943-1957）：1-2 字节长度前缀 + 数据。
// 返回原始字节（含前缀），供回显原样写出。
func readVstring(r io.Reader) ([]byte, error) {
	first, err := readByte(r)
	if err != nil {
		return nil, err
	}
	var n int
	if first&0x80 != 0 {
		second, err := readByte(r)
		if err != nil {
			return nil, err
		}
		n = int(first&0x7F)<<8 | int(second)
		raw := []byte{first, second}
		if n > 0 {
			data := make([]byte, n)
			if _, err := io.ReadFull(r, data); err != nil {
				return nil, err
			}
			raw = append(raw, data...)
		}
		return raw, nil
	}
	n = int(first)
	if n > 0 {
		data := make([]byte, n)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		return append([]byte{first}, data...), nil
	}
	return []byte{first}, nil
}

// literalWriter 把明文内容切成 ≤32KiB 的字面量 token 块写出（simple_send_token，
// token.c:304-319：int32 长度 + 原始字节）。文件结束标记 int32 0 由调用方追加。
type literalWriter struct {
	out *MuxWriter
}

func (w *literalWriter) Write(p []byte) (int, error) {
	orig := len(p)
	for len(p) > 0 {
		n := min(len(p), literalChunkSize)
		if err := writeLiteralBlock(w.out, p[:n]); err != nil {
			return 0, err
		}
		p = p[n:]
	}
	return orig, nil
}

func writeLiteralBlock(out *MuxWriter, data []byte) error {
	var buf bytesBuffer
	if err := WriteInt32(&buf, int32(len(data))); err != nil {
		return err
	}
	buf.Write(data)
	return out.WriteData(buf.Bytes())
}

// writeFileSum 写整文件强校验和（xfer_sum_len=16，md5）。
func writeFileSum(out *MuxWriter, sum [16]byte) error {
	return out.WriteData(sum[:])
}

// isRegularMode 判断 mode 是否普通文件（无 S_IFMT 类型位）。
func isRegularMode(mode uint32) bool {
	return mode&sIfmt == 0 || mode&sIfmt == sIfReg
}

// pathBase 返回路径最后一段（单文件拉取时条目名 = 文件名）。
func pathBase(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

// modulePrefix 从客户端模块路径参数解析快照内子路径前缀（glob_expand_module，
// util1.c:787 剥模块名前缀 + send_file_list 路径拆分）：
//
//	"mod/" / "mod" → ""（模块根，全量）；"mod/sub/" → "sub"；"mod/file.txt" → "file.txt"。
func modulePrefix(moduleArg, moduleName string) string {
	p := moduleArg
	if strings.HasPrefix(p, moduleName) && (len(p) == len(moduleName) || p[len(moduleName)] == '/') {
		p = p[len(moduleName):]
	}
	p = strings.Trim(p, "/")
	if p == "" {
		return ""
	}
	// 安全路径校验（与 cleanPath 一致：拒绝 ".." 段与绝对路径）
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return ""
		}
	}
	return p
}
