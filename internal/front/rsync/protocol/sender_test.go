// internal/front/rsync/protocol/sender_test.go
package protocol

import (
	"bytes"
	"context"
	"crypto/md5"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"crysync/internal/backend"
	"crysync/internal/config"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
	"crysync/internal/core/repo"
)

// seedSenderRepo 造一个含文件/目录/符号链接的快照（供 sender 会话测试）。
func seedSenderRepo(t *testing.T) (*repo.Repo, int64) {
	t.Helper()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	key, _ := crypto.GenerateKey()
	r := repo.New(db, backend.NewInMemory(), key, 64)

	txn, err := r.BeginSnapshot(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	add := func(path string, data []byte, mode uint32, mtimeNs int64) {
		t.Helper()
		var refs []meta.ChunkRef
		idx := 0
		if len(data) > 0 {
			id, _, err := r.StoreChunk(data)
			if err != nil {
				t.Fatal(err)
			}
			refs = []meta.ChunkRef{{ChunkID: id, IDX: idx}}
			idx++
		}
		if err := txn.UpsertFile(meta.FileRow{Path: path, Mode: mode, Size: int64(len(data)), MTimeNs: mtimeNs}, refs); err != nil {
			t.Fatal(err)
		}
	}
	add("a.txt", []byte("hello restore"), 0o644, 1700000000*1e9+123)
	add("sub/b.txt", []byte("second file content"), 0o600, 1700000001*1e9)
	if err := txn.UpsertFile(meta.FileRow{Path: "sub", IsDir: true, Mode: 0o40755, MTimeNs: 1700000000 * 1e9}, nil); err != nil {
		t.Fatal(err)
	}
	if err := txn.UpsertFile(meta.FileRow{Path: "link1", IsSymlink: true, Mode: 0o120777, MTimeNs: 1700000002 * 1e9, LinkTarget: "a.txt"}, nil); err != nil {
		t.Fatal(err)
	}
	sid, err := txn.Commit()
	if err != nil {
		t.Fatal(err)
	}
	return r, sid
}

// senderClient 在内存 pipe 上扮演 rsync 客户端（receiver+generator），
// 驱动服务端 sender 会话并逐字节验证协议。
type senderClient struct {
	t      *testing.T
	mr     *MuxReader
	mw     *MuxWriter
	stream *MuxStream
	ndxIn  *ndxCodec // 读服务端（S->C）
	ndxOut *ndxCodec // 写请求（C->S）
}

func newSenderClient(t *testing.T, conn net.Conn) *senderClient {
	t.Helper()
	mr, _ := NewMuxReader(conn)
	mw := NewMuxWriter(conn)
	return &senderClient{t: t, mr: mr, mw: mw, stream: NewMuxStream(mr), ndxIn: newNdxCodec(), ndxOut: newNdxCodec()}
}

// sendFilter 发空 filter 列表（int32 0）。
func (c *senderClient) sendFilter() {
	c.t.Helper()
	if err := c.mw.WriteData([]byte{0, 0, 0, 0}); err != nil {
		c.t.Fatal(err)
	}
}

// readFlist 读 flist 直到哨兵，返回条目列表。
func (c *senderClient) readFlist() []FileEntry {
	c.t.Helper()
	p := NewFlistParser()
	var out []FileEntry
	for {
		e, err := p.Parse(c.stream, true, true, true, false, false, false)
		if err == ErrFlistEnd {
			return out
		}
		if err != nil {
			c.t.Fatalf("解析 flist: %v", err)
		}
		out = append(out, e)
	}
}

// readIdList 读一个 id list 空段（varint 0）。
func (c *senderClient) readIdList() {
	c.t.Helper()
	v, err := ReadVarint(c.stream)
	if err != nil || v != 0 {
		c.t.Fatalf("id list 应为空段: %d %v", v, err)
	}
}

// requestTransfer 发送传输请求：ndx + iflags + sum_head（count=0 全零）。
func (c *senderClient) requestTransfer(ndx int32, iflags uint16) {
	c.t.Helper()
	var buf bytesBuffer
	if err := c.ndxOut.Write(&buf, ndx); err != nil {
		c.t.Fatal(err)
	}
	if err := WriteShortint(&buf, iflags); err != nil {
		c.t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := WriteInt32(&buf, 0); err != nil {
			c.t.Fatal(err)
		}
	}
	if err := c.mw.WriteData(buf.Bytes()); err != nil {
		c.t.Fatal(err)
	}
}

// requestItemize 发送仅 itemize 条目（无 sums）：ndx + iflags。
func (c *senderClient) requestItemize(ndx int32, iflags uint16) {
	c.t.Helper()
	var buf bytesBuffer
	if err := c.ndxOut.Write(&buf, ndx); err != nil {
		c.t.Fatal(err)
	}
	if err := WriteShortint(&buf, iflags); err != nil {
		c.t.Fatal(err)
	}
	if err := c.mw.WriteData(buf.Bytes()); err != nil {
		c.t.Fatal(err)
	}
}

// requestWithAttrs 发送带 basis 类型字节与 xname vstring 的请求
// （write_ndx_and_attrs 语义：ITEM_BASIS_TYPE_FOLLOWS/ITEM_XNAME_FOLLOWS 跟随位）。
func (c *senderClient) requestWithAttrs(ndx int32, iflags uint16, basis byte, xname string) {
	c.t.Helper()
	var buf bytesBuffer
	if err := c.ndxOut.Write(&buf, ndx); err != nil {
		c.t.Fatal(err)
	}
	if err := WriteShortint(&buf, iflags); err != nil {
		c.t.Fatal(err)
	}
	if iflags&itemBasisTypeFollows != 0 {
		if err := writeByte(&buf, basis); err != nil {
			c.t.Fatal(err)
		}
	}
	if iflags&itemXnameFollows != 0 {
		// vstring：1-2 字节长度前缀（io.c write_vstring）
		if len(xname) > 0x7F {
			c.t.Fatalf("测试 xname 过长")
		}
		if err := writeByte(&buf, byte(len(xname))); err != nil {
			c.t.Fatal(err)
		}
		if _, err := buf.Write([]byte(xname)); err != nil {
			c.t.Fatal(err)
		}
	}
	if err := c.mw.WriteData(buf.Bytes()); err != nil {
		c.t.Fatal(err)
	}
}

// readEchoAttrs 读回显的 ndx+iflags+basis+xname（有跟随位时）。
func (c *senderClient) readEchoAttrs(wantBasis byte, wantXname string) (int32, uint16) {
	c.t.Helper()
	ndx, iflags := c.readEcho()
	if iflags&itemBasisTypeFollows != 0 {
		b, err := readByte(c.stream)
		if err != nil || b != wantBasis {
			c.t.Fatalf("basis 回显不符: %x %v", b, err)
		}
	}
	if iflags&itemXnameFollows != 0 {
		raw, err := readVstring(c.stream)
		if err != nil {
			c.t.Fatal(err)
		}
		if got := string(raw[len(raw)-len(wantXname):]); got != wantXname {
			c.t.Fatalf("xname 回显不符: %q want %q", got, wantXname)
		}
	}
	return ndx, iflags
}

// readEcho 读回显的 ndx+iflags（write_ndx_and_attrs，无 basis/xname）。
func (c *senderClient) readEcho() (int32, uint16) {
	c.t.Helper()
	ndx, err := c.ndxIn.Read(c.stream)
	if err != nil {
		c.t.Fatal(err)
	}
	iflags, err := ReadShortint(c.stream)
	if err != nil {
		c.t.Fatal(err)
	}
	return ndx, iflags
}

// readFileData 读文件响应：sum_head 回显 + token 流 + 结束 0 + 校验和。
// 返回（sum_head, 内容, 校验和）。
func (c *senderClient) readFileData(seed int32) (sumHead, []byte, [16]byte) {
	c.t.Helper()
	sum, err := readSumHead(c.stream)
	if err != nil {
		c.t.Fatal(err)
	}
	var data bytes.Buffer
	for {
		token, err := ReadInt32(c.stream)
		if err != nil {
			c.t.Fatal(err)
		}
		if token == 0 {
			break
		}
		if token < 0 {
			c.t.Fatalf("v1 不应收到 match token: %d", token)
		}
		b := make([]byte, token)
		if _, err := io.ReadFull(c.stream, b); err != nil {
			c.t.Fatal(err)
		}
		data.Write(b)
	}
	var sumBuf [16]byte
	if _, err := io.ReadFull(c.stream, sumBuf[:]); err != nil {
		c.t.Fatal(err)
	}
	// 整文件强校验和 = 纯 MD5(content)（sum_end，seed 对 MD5 无效）
	want := md5.Sum(data.Bytes())
	if sumBuf != want {
		c.t.Fatalf("文件校验和不符: got %x want %x", sumBuf, want)
	}
	return sum, data.Bytes(), sumBuf
}

// sendDones 发送 n 个 NDX_DONE：前 2 个各读回 ACK，第 3 个只发不读
// （send_files 收到第 3 个 DONE 即 break，无 ACK；随后立即发尾部 DONE+stats，
// 该读留给 readTail）。
func (c *senderClient) sendDones(n int) int {
	c.t.Helper()
	ack := 0
	for i := 0; i < n; i++ {
		if err := c.mw.WriteData([]byte{0}); err != nil {
			c.t.Fatal(err)
		}
		if i < 2 {
			v, err := c.ndxIn.Read(c.stream)
			if err != nil {
				c.t.Fatal(err)
			}
			if v != ndxDone {
				c.t.Fatalf("期待 NDX_DONE ACK，收到 %d", v)
			}
			ack++
		}
	}
	return ack
}

// readTail 读会话尾部：DONE + stats（5×varlong30）。
func (c *senderClient) readTail() {
	c.t.Helper()
	v, err := c.ndxIn.Read(c.stream)
	if err != nil || v != ndxDone {
		c.t.Fatalf("尾部应收到 NDX_DONE: %d %v", v, err)
	}
	for i := 0; i < 5; i++ {
		if _, err := ReadVarlong(c.stream, 3); err != nil {
			c.t.Fatalf("读 stats: %v", err)
		}
	}
}

// finalGoodbye 走 read_final_goodbye 的双向交换：发 G4（main.c:1119 最终 goodbye）
// -> 读服务端 S4 ACK -> 发 G5（服务端读 G5 后完成，无回写）。
func (c *senderClient) finalGoodbye() {
	c.t.Helper()
	if err := c.mw.WriteData([]byte{0}); err != nil {
		c.t.Fatal(err)
	}
	v, err := c.ndxIn.Read(c.stream)
	if err != nil || v != ndxDone {
		c.t.Fatalf("G4 后应收到 S4 ACK: %d %v", v, err)
	}
	if err := c.mw.WriteData([]byte{0}); err != nil {
		c.t.Fatal(err)
	}
}

func TestSenderSession(t *testing.T) {
	r, sid := seedSenderRepo(t)
	connS, connC := net.Pipe()
	defer connS.Close()
	defer connC.Close()

	module := &config.ModuleConfig{Name: "home"}
	neg := &Negotiation{PreserveUID: true, PreserveGID: true, PreserveLinks: true, Recurse: true, ChecksumSeed: 42}
	done := make(chan error, 1)
	go func() {
		done <- processSendSession(context.Background(), mustMux(connS), mustMuxW(connS), module, r, neg, nil)
	}()

	c := newSenderClient(t, connC)
	c.sendFilter()
	entries := c.readFlist()
	if len(entries) != 4 {
		t.Fatalf("flist 应含 4 条: %d", len(entries))
	}
	// 排序：a.txt(0), link1(1), sub(2), sub/b.txt(3)
	if entries[0].Path != "a.txt" || entries[1].Path != "link1" ||
		entries[2].Path != "sub" || entries[3].Path != "sub/b.txt" {
		t.Fatalf("flist 排序不符: %+v", entries)
	}
	c.readIdList() // uid
	c.readIdList() // gid

	// 文件传输：a.txt
	c.requestTransfer(0, itemTransfer|itemIsNew)
	ndx, iflags := c.readEcho()
	if ndx != 0 || iflags != itemTransfer|itemIsNew {
		t.Fatalf("回显不符: %d %x", ndx, iflags)
	}
	sum, data, _ := c.readFileData(42)
	if sum.count != 0 || !bytes.Equal(data, []byte("hello restore")) {
		t.Fatalf("a.txt 数据不符: %q %+v", data, sum)
	}

	// 目录/链接 itemize：sub(2)、link1(1)（不传输）
	for _, tc := range []struct {
		ndx    int32
		iflags uint16
	}{
		{2, itemIsNew},
		{1, itemIsNew | 0x4000}, // ITEM_LOCAL_CHANGE
	} {
		c.requestItemize(tc.ndx, tc.iflags)
		ndx, iflags := c.readEcho()
		if ndx != tc.ndx || iflags != tc.iflags {
			t.Fatalf("itemize 回显不符: %d %x", ndx, iflags)
		}
	}

	// basis/xname 跟随位：请求带 ITEM_BASIS_TYPE_FOLLOWS + 字节 + ITEM_XNAME_FOLLOWS +
	// vstring，服务端必须原样回显（增量恢复时客户端对本地已有同名文件会带 basis）
	c.requestWithAttrs(2, itemIsNew|itemBasisTypeFollows|itemXnameFollows, 0x80, "some-xname")
	ndx2, iflags2 := c.readEchoAttrs(0x80, "some-xname")
	if ndx2 != 2 || iflags2 != itemIsNew|itemBasisTypeFollows|itemXnameFollows {
		t.Fatalf("basis 请求回显不符: %d %x", ndx2, iflags2)
	}

	// 第二个文件：sub/b.txt
	c.requestTransfer(3, itemTransfer|itemIsNew)
	ndx, iflags = c.readEcho()
	if ndx != 3 {
		t.Fatalf("回显 ndx 不符: %d", ndx)
	}
	_, data, _ = c.readFileData(42)
	if !bytes.Equal(data, []byte("second file content")) {
		t.Fatalf("b.txt 数据不符: %q", data)
	}

	// DONE 序列：前 2 个 ACK，第 3 个 break
	if ack := c.sendDones(3); ack != 2 {
		t.Fatalf("前 2 个 DONE 应各回 ACK: %d", ack)
	}
	c.readTail()

	// read_final_goodbye：G4 回 ACK，G5 无
	c.finalGoodbye()

	if err := <-done; err != nil {
		t.Fatalf("sender 会话失败: %v", err)
	}
	_ = sid
}

func TestSenderSessionDelStats(t *testing.T) {
	r, _ := seedSenderRepo(t)
	connS, connC := net.Pipe()
	defer connS.Close()
	defer connC.Close()

	module := &config.ModuleConfig{Name: "home"}
	neg := &Negotiation{ChecksumSeed: 0, PreserveUID: true, PreserveLinks: true, Recurse: true}
	done := make(chan error, 1)
	go func() {
		done <- processSendSession(context.Background(), mustMux(connS), mustMuxW(connS), module, r, neg, nil)
	}()

	c := newSenderClient(t, connC)
	c.sendFilter()
	c.readFlist()
	c.readIdList() // uid（preserve_uid=true 且非 numeric）

	// NDX_DEL_STATS：ndx(-3) + 5 varint，服务端应回显原值
	var buf bytesBuffer
	c.ndxOut.Write(&buf, ndxDelStats)
	for _, v := range []int32{10, 2, 1, 0, 0} {
		WriteVarint(&buf, v)
	}
	if err := c.mw.WriteData(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	ndx, err := c.ndxIn.Read(c.stream)
	if err != nil || ndx != ndxDelStats {
		t.Fatalf("应回显 NDX_DEL_STATS: %d %v", ndx, err)
	}
	for _, want := range []int32{10, 2, 1, 0, 0} {
		got, err := ReadVarint(c.stream)
		if err != nil || got != want {
			t.Fatalf("删除统计回显不符: got %d want %d (%v)", got, want, err)
		}
	}

	if ack := c.sendDones(3); ack != 2 {
		t.Fatalf("DONE ACK 数不符: %d", ack)
	}
	c.readTail()
	c.finalGoodbye()
	if err := <-done; err != nil {
		t.Fatalf("sender 会话失败: %v", err)
	}
}

func mustMux(conn net.Conn) *MuxReader {
	mr, _ := NewMuxReader(conn)
	return mr
}

func mustMuxW(conn net.Conn) *MuxWriter {
	return NewMuxWriter(conn)
}

// modulePrefix 解析客户端模块路径参数（glob_expand_module 语义：剥模块名前缀，
// 尾部斜杠不影响过滤结果）。
func TestModulePrefix(t *testing.T) {
	for _, tc := range []struct {
		arg, module, want string
	}{
		{"mod/", "mod", ""},        // 模块根 → 全量
		{"mod", "mod", ""},         // 无斜杠模块名
		{"mod/sub/", "mod", "sub"}, // 子目录（尾斜杠）
		{"mod/sub", "mod", "sub"},  // 子目录（无尾斜杠）
		{"mod/file.txt", "mod", "file.txt"},
		{"mod/a/b/c.txt", "mod", "a/b/c.txt"},
		{"other/", "mod", "other"}, // 与模块名不匹配时不剥（防御）
	} {
		if got := modulePrefix(tc.arg, tc.module); got != tc.want {
			t.Fatalf("modulePrefix(%q, %q): got %q want %q", tc.arg, tc.module, got, tc.want)
		}
	}
}
