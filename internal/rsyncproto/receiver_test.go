package rsyncproto

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"crysync/internal/backend"
	"crysync/internal/config"
	"crysync/internal/crypto"
	"crysync/internal/meta"
	"crysync/internal/repo"
)

// buildEntry 构造一条旧式编码的 flist 记录（xfer_flags_as_varint=0）。
// SAME_TIME|SAME_UID|SAME_GID 置位（省略字段），mode 恒发送。
func buildEntry(t *testing.T, name string, mode uint32, size int32, isDir bool, linkTarget string) []byte {
	t.Helper()
	var buf bytes.Buffer
	xflags := XmitSameTime | XmitSameUID | XmitSameGID
	if isDir {
		xflags |= XmitNoContentDir
		mode |= 0o040000 // S_IFDIR
	}
	writeXflags := xflags
	if xflags&0xFF00 != 0 || xflags == 0 {
		writeXflags |= XmitExtended
		WriteShortint(&buf, uint16(writeXflags))
	} else {
		buf.WriteByte(byte(writeXflags))
	}
	buf.WriteByte(byte(len(name)))
	buf.WriteString(name)
	WriteVarlong30(&buf, size)
	WriteInt32(&buf, int32(mode)) // SAME_MODE 未置位
	if linkTarget != "" {
		WriteVarint(&buf, int32(len(linkTarget))) // symlink 长度 varint30（flist.c:640）
		buf.WriteString(linkTarget)
	}
	return buf.Bytes()
}

func newTestRepoFull(t *testing.T) (*repo.Repo, *config.ModuleConfig) {
	t.Helper()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	key, _ := crypto.GenerateKey()
	be := backend.NewInMemory()
	r := repo.New(db, be, key, 64)
	m := &config.ModuleConfig{Name: "home", Path: "/"}
	return r, m
}

// pipeConn：真实 TCP 连接对。client 端写 input、持续消费服务端输出。
func pipeConn(t *testing.T, input []byte) (net.Conn, *bytes.Buffer) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()
	var serverOut bytes.Buffer
	done := make(chan struct{})
	t.Cleanup(func() {
		client.Close()
		server.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	})
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				serverOut.Write(buf[:n])
			}
			if err != nil {
				break
			}
		}
		close(done)
	}()
	go func() {
		client.Write(input)
	}()
	return server, &serverOut
}

// buildSessionInput 组装一次完整会话输入（客户端 -> daemon 方向，新协议模型）：
// argv（NUL 分隔）-> mux 数据帧[flist 记录+哨兵 -> id list -> 逐条响应（回显/token 流）-> goodbye ACK x3]。
// 目录/链接条目响应：回显 ndx+iflags；文件条目响应：回显 ndx+iflags + 回显 sum_head(16B) +
// literal token 流 + 0 + 16 字节整文件校验和。
func buildSessionInput(t *testing.T, entries [][]byte, contents [][]byte) []byte {
	t.Helper()
	var pre bytes.Buffer
	// 典型 -a 推送 argv（含 'o''g' -> preserve uid/gid，因此需要 id list）
	pre.WriteString("--server\x00--sender\x00-vlogDtpre.iLsfxCIvu\x00.\x00home/\x00\x00")

	var payload bytes.Buffer
	// flist：逐条记录 + 单字节 0 哨兵（无 count 头）
	for _, e := range entries {
		payload.Write(e)
	}
	payload.WriteByte(0)
	// id list：preserve uid/gid 各一段（空段 = 单个 varint 0）
	payload.WriteByte(0)
	payload.WriteByte(0)
	// 传输阶段逐条响应（模拟客户端 sender；ndx 回显为递增序列，每条差分编码 [01]）
	for _, c := range contents {
		payload.WriteByte(0x01) // 客户端 write_ndx(i)：prev=-1 起点，diff=1
		if c == nil {
			// 目录/链接：iflags 回显 = 服务端发的 itemIsNew
			WriteShortint(&payload, itemIsNew)
			continue
		}
		// 文件：iflags 回显 = itemTransfer|itemIsNew，回显 sum_head 16 字节 0
		WriteShortint(&payload, itemTransfer|itemIsNew)
		for k := 0; k < 4; k++ {
			WriteInt32(&payload, 0)
		}
		// token 流：literal（正长度 + 字节）+ 0 结束
		WriteInt32(&payload, int32(len(c)))
		payload.Write(c)
		WriteInt32(&payload, 0)
		// 整文件强校验和（md5 = 16 字节）
		payload.Write(make([]byte, 16))
	}
	// goodbye：客户端对 DONE#1/#2/#4 回 ACK（NDX_DONE = 单字节 0x00）
	payload.WriteByte(0)
	payload.WriteByte(0)
	payload.WriteByte(0)

	pre.Write(muxDataFrame(t, payload.Bytes()))
	return pre.Bytes()
}

// muxDataFrame：把数据包成 mux 数据帧（帧头小端 [len 低 24 位][tag=MPLEX_BASE+MSG_DATA]）。
func muxDataFrame(t *testing.T, data []byte) []byte {
	t.Helper()
	var hdr [4]byte
	hdr[0] = byte(len(data))
	hdr[1] = byte(len(data) >> 8)
	hdr[2] = byte(len(data) >> 16)
	hdr[3] = 7 // MPLEX_BASE + MSG_DATA
	return append(hdr[:], data...)
}

// TestReceiverDirsAndFiles：目录 + 符号链接 + 两个文件（内容传输）
func TestReceiverDirsAndFiles(t *testing.T) {
	r, m := newTestRepoFull(t)
	entries := [][]byte{
		buildEntry(t, "sub/", 0o755, 0, true, ""),
		buildEntry(t, "link1", 0o120000|0o777, 0, false, "target"),
		buildEntry(t, "a.txt", 0o644, 5, false, ""),
		buildEntry(t, "b.txt", 0o644, 3, false, ""),
	}
	// 传输阶段按 f_name_cmp 排序后顺序（generator.c:2316-2322）：a.txt、b.txt、link1、sub
	contents := [][]byte{[]byte("hello"), []byte("abc"), nil, nil}
	input := buildSessionInput(t, entries, contents)

	conn, serverOut := pipeConn(t, input)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := RunReceiver(ctx, bufio.NewReader(conn), conn, m, r)
	if err != nil {
		t.Logf("daemon 输出: % x", serverOut.Bytes())
		t.Fatalf("receiver: %v", err)
	}
	sid, _ := r.LatestSnapshotID()
	files, err := r.GetFilesForTest(sid)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range files {
		got[f.Path] = true
	}
	for _, want := range []string{"sub", "link1", "a.txt", "b.txt"} {
		if !got[want] {
			t.Fatalf("缺少 %s，快照内容: %v", want, files)
		}
	}
	var out bytes.Buffer
	if err := r.ReadFile(sid, "a.txt", &out); err != nil || out.String() != "hello" {
		t.Fatalf("读取 a.txt: %v %q", err, out.String())
	}
	out.Reset()
	if err := r.ReadFile(sid, "b.txt", &out); err != nil || out.String() != "abc" {
		t.Fatalf("读取 b.txt: %v %q", err, out.String())
	}
}

// TestReceiverTopDirEntry：顶层目录 "." 条目（cleanPath 拒绝）不落库但协议交互完整
func TestReceiverTopDirEntry(t *testing.T) {
	r, m := newTestRepoFull(t)
	entries := [][]byte{
		buildEntry(t, ".", 0o755, 0, true, ""), // 顶层目录
		buildEntry(t, "a.txt", 0o644, 2, false, ""),
	}
	contents := [][]byte{nil, []byte("hi")}
	input := buildSessionInput(t, entries, contents)

	conn, _ := pipeConn(t, input)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := RunReceiver(ctx, bufio.NewReader(conn), conn, m, r); err != nil {
		t.Fatalf("receiver: %v", err)
	}
	sid, _ := r.LatestSnapshotID()
	files, err := r.GetFilesForTest(sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "a.txt" {
		t.Fatalf("顶层目录不应落库，快照内容: %v", files)
	}
}
