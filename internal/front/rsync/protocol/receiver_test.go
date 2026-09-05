package protocol

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crysync/internal/backend"
	"crysync/internal/config"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
	"crysync/internal/core/repo"
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

// newTestRepoLarge 同 newTestRepoFull 但用生产规模分块（4MiB）。
// 原因：CalcSizes 按 rsync SUM_CHUNK=700 定 blength，而 newTestRepoFull 的
// 64B 分块 < blength，delta 路径 match 块会超出服务端一块缓冲（生产 4MiB
// 远大于 700，无此问题）。仅 delta 测试用，全量/quick check 测试保持 64B，
// 以沿用既有去重分块语义。
func newTestRepoLarge(t *testing.T) (*repo.Repo, *config.ModuleConfig) {
	t.Helper()
	db, err := meta.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	key, _ := crypto.GenerateKey()
	be := backend.NewInMemory()
	r := repo.New(db, be, key, 4<<20)
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
	return buildSessionInputSum(t, entries, contents, -1)
}

// buildSessionInputSum 同 buildSessionInput，badSumIdx 指定的文件（contents 下标）
// 发坏整文件校验和（P1#9：客户端读源中途失败场景）；-1 表示全部正确。
func buildSessionInputSum(t *testing.T, entries [][]byte, contents [][]byte, badSumIdx int) []byte {
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
	for i, c := range contents {
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
		// 整文件强校验和：真实 MD5(内容)（坏校验和场景由 buildSessionInputSum 定制）
		sum := md5.Sum(c)
		if badSumIdx == i {
			sum[0] ^= 0xFF // 坏校验和（模拟客户端读源中途失败）
		}
		payload.Write(sum[:])
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
	err := RunReceiver(ctx, bufio.NewReader(conn), conn, m, r, nil)
	if err != nil {
		t.Logf("daemon 输出: % x", serverOut.Bytes())
		t.Fatalf("receiver: %v", err)
	}
	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range files {
		got[f.Path] = true
	}
	for _, want := range []string{"sub", "link1", "a.txt", "b.txt"} {
		if !got[want] {
			t.Fatalf("缺少 %s，清单内容: %v", want, files)
		}
	}
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "hello" {
		t.Fatalf("读取 a.txt: %v %q", err, out.String())
	}
	out.Reset()
	if err := r.ReadFile("b.txt", &out); err != nil || out.String() != "abc" {
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
	if err := RunReceiver(ctx, bufio.NewReader(conn), conn, m, r, nil); err != nil {
		t.Fatalf("receiver: %v", err)
	}
	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "a.txt" {
		t.Fatalf("顶层目录不应落库，清单内容: %v", files)
	}
}

// listenOnce 建立一对 TCP 连接（server 端 = RunReceiver 接受方，client 端 =
// 模拟 rsync 客户端）。先 Dial 再 Accept：与 pipeConn 相同，确保真实 TCP 上
// 已有连接在等 Accept（否则 Accept 阻塞无客户端连接而死锁）。
func listenOnce(t *testing.T) (net.Conn, net.Conn) {
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
	t.Cleanup(func() { server.Close(); client.Close() })
	return server, client
}

// runDeltaClient 交互式模拟 rsync 客户端（sender）：先发 pre（argv + mux
// 帧[flist+idlist]），然后逐帧读服务端输出：NDX_DONE 回 ACK（goodbye
// DONE#1/#2/#4 后各一次，共 3 个）；传输帧解析出 SumTable 后调用 respond
// 构造回显（ndx+iflags+sum_head 镜像 + token 流 + 16B 整文件 MD5）。所有
// 客户端输出字节经 resp 写出；返回 done 通道（会话正常结束回 nil）。
func runDeltaClient(t *testing.T, client net.Conn, pre []byte, respond func(tbl *SumTable, resp *bytes.Buffer) error) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		if _, err := client.Write(pre); err != nil {
			done <- err
			return
		}
		// 读服务端协商回显：compat_flags(varint 0，1B) + checksum_seed(int32，4B)
		negbuf := make([]byte, 5)
		if _, err := io.ReadFull(client, negbuf); err != nil {
			done <- err
			return
		}
		mr, err := NewMuxReader(client)
		if err != nil {
			done <- err
			return
		}
		ackCount := 0
		for {
			frame, tag, err := mr.Next()
			if err != nil {
				done <- err
				return
			}
			if tag != 0 {
				continue // 消息帧（MSG_ERROR_XFER 等）：跳过
			}
			if len(frame) == 1 && frame[0] == 0x00 { // NDX_DONE（write_ndx 30+ 编码单字节 0）
				ackCount++
				if ackCount == 1 || ackCount == 2 || ackCount == 4 {
					if _, err := client.Write(muxDataFrame(t, []byte{0x00})); err != nil {
						done <- err
						return
					}
				}
				if ackCount == 5 {
					done <- nil
					return
				}
				continue
			}
			// 传输帧：ndx(差分) + iflags(2B) + sum_head(16B) + 逐块 sum1+sum2
			br := bytes.NewReader(frame)
			if _, err := newNdxCodec().Read(br); err != nil { // 服务端 ndxOut 从 -1 起，差分首位必为 1
				done <- fmt.Errorf("解析 ndx: %w", err)
				return
			}
			if _, err := ReadShortint(br); err != nil {
				done <- err
				return
			}
			tbl, err := readSumTable(br)
			if err != nil {
				done <- err
				return
			}
			var resp bytes.Buffer
			if err := respond(&tbl, &resp); err != nil {
				done <- err
				return
			}
			if _, err := client.Write(muxDataFrame(t, resp.Bytes())); err != nil {
				done <- err
				return
			}
		}
	}()
	return done
}

// readSumTable 解析服务端发的 sum_head + 逐块校验和。
func readSumTable(r io.Reader) (SumTable, error) {
	var t SumTable
	var err error
	if t.Count, err = ReadInt32(r); err != nil {
		return t, err
	}
	if t.Blength, err = ReadInt32(r); err != nil {
		return t, err
	}
	if t.S2Length, err = ReadInt32(r); err != nil {
		return t, err
	}
	if t.Remainder, err = ReadInt32(r); err != nil {
		return t, err
	}
	if t.Count < 0 || t.Count > 1<<24 || t.Blength <= 0 || t.S2Length != 16 {
		return t, fmt.Errorf("非法 sum_head: %+v", t)
	}
	t.Sums = make([]BlockSum, t.Count)
	for i := range t.Sums {
		s1, err := ReadInt32(r)
		if err != nil {
			return t, err
		}
		t.Sums[i].Sum1 = uint32(s1)
		if _, err := io.ReadFull(r, t.Sums[i].Sum2[:]); err != nil {
			return t, err
		}
	}
	return t, nil
}

// buildDeltaResponse 构造客户端回显：ndx+iflags 回显 + sum_head 镜像 +
// token 流（match/literal 混合）+ 0 + 整文件 MD5（重组内容）。
func buildDeltaResponse(tbl *SumTable, tokens []int32, literals [][]byte, newContent []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(0x01) // 回显 ndx（差分 1）
	WriteShortint(&buf, itemTransfer|itemIsNew)
	for _, v := range [...]int32{tbl.Count, tbl.Blength, tbl.S2Length, tbl.Remainder} {
		WriteInt32(&buf, v)
	}
	li := 0
	for _, tok := range tokens {
		if tok > 0 {
			WriteInt32(&buf, tok)
			buf.Write(literals[li])
			li++
		} else {
			WriteInt32(&buf, tok) // match：负数原样（-1 = 块 0）
		}
	}
	WriteInt32(&buf, 0)
	buf.Write(newContentMD5(newContent)) // 整文件校验和：纯 MD5(内容)
	return buf.Bytes()
}

// newContentMD5 纯 MD5（sum_end 不混 seed）。
func newContentMD5(b []byte) []byte {
	h := md5.Sum(b)
	return h[:]
}

// TestReceiverQuickCheck：二次会话（flist 相同、mtime/size 不变）→ 服务端
// 不发任何传输请求，客户端只需回 goodbye ACK；当前清单内容保持不变。
func TestReceiverQuickCheck(t *testing.T) {
	r, m := newTestRepoFull(t)
	entry := buildEntry(t, "a.txt", 0o644, 5, false, "")
	// 第一次会话：全量备份（buildSessionInput 既有路径）
	conn, _ := pipeConn(t, buildSessionInput(t, [][]byte{entry}, [][]byte{[]byte("hello")}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := RunReceiver(ctx, bufio.NewReader(conn), conn, m, r, nil); err != nil {
		t.Fatalf("首次备份: %v", err)
	}
	cancel()

	// 第二次会话：同样 flist（SAME_TIME → MTimeNs=0 与库中一致）→ quick check 跳过
	conn2, _ := pipeConn(t, buildSessionInput(t, [][]byte{entry}, nil))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := RunReceiver(ctx2, bufio.NewReader(conn2), conn2, m, r, nil); err != nil {
		t.Fatalf("二次备份: %v", err)
	}
	// 内容仍在（quick check 不动当前状态），可读回
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || out.String() != "hello" {
		t.Fatalf("继承内容读取失败: %v %q", err, out.String())
	}
}

// TestReceiverDelta：文件变化 → 服务端发真实 sum_head+块校验和，客户端回
// match/literal 混合 token 流；重组内容正确。
func TestReceiverDelta(t *testing.T) {
	r, m := newTestRepoLarge(t)
	// 旧内容 1500B：blength=700 → 3 块（700/700/100）
	oldContent := bytes.Repeat([]byte{0x41}, 700)                       // 块 0
	oldContent = append(oldContent, bytes.Repeat([]byte{0x42}, 700)...) // 块 1
	oldContent = append(oldContent, bytes.Repeat([]byte{0x43}, 100)...) // 块 2（remainder）
	// 新内容 = 块 0 相同 + 插入 200B + 块 1 相同 + 末尾 100B 变化
	newContent := append(append([]byte{}, oldContent[:700]...), bytes.Repeat([]byte{0x5A}, 200)...)
	newContent = append(newContent, oldContent[700:1400]...)
	newContent = append(newContent, bytes.Repeat([]byte{0x44}, 100)...) // 末块内容变化

	// 第一次会话：全量备份旧内容（size=1500）
	oldEntry := buildEntry(t, "a.txt", 0o644, 1500, false, "")
	conn, _ := pipeConn(t, buildSessionInput(t, [][]byte{oldEntry}, [][]byte{oldContent}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := RunReceiver(ctx, bufio.NewReader(conn), conn, m, r, nil)
	cancel()
	if err != nil {
		t.Fatalf("首次备份: %v", err)
	}

	// 第二次会话：flist 中 a.txt（size=1700，mtime 变——SAME_TIME 语义下
	// 与首次同为 0，需改 size 触发 delta；size 不同 → quick check 不命中）
	newEntry := buildEntry(t, "a.txt", 0o644, 1700, false, "")
	var pre bytes.Buffer
	pre.WriteString("--server\x00--sender\x00-vlogDtpre.iLsfxCIvu\x00.\x00home/\x00\x00")
	var payload bytes.Buffer
	payload.Write(newEntry)
	payload.WriteByte(0) // flist 哨兵
	payload.WriteByte(0) // id list：preserve uid/gid 各空段
	payload.WriteByte(0)
	pre.Write(muxDataFrame(t, payload.Bytes()))

	// 交互客户端：收到 sum 表后回 match 块0 + literal 200 + match 块1 + literal 100
	server, client := listenOnce(t)
	done := runDeltaClient(t, client, pre.Bytes(), func(tbl *SumTable, resp *bytes.Buffer) error {
		if tbl.Count != 3 || tbl.Blength != 700 || tbl.Remainder != 100 {
			return fmt.Errorf("sum 表不符: %+v", tbl)
		}
		tokens := []int32{-1, 200, -2, 100}
		literals := [][]byte{bytes.Repeat([]byte{0x5A}, 200), bytes.Repeat([]byte{0x44}, 100)}
		resp.Write(buildDeltaResponse(tbl, tokens, literals, newContent))
		return nil
	})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	err = RunReceiver(ctx2, bufio.NewReader(server), server, m, r, nil)
	cancel2()
	if cerr := <-done; cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("二次备份: %v", err)
	}
	// 断言重组内容
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil {
		t.Fatalf("读取: %v", err)
	}
	if !bytes.Equal(out.Bytes(), newContent) {
		t.Fatalf("重组内容不符: got %d bytes want %d", out.Len(), len(newContent))
	}
}

// TestReceiverDeltaProtocolErrors：协议违例防御（match 回退 / 越界 / 整文件
// MD5 不匹配均报错且不产生快照）。
func TestReceiverDeltaProtocolErrors(t *testing.T) {
	// 错误场景表格：respond 返回的 token 流 / MD5 各自注入错误
	cases := []struct {
		name    string
		respond func(tbl *SumTable, resp *bytes.Buffer) error
		wantErr string
	}{
		{
			name: "match 偏移回退",
			respond: func(tbl *SumTable, resp *bytes.Buffer) error {
				tokens := []int32{-2, -1} // 先块 1 再块 0 → 回退
				resp.Write(buildDeltaResponse(tbl, tokens, nil, nil))
				return nil
			},
			wantErr: "回退",
		},
		{
			name: "match 越界",
			respond: func(tbl *SumTable, resp *bytes.Buffer) error {
				tokens := []int32{-10} // 只有 3 块，块 9 越界
				resp.Write(buildDeltaResponse(tbl, tokens, nil, nil))
				return nil
			},
			wantErr: "越界",
		},
		// 整文件校验和不匹配不再是会话错误（P1#9 改为单文件跳过+会话继续），
		// 新语义由 TestReceiverDeltaBadChecksumPartial 覆盖
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, m := newTestRepoLarge(t)
			oldContent := bytes.Repeat([]byte{0x41}, 1500)
			oldEntry := buildEntry(t, "a.txt", 0o644, 1500, false, "")
			conn, _ := pipeConn(t, buildSessionInput(t, [][]byte{oldEntry}, [][]byte{oldContent}))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := RunReceiver(ctx, bufio.NewReader(conn), conn, m, r, nil); err != nil {
				t.Fatalf("首次备份: %v", err)
			}
			cancel()

			newEntry := buildEntry(t, "a.txt", 0o644, 3, false, "")
			var pre bytes.Buffer
			pre.WriteString("--server\x00--sender\x00-vlogDtpre.iLsfxCIvu\x00.\x00home/\x00\x00")
			var payload bytes.Buffer
			payload.Write(newEntry)
			payload.WriteByte(0)
			payload.WriteByte(0)
			payload.WriteByte(0)
			pre.Write(muxDataFrame(t, payload.Bytes()))

			server, client := listenOnce(t)
			done := runDeltaClient(t, client, pre.Bytes(), c.respond)
			ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
			// 错误路径：服务端提前返回（goodbye 未发），客户端仍在等帧。
			// 关闭服务端连接使客户端读帧得到 EOF，解除客户端 goroutine 阻塞（否则 <-done 死锁）。
			err := RunReceiver(ctx2, bufio.NewReader(server), server, m, r, nil)
			cancel2()
			if err != nil {
				server.Close()
			}
			if cerr := <-done; cerr != nil && err == nil {
				err = cerr
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("期望错误含 %q，实际: %v", c.wantErr, err)
			}
			// 会话失败 = 半更新：a.txt 应保留首次备份的旧内容（未被破坏性覆盖）
			var out bytes.Buffer
			if err := r.ReadFile("a.txt", &out); err != nil || !bytes.Equal(out.Bytes(), oldContent) {
				t.Fatalf("失败会话不应改动已有内容: %v (%d 字节)", err, out.Len())
			}
		})
	}
}

// TestReceiverBadChecksumSkip P1#9：客户端发坏整文件校验和（读源中途失败）时
// legacy 路径此前不比对静默存坏数据；修复后不落库该文件，会话继续其余文件
// 并正常收尾（rsync 语义：单文件报错，其余完成；单一状态模型下坏文件保留
// 旧状态或不存在）。
func TestReceiverBadChecksumSkip(t *testing.T) {
	r, m := newTestRepoFull(t)
	entries := [][]byte{
		buildEntry(t, "bad.txt", 0o644, 6, false, ""),
		buildEntry(t, "good.txt", 0o644, 4, false, ""),
	}
	// 排序后 bad.txt(0)、good.txt(1)；bad.txt 坏校验和
	input := buildSessionInputSum(t, entries, [][]byte{[]byte("badsum"), []byte("good")}, 0)

	conn, serverOut := pipeConn(t, input)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := RunReceiver(ctx, bufio.NewReader(conn), conn, m, r, nil); err != nil {
		t.Logf("daemon 输出: % x", serverOut.Bytes())
		t.Fatalf("receiver: %v", err)
	}
	files, err := r.GetFilesForTest()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.Path == "bad.txt" {
			t.Fatalf("坏校验和文件不应落库: %v", files)
		}
	}
	var out bytes.Buffer
	if err := r.ReadFile("good.txt", &out); err != nil || out.String() != "good" {
		t.Fatalf("好文件应完好入库: %v %q", err, out.String())
	}
}

// TestReceiverDeltaBadChecksumPartial P1#9 delta 变体：整文件校验和不匹配
// （客户端读源中途失败发坏校验和）不再使整会话回滚——该文件不覆盖（保留
// 旧版本内容），会话正常收尾，客户端收到 file corruption 错误。
func TestReceiverDeltaBadChecksumPartial(t *testing.T) {
	r, m := newTestRepoLarge(t)
	oldContent := bytes.Repeat([]byte{0x41}, 1500)
	oldEntry := buildEntry(t, "a.txt", 0o644, 1500, false, "")
	conn, _ := pipeConn(t, buildSessionInput(t, [][]byte{oldEntry}, [][]byte{oldContent}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := RunReceiver(ctx, bufio.NewReader(conn), conn, m, r, nil); err != nil {
		t.Fatalf("首次备份: %v", err)
	}
	cancel()

	newEntry := buildEntry(t, "a.txt", 0o644, 3, false, "")
	var pre bytes.Buffer
	pre.WriteString("--server\x00--sender\x00-vlogDtpre.iLsfxCIvu\x00.\x00home/\x00\x00")
	var payload bytes.Buffer
	payload.Write(newEntry)
	payload.WriteByte(0)
	payload.WriteByte(0)
	payload.WriteByte(0)
	pre.Write(muxDataFrame(t, payload.Bytes()))

	server, client := listenOnce(t)
	done := runDeltaClient(t, client, pre.Bytes(), func(tbl *SumTable, resp *bytes.Buffer) error {
		tokens := []int32{3}
		resp.Write(buildDeltaResponse(tbl, tokens, [][]byte{[]byte("abc")}, nil)) // nil → 空 MD5（坏校验和）
		return nil
	})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	err := RunReceiver(ctx2, bufio.NewReader(server), server, m, r, nil)
	server.Close()
	if cerr := <-done; err == nil {
		err = cerr
	}
	if err != nil {
		t.Fatalf("坏校验和会话应正常完成: %v", err)
	}
	// 坏校验和文件不覆盖：a.txt 保留首次备份的旧版本内容
	var out bytes.Buffer
	if err := r.ReadFile("a.txt", &out); err != nil || !bytes.Equal(out.Bytes(), oldContent) {
		t.Fatalf("a.txt 应保留旧版本: %v %d 字节", err, out.Len())
	}
}

// TestSessionIoTimeout（P1#10）：客户端 argv 含 --timeout=N 且协商后停止发送时，
// 服务端会话应在 N 秒空闲后超时退出（idle 语义，对齐 rsyncd check_timeout），
// 而非挂死到 24h 硬 deadline。备份与恢复两个方向分别验证。
func TestSessionIoTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		argv string // 协商 argv（NUL 分隔）
		run  func(server net.Conn, r *repo.Repo, m *config.ModuleConfig) error
	}{
		{
			name: "备份方向",
			argv: "--server\x00--timeout=1\x00-vlogDtpre.iLsfxCIvu\x00.\x00home/\x00\x00",
			run: func(server net.Conn, r *repo.Repo, m *config.ModuleConfig) error {
				return RunReceiver(context.Background(), bufio.NewReader(server), server, m, r, nil)
			},
		},
		{
			name: "恢复方向",
			argv: "--server\x00--sender\x00--timeout=1\x00-vlogDtpre.iLsfxCIvu\x00.\x00home/\x00\x00",
			run: func(server net.Conn, r *repo.Repo, m *config.ModuleConfig) error {
				return RunSession(context.Background(), bufio.NewReader(server), server, m, r, nil)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, m := newTestRepoFull(t)
			// 客户端只完成协商（argv），之后不发任何数据（模拟挂死/半开连接）
			server, out := pipeConn(t, []byte(tc.argv))
			done := make(chan error, 1)
			go func() { done <- tc.run(server, r, m) }()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), "超时") {
					t.Fatalf("会话应因 io 空闲超时返回错误, got: %v", err)
				}
				// 协商完成后 daemon 立即主动发 MSG_IO_TIMEOUT(33) 宣告帧：
				// 帧头 04 00 00 28（len=4, tag=7+33）+ payload LE32(1)
				// （main.c start_server：multiplex 建立后先于任何数据发送）
				announce := []byte{0x04, 0x00, 0x00, 0x28, 0x01, 0x00, 0x00, 0x00}
				if !bytes.Contains(out.Bytes(), announce) {
					t.Fatalf("服务端输出应含 MSG_IO_TIMEOUT 宣告帧: % x", out.Bytes())
				}
				// 超时后向客户端补发 FERROR 文本（code 3）再断连：
				// "[server] io timeout after 1 seconds -- exiting\n" = 47 字节
				ferr := []byte{0x2f, 0x00, 0x00, 0x0a} // len=47, tag=7+3
				deadline := time.Now().Add(2 * time.Second)
				for !bytes.Contains(out.Bytes(), ferr) && time.Now().Before(deadline) {
					time.Sleep(50 * time.Millisecond)
				}
				if !bytes.Contains(out.Bytes(), ferr) {
					t.Fatalf("超时后应补发 FERROR(MSG_ERROR) 帧: % x", out.Bytes())
				}
			case <-time.After(6 * time.Second):
				server.Close()
				t.Fatal("客户端 --timeout=1 停发后服务端未在空闲超时内退出（无 io_timeout 保护，将挂到 24h 硬 deadline）")
			}
		})
	}
}

// TestReadOnlyRejectMessage（P2#12）：只读模块拒绝推送时应先向客户端发送
// 错误文本（真实 rsyncd：do_server_recv 报 "ERROR: module is read only"）
// 再断连，而非静默关闭（客户端只见 connection unexpectedly closed）。
func TestReadOnlyRejectMessage(t *testing.T) {
	r, m := newTestRepoFull(t)
	m.ReadOnly = true
	server, out := pipeConn(t, []byte("--server\x00-vlogDtpre.iLs\x00.\x00home/\x00\x00"))
	done := make(chan error, 1)
	go func() {
		done <- RunSession(context.Background(), bufio.NewReader(server), server, m, r, nil)
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "只读") {
			t.Fatalf("应返回只读错误: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("会话未返回")
	}
	// 帧经 TCP 回读 goroutine 异步落 buffer，轮询等待
	deadline := time.Now().Add(2 * time.Second)
	for !bytes.Contains(out.Bytes(), []byte("module is read only")) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !bytes.Contains(out.Bytes(), []byte("module is read only")) {
		t.Fatalf("客户端应收到 read only 错误文本再断连: % x", out.Bytes())
	}
}

// TestAppendRejected（P2#14）：--append/--append-verify（wire 上为 1/2 个独立
// "--append" 项）语义是数据从客户端已有偏移开始（receiver offset=sum.flength），
// 与本项目"从 0 全量重组"假设冲突，须明确拒绝并告知客户端，而非误接受产出错误数据。
func TestAppendRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		argv string
	}{
		{"append", "--server\x00--append\x00-vlogDtpre.iLs\x00.\x00home/\x00\x00"},
		{"append-verify 两个 --append 项", "--server\x00--append\x00--append\x00-vlogDtpre.iLs\x00.\x00home/\x00\x00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, m := newTestRepoFull(t)
			server, out := pipeConn(t, []byte(tc.argv))
			done := make(chan error, 1)
			go func() {
				done <- RunSession(context.Background(), bufio.NewReader(server), server, m, r, nil)
			}()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), "--append") {
					t.Fatalf("应拒绝 --append: %v", err)
				}
			case <-time.After(5 * time.Second):
				server.Close()
				t.Fatal("会话未返回（--append 未检测，误入会话等待数据）")
			}
			deadline := time.Now().Add(2 * time.Second)
			for !bytes.Contains(out.Bytes(), []byte("--append is not supported")) && time.Now().Before(deadline) {
				time.Sleep(50 * time.Millisecond)
			}
			if !bytes.Contains(out.Bytes(), []byte("--append is not supported")) {
				t.Fatalf("客户端应收到 --append 拒绝文本: % x", out.Bytes())
			}
		})
	}
}

// TestRestoreAtimesRejected（P2#13 恢复方向）：服务端为 sender 时客户端带 -U
// （--atimes）会期待 flist 携带 atime 字段；快照未持久化 atime 无法提供，
// 不拒绝会导致客户端解码错位，须明确拒绝。备份方向不拒绝（Parse 读掉字段）。
func TestRestoreAtimesRejected(t *testing.T) {
	r, m := newTestRepoFull(t)
	server, out := pipeConn(t, []byte("--server\x00--sender\x00-vlogDUtpre.iLs\x00.\x00home/\x00\x00"))
	done := make(chan error, 1)
	go func() {
		done <- RunSession(context.Background(), bufio.NewReader(server), server, m, r, nil)
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "--atimes") {
			t.Fatalf("应拒绝恢复方向 --atimes: %v", err)
		}
	case <-time.After(5 * time.Second):
		server.Close()
		t.Fatal("会话未返回")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !bytes.Contains(out.Bytes(), []byte("--atimes is not supported on restore")) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !bytes.Contains(out.Bytes(), []byte("--atimes is not supported on restore")) {
		t.Fatalf("客户端应收到 --atimes 拒绝文本: % x", out.Bytes())
	}
}
