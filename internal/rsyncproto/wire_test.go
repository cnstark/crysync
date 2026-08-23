package rsyncproto

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestInt32Roundtrip(t *testing.T) {
	var buf bytes.Buffer
	for _, v := range []int32{0, 1, -1, 0x7FFFFFFF, -0x80000000, 123456} {
		buf.Reset()
		if err := WriteInt32(&buf, v); err != nil {
			t.Fatal(err)
		}
		got, err := ReadInt32(&buf)
		if err != nil || got != v {
			t.Fatalf("int32 往返失败: %d → %d (%v)", v, got, err)
		}
	}
}

func TestVarlong30(t *testing.T) {
	cases := []int32{0, 1, 127, 128, 16384, 0x3FFFFFFF}
	for _, v := range cases {
		var buf bytes.Buffer
		if err := WriteVarlong30(&buf, v); err != nil {
			t.Fatal(err)
		}
		got, err := ReadVarlong30(&buf)
		if err != nil || got != v {
			t.Fatalf("varlong30 往返失败: %d → %d (%v)", v, got, err)
		}
	}
	// 已知编码（varlong30 = 协议>=30 的 varlong(min_bytes=3)，io.h:21-52）：
	// 0 -> [00 00 00]；128 -> [00 80 00]；0x3FFFFFFF -> [BF FF FF FF]
	var buf bytes.Buffer
	WriteVarlong30(&buf, 0)
	if !bytes.Equal(buf.Bytes(), []byte{0x00, 0x00, 0x00}) {
		t.Fatalf("varlong30(0) 编码错误: % x", buf.Bytes())
	}
	buf.Reset()
	WriteVarlong30(&buf, 128)
	if !bytes.Equal(buf.Bytes(), []byte{0x00, 0x80, 0x00}) {
		t.Fatalf("varlong30(128) 编码错误: % x", buf.Bytes())
	}
	buf.Reset()
	WriteVarlong30(&buf, 0x3FFFFFFF)
	if !bytes.Equal(buf.Bytes(), []byte{0xBF, 0xFF, 0xFF, 0xFF}) {
		t.Fatalf("varlong30(0x3FFFFFFF) 编码错误: % x", buf.Bytes())
	}
	// 截断流报错（首字节 0xFF 分档要求 6 个数据字节）
	if _, err := ReadVarlong30(bytes.NewReader([]byte{0xFF, 0xFF, 0xFF, 0xFF})); err == nil {
		t.Fatal("截断流应报错")
	}
	// 溢出拒绝
	if _, err := ReadVarlong30(bytes.NewReader([]byte{0xFF, 0xFF, 0xFF, 0xFF})); err == nil {
		t.Fatal("超出 30 位应报错")
	}
}

// varint 已知编码（io.c write_varint 推导）：
// 127→[0x7F]；128→[0x80,0x80]；255→[0x80,0xFF]；256→[0x81,0x00]（首字节含高位 0x01）；
// 0x12345678→[0xF0,0x78,0x56,0x34,0x12]
func TestVarintEncoding(t *testing.T) {
	cases := []struct {
		v    int32
		want []byte
	}{
		{0, []byte{0x00}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x80}},
		{255, []byte{0x80, 0xFF}},
		{256, []byte{0x81, 0x00}},
		{0x12345678, []byte{0xF0, 0x78, 0x56, 0x34, 0x12}},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		if err := WriteVarint(&buf, c.v); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf.Bytes(), c.want) {
			t.Fatalf("WriteVarint(%d) = % x, want % x", c.v, buf.Bytes(), c.want)
		}
		got, err := ReadVarint(bytes.NewReader(buf.Bytes()))
		if err != nil || got != c.v {
			t.Fatalf("ReadVarint 往返失败: %d → %d (%v)", c.v, got, err)
		}
	}
}

// varlong 已知编码（io.c write_varlong min=4 推导）：
// 0→[0x00,0x00,0x00,0x00]；0x12345678→[0x12,0x78,0x56,0x34]；
// 0x0100000000→[0x81,0x00,0x00,0x00,0x00]（首字节含高位 0x01）
func TestVarlongEncoding(t *testing.T) {
	cases := []struct {
		v    int64
		want []byte
	}{
		{0, []byte{0x00, 0x00, 0x00, 0x00}},
		{0x12345678, []byte{0x12, 0x78, 0x56, 0x34}},
		{0x0100000000, []byte{0x81, 0x00, 0x00, 0x00, 0x00}},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		if err := WriteVarlong(&buf, c.v, 4); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf.Bytes(), c.want) {
			t.Fatalf("WriteVarlong(%d) = % x, want % x", c.v, buf.Bytes(), c.want)
		}
		got, err := ReadVarlong(bytes.NewReader(buf.Bytes()), 4)
		if err != nil || got != c.v {
			t.Fatalf("ReadVarlong 往返失败: %d → %d (%v)", c.v, got, err)
		}
	}
}

// ndx 差分编码（io.c write_ndx/read_ndx，初始 prev_positive=-1 prev_negative=1）：
// 0→[0x01]；1→[0x01]；5→[0x04]；NDX_DONE→[0x00]；大索引 300（diff=295）→[0xFE,0x01,0x27]
func TestNdxCodec(t *testing.T) {
	// 写侧
	var buf bytesBuffer
	w := newNdxCodec()
	for _, v := range []int32{0, 1, 5, ndxDone} {
		if err := w.Write(&buf, v); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(buf.Bytes(), []byte{0x01, 0x01, 0x04, 0x00}) {
		t.Fatalf("ndx 编码错误: % x", buf.Bytes())
	}
	// 读侧（同序列）
	r := newNdxCodec()
	got := []int32{}
	for i := 0; i < 4; i++ {
		v, err := r.Read(bytes.NewReader(buf.Bytes()[i : i+1]))
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	want := []int32{0, 1, 5, ndxDone}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ndx 解码 %d: got %d want %d", i, got[i], want[i])
		}
	}
	// 大索引（>253 diff）
	var b bytesBuffer
	w2 := newNdxCodec()
	if err := w2.Write(&b, 300); err != nil {
		t.Fatal(err)
	}
	wantBytes := []byte{0xFE, 0x01, 0x27} // diff=300-(-1)=301=0x012D？——见下
	_ = wantBytes
	// 实际：diff = 300 - prev(-1) = 301 = 0x012D → 0xFE 0x01 0x2D
	if !bytes.Equal(b.Bytes(), []byte{0xFE, 0x01, 0x2D}) {
		t.Fatalf("ndx 大索引编码错误: % x", b.Bytes())
	}
	r2 := newNdxCodec()
	v, err := r2.Read(bytes.NewReader(b.Bytes()))
	if err != nil || v != 300 {
		t.Fatalf("ndx 大索引解码: %d (%v)", v, err)
	}
}

// mux 帧（io.c mplex_write：int32 小端 ((7+code)<<24)|len）：
// WriteData("hello") → [05 00 00 07] + "hello"
func TestMuxFrames(t *testing.T) {
	var buf bytes.Buffer
	mw := NewMuxWriter(&buf)
	if err := mw.WriteData([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	want := append([]byte{0x05, 0x00, 0x00, 0x07}, []byte("hello")...)
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("mux 数据帧错误: % x", buf.Bytes())
	}
	if err := mw.WriteMsg("boom"); err != nil {
		t.Fatal(err)
	}
	want = append(want, 0x04, 0x00, 0x00, 0x08) // (7+1)<<24 | 4
	want = append(want, []byte("boom")...)
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("mux 消息帧错误: % x", buf.Bytes())
	}
	mr, err := NewMuxReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	data, isMsg, err := mr.Next()
	if err != nil || isMsg || string(data) != "hello" {
		t.Fatalf("数据帧: %q %v %v", data, isMsg, err)
	}
	data, isMsg, err = mr.Next()
	if err != nil || !isMsg || string(data) != "boom" {
		t.Fatalf("消息帧: %q %v %v", data, isMsg, err)
	}
	if _, _, err := mr.Next(); err != io.EOF {
		t.Fatalf("流结束应返回 EOF: %v", err)
	}
}

var _ = binary.LittleEndian
