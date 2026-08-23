// internal/rsyncproto/sum_test.go
package rsyncproto

import (
	"bytes"
	"testing"
)

// TestCalcSizes：块结构计算（generator.c sum_sizes_sqroot 复刻，固定向量来自源码算法推导）。
func TestCalcSizes(t *testing.T) {
	cases := []struct {
		len                 int64
		count, blength, rem int32
	}{
		{0, 0, 700, 0},                   // 空文件无块
		{1, 1, 700, 1},                   // 单字节 → 一块
		{700, 1, 700, 0},                 // 恰好一块（rem=0）
		{701, 2, 700, 1},                 // 余 1 字节一块
		{490000, 700, 700, 0},            // 700² 边界（len <= 490000 分支）
		{490001, 701, 700, 1},            // 超过 700²：sqrt≈700.0007 舍入 8 倍数=696 → max(696,700)=700
		{8388608, 2897, 2896, 1792},      // 8MiB：二进制 sqrt=2896.3 下取整舍入 8 倍数=2896
		{17179869184, 131072, 131072, 0}, // 16GiB：c 溢出到 >= MAX_BLOCK_SIZE → blength=131072
	}
	for _, c := range cases {
		count, blength, rem, err := CalcSizes(c.len)
		if err != nil {
			t.Fatalf("CalcSizes(%d): %v", c.len, err)
		}
		if count != c.count || blength != c.blength || rem != c.rem {
			t.Fatalf("CalcSizes(%d) = (%d,%d,%d)，期望 (%d,%d,%d)",
				c.len, count, blength, rem, c.count, c.blength, c.rem)
		}
	}
	if _, _, _, err := CalcSizes(-1); err == nil {
		t.Fatal("负长度应报错")
	}
}

// TestRollingSum1：get_checksum1 复刻（数据按 signed char、uint32 回绕）。
func TestRollingSum1(t *testing.T) {
	seq := make([]byte, 256)
	for i := range seq {
		seq[i] = byte(i)
	}
	cases := []struct {
		data []byte
		want uint32
	}{
		{[]byte("hello"), 103219732},
		{[]byte("abc"), 38404390},
		{nil, 0},
		{seq, 1786838912}, // 0..255 全序列（中间值回绕路径）
		{[]byte{0x80, 0x81, 0xFF, 0x00, 0x7F}, 4227923839}, // signed char 负值路径
	}
	for _, c := range cases {
		if got := RollingSum1(c.data); got != c.want {
			t.Fatalf("RollingSum1(% x) = %d，期望 %d", c.data, got, c.want)
		}
	}
}

// TestStrongSum2：get_checksum2 CSUM_MD5 复刻（proper_seed_order=0 → seed 后缀）。
func TestStrongSum2(t *testing.T) {
	cases := []struct {
		data []byte
		seed int32
		want string // hex
	}{
		{nil, 0, "d41d8cd98f00b204e9800998ecf8427e"}, // MD5("")
		{[]byte("hello"), 0, "5d41402abc4b2a76b9719d911017c592"},
		{[]byte("hello"), 123456, "579e593e1e916736d106669db8c5f69e"}, // MD5(hello||LE32(123456))
		{[]byte("abc"), 0, "900150983cd24fb0d6963f7d28e17f72"},
		{[]byte("abc"), 123456, "bd05da5b5858a30bb0aecb81cb48d7bf"},
	}
	for _, c := range cases {
		got := StrongSum2(c.data, c.seed)
		if hexOut := bytesToHex(got[:]); hexOut != c.want {
			t.Fatalf("StrongSum2(%q, %d) = %s，期望 %s", c.data, c.seed, hexOut, c.want)
		}
	}
}

func bytesToHex(b []byte) string {
	const hexDigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexDigits[v>>4]
		out[i*2+1] = hexDigits[v&0xf]
	}
	return string(out)
}

// TestCalcBlockSums：流式计算整表（1401 字节 → 3 块：700/700/1）。
func TestCalcBlockSums(t *testing.T) {
	data := bytes.Repeat([]byte{0x61}, 1401) // 'a'×1401
	tbl, err := CalcBlockSums(int64(len(data)), bytes.NewReader(data), 0)
	if err != nil {
		t.Fatal(err)
	}
	if tbl.Count != 3 || tbl.Blength != 700 || tbl.Remainder != 1 || tbl.S2Length != 16 {
		t.Fatalf("表头不符: %+v", tbl)
	}
	if len(tbl.Sums) != 3 {
		t.Fatalf("块数不符: %d", len(tbl.Sums))
	}
	// 前两块 700B、末块 1B；sum1 与分块手算一致
	if tbl.Sums[0].Sum1 != RollingSum1(data[:700]) ||
		tbl.Sums[1].Sum1 != RollingSum1(data[700:1400]) ||
		tbl.Sums[2].Sum1 != RollingSum1(data[1400:]) {
		t.Fatal("sum1 与分块计算不一致")
	}
	// 空文件：0 块
	empty, err := CalcBlockSums(0, bytes.NewReader(nil), 0)
	if err != nil || empty.Count != 0 || len(empty.Sums) != 0 {
		t.Fatalf("空文件: %+v err=%v", empty, err)
	}
	// 数据不足应报错
	if _, err := CalcBlockSums(10, bytes.NewReader([]byte("abc")), 0); err == nil {
		t.Fatal("数据不足应报错")
	}
}

// TestWriteSumTable：sum_head + 逐块校验和的线格式（count/blength/s2length/remainder
// 各 4B，正文每块 sum1 4B + sum2 16B）。
func TestWriteSumTable(t *testing.T) {
	tbl := SumTable{Count: 2, Blength: 700, S2Length: 16, Remainder: 0,
		Sums: []BlockSum{{Sum1: 0x11223344}, {Sum1: 0xAABBCCDD}}}
	var out bytes.Buffer
	if err := WriteSumTable(NewMuxWriter(&out), &tbl); err != nil {
		t.Fatal(err)
	}
	raw := out.Bytes()
	if len(raw) != 4+16+2*(4+16) { // 帧头 4B + 表头 16B + 正文 2×20B = 60
		t.Fatalf("长度不符: %d", len(raw))
	}
	// 跳过帧头：16B 表头
	head := raw[4:20]
	if !bytes.Equal(head, []byte{2, 0, 0, 0, 188, 2, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("表头字节不符: % x", head)
	}
	// 第一块 sum1 = 0x11223344 LE
	if !bytes.Equal(raw[20:24], []byte{0x44, 0x33, 0x22, 0x11}) {
		t.Fatalf("sum1 字节不符: % x", raw[20:24])
	}
	// 第二块 sum1 = 0xAABBCCDD LE
	if !bytes.Equal(raw[40:44], []byte{0xDD, 0xCC, 0xBB, 0xAA}) {
		t.Fatalf("sum1[1] 字节不符: % x", raw[40:44])
	}
}
