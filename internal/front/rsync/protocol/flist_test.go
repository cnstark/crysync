package protocol

import (
	"bytes"
	"crypto/md5"
	"testing"
)

// 构造一条真实编码的 flist 记录（flist.c send_file_entry，protocol 31）：
// 常规文件 "a.txt"：xflags=0（无 SAME 位，1 字节 0x00？——xflags==0 会触发 EXTENDED_FLAGS 2 字节。
// 为构造简单，用 SAME_TIME|SAME_UID|SAME_GID（mtime/uid/gid 走 SAME，不发）：
// xflags = 0x80(SAME_TIME)|0x08(SAME_UID)|0x10(SAME_GID) = 0x98 → 1 字节
// name_len=5 "a.txt"（无 SAME_NAME/LONG_NAME）→ write_byte(5) + 字节
// F_LENGTH=3 → varlong30 [0x03]
// mode=0o644 非 SAME → int32
func buildRegularFileEntry(t *testing.T, name string, mode uint32, size int32, mtime int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	xflags := XmitSameTime | XmitSameUID | XmitSameGID
	buf.WriteByte(byte(xflags))
	buf.WriteByte(byte(len(name)))
	buf.WriteString(name)
	WriteVarlong30(&buf, size)
	if xflags&XmitSameTime == 0 {
		WriteVarlong(&buf, mtime, 4)
	}
	if xflags&XmitSameMode == 0 {
		WriteInt32(&buf, int32(mode))
	}
	// uid/gid SAME 不发
	return buf.Bytes()
}

func TestParseFileEntryRegular(t *testing.T) {
	data := buildRegularFileEntry(t, "a.txt", 0o644, 3, 0)
	p := NewFlistParser()
	e, err := p.Parse(bytes.NewReader(data), true, true, true, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if e.Path != "a.txt" || e.Mode != 0o644 || e.Size != 3 || e.UID != 0 || e.GID != 0 {
		t.Fatalf("解析错误: %+v", e)
	}
	if e.IsDir || e.IsSymlink {
		t.Fatalf("应为普通文件: %+v", e)
	}
}

// 名字公共前缀：先 "a.txt" 再 "a.log"（前缀 "a."，SAME_NAME=0x20）
func TestParseFileEntrySameName(t *testing.T) {
	p := NewFlistParser()
	e1, err := p.Parse(bytes.NewReader(buildRegularFileEntry(t, "a.txt", 0o644, 3, 0)), true, true, true, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	_ = e1
	// 第二条：xflags = SAME_TIME|SAME_UID|SAME_GID|SAME_NAME；l1=2（"a."）；l2=3（"log"）
	var buf bytes.Buffer
	xflags := XmitSameTime | XmitSameUID | XmitSameGID | XmitSameName
	buf.WriteByte(byte(xflags))
	buf.WriteByte(2) // l1 公共前缀
	buf.WriteByte(3) // l2
	buf.WriteString("log")
	WriteVarlong30(&buf, 3) // F_LENGTH
	WriteInt32(&buf, 0o644) // mode（非 SAME）
	e2, err := p.Parse(bytes.NewReader(buf.Bytes()), true, true, true, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if e2.Path != "a.log" {
		t.Fatalf("公共前缀名字错误: %q", e2.Path)
	}
}

// 目录条目：xflags 默认含 XMIT_NO_CONTENT_DIR(0x100)（protocol 30+）→ 高字节非 0 → 2 字节格式；
// 目录 mode 总是发送（S_ISDIR 判定依赖 mode）
func TestParseFileEntryDir(t *testing.T) {
	var buf bytes.Buffer
	xflags := XmitNoContentDir | XmitSameTime | XmitSameUID | XmitSameGID
	// 高字节 0x01 非 0 → 加 EXTENDED_FLAGS(0x04) → write_shortint
	writeXflags := xflags | XmitExtended
	WriteShortint(&buf, uint16(writeXflags))
	buf.WriteByte(3) // l2
	buf.WriteString("sub")
	WriteVarlong30(&buf, 0)          // F_LENGTH=0（目录）
	WriteInt32(&buf, 0o040000|0o755) // mode（目录总是发送）
	// mtime/uid/gid 全 SAME 不发
	p := NewFlistParser()
	e, err := p.Parse(bytes.NewReader(buf.Bytes()), true, true, true, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !e.IsDir || e.Path != "sub" {
		t.Fatalf("目录解析错误: %+v", e)
	}
}

// 符号链接：mode 含 S_IFLNK（0o120000），symlink 目标 varint30 长度 + 字节
// （flist.c:640 write_varint30 不含 '\0'；接收侧 read_sbuf(f,bp,linkname_len-1) 读 len 字节）
func TestParseFileEntrySymlink(t *testing.T) {
	var buf bytes.Buffer
	xflags := XmitSameTime | XmitSameUID | XmitSameGID
	buf.WriteByte(byte(xflags))
	buf.WriteByte(5) // l2
	buf.WriteString("link1")
	WriteVarlong30(&buf, 0)          // F_LENGTH=0（链接）
	WriteInt32(&buf, 0o120000|0o777) // mode：S_IFLNK
	WriteVarint(&buf, 6)             // symlink_len = 6
	buf.WriteString("target")
	p := NewFlistParser()
	e, err := p.Parse(bytes.NewReader(buf.Bytes()), true, true, true, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !e.IsSymlink || e.LinkTarget != "target" || e.Path != "link1" {
		t.Fatalf("符号链接解析错误: %+v", e)
	}
}

// TestParsePreserveOff 模拟真实客户端 preserve off（如 `rsync -r`，无 -og/-l）发送的
// flist 字节流：XMIT_SAME_UID/XMIT_SAME_GID 置位但 uid/gid 字段不发（flist.c
// send_file_entry：!preserve_uid → 置位跳过），symlink 条目 mode 含 S_IFLNK 但
// target 字段段不发（!preserve_links → 跳过）。F_LENGTH 恒为 target 长度。
// 三条目 + 哨兵：普通文件 a.txt、symlink link1（无 target）、目录 sub。
func TestParsePreserveOff(t *testing.T) {
	var buf bytes.Buffer
	// 条目 1：普通文件 "a.txt"
	xflags := XmitSameUID | XmitSameGID // preserve off：客户端置 SAME 不发字段
	buf.WriteByte(byte(xflags))
	buf.WriteByte(5)
	buf.WriteString("a.txt")
	WriteVarlong30(&buf, 3)  // F_LENGTH
	WriteVarlong(&buf, 1e9, 4) // mtime（非 SAME_TIME）
	WriteInt32(&buf, 0o100644) // mode
	// 无 uid/gid 字段
	// 条目 2：symlink "link1"（preserve_links off：无 target 段）
	buf.WriteByte(byte(xflags))
	buf.WriteByte(5)
	buf.WriteString("link1")
	WriteVarlong30(&buf, 6)    // F_LENGTH = target 长度（恒发）
	WriteVarlong(&buf, 1e9, 4) // mtime
	WriteInt32(&buf, 0o120777) // mode：S_IFLNK
	// 无 target 段
	// 条目 3：目录 "sub"（xflags 高字节非 0 → EXTENDED + shortint 2 字节）
	dirXflags := XmitNoContentDir | XmitSameUID | XmitSameGID
	WriteShortint(&buf, uint16(dirXflags|XmitExtended))
	buf.WriteByte(3)
	buf.WriteString("sub")
	WriteVarlong30(&buf, 0)
	WriteVarlong(&buf, 1e9, 4)
	WriteInt32(&buf, 0o040755)
	// 哨兵
	buf.WriteByte(0)

	p := NewFlistParser()
	r := bytes.NewReader(buf.Bytes())
	e1, err := p.Parse(r, false, false, false, false, false, false)
	if err != nil {
		t.Fatalf("条目 1 解析失败: %v", err)
	}
	if e1.Path != "a.txt" || e1.IsSymlink || e1.Size != 3 {
		t.Fatalf("条目 1 解析错误: %+v", e1)
	}
	e2, err := p.Parse(r, false, false, false, false, false, false)
	if err != nil {
		t.Fatalf("条目 2（symlink preserve off）解析失败: %v", err)
	}
	if e2.Path != "link1" || !e2.IsSymlink || e2.LinkTarget != "" {
		t.Fatalf("条目 2 解析错误: %+v", e2)
	}
	e3, err := p.Parse(r, false, false, false, false, false, false)
	if err != nil {
		t.Fatalf("条目 3（目录）解析失败: %v", err)
	}
	if e3.Path != "sub" || !e3.IsDir {
		t.Fatalf("条目 3 解析错误: %+v", e3)
	}
	if _, err := p.Parse(r, false, false, false, false, false, false); err != ErrFlistEnd {
		t.Fatalf("应读到哨兵，得到 %v", err)
	}
}

// TestParsePreserveOnUidGid preserve on 但 SAME 位未置（首条目）：uid/gid 正常读取。
// xflags=TOP_DIR（非 0、无任何 SAME 位）：mtime/mode/uid/gid 全部发送。
func TestParsePreserveOnUidGid(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(byte(XmitTopDir))
	buf.WriteByte(5)
	buf.WriteString("a.txt")
	WriteVarlong30(&buf, 3)
	WriteVarlong(&buf, 1e9, 4)
	WriteInt32(&buf, 0o100644)
	WriteVarint(&buf, 1000) // uid
	WriteVarint(&buf, 1000) // gid

	p := NewFlistParser()
	e, err := p.Parse(bytes.NewReader(buf.Bytes()), true, true, true, false, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if e.UID != 1000 || e.GID != 1000 {
		t.Fatalf("uid/gid 解析错误: %+v", e)
	}
}

// 路径安全：.. 段与绝对路径拒绝
func TestCleanPathRejects(t *testing.T) {
	for _, p := range []string{"", ".", "/etc/passwd", "../x", "a/../b"} {
		if got := cleanPath(p); got != "" {
			t.Fatalf("cleanPath(%q) 应拒绝，得到 %q", p, got)
		}
	}
	if got := cleanPath("sub/"); got != "sub/" {
		t.Fatalf("cleanPath 应保留尾斜杠: %q", got)
	}
}

// TestFNameCmp 验证 fNameCmp 复刻 rsync f_name_cmp（flist.c:3217，protocol≥29）的排序。
// 权威顺序来自 C 源码逐对验证（/tmp/fncmp.c）；a 排 b 前 → fNameCmp(a,b) < 0。
func TestFNameCmp(t *testing.T) {
	f := func(p string, dir bool) FileEntry { return FileEntry{Path: p, IsDir: dir} }
	cases := []struct {
		a, b FileEntry
	}{
		{f("x/y2", false), f("x/y/z.txt", false)}, // 文件 vs 文件：dirname 段 "x" 完插 "/" 与 "x/y" 的 'y' 比
		{f("x/y2", false), f("x/y", true)},        // 文件 vs 目录：同 dirname 时文件前（s_SLASH 重判 type）
		{f("x", true), f("x/y2", false)},          // 目录 vs 其前缀文件：目录尾 "/" < 文件继续字符
		{f("x", true), f("x/y", true)},            // 目录 vs 目录："/" 边界
		{f("a-b", false), f("a.txt", false)},      // '-' (0x2d) < '.' (0x2e)
		{f("a.txt", false), f("link1", false)},    // 字符序
		{f("a.txt", false), f("a", true)},         // 文件前于同深度目录（t_ITEM 前 t_PATH）
		{f("a", true), f("a/b.txt", false)},       // 目录前于其内容
		{f("sub", true), f("sub/b.bin", false)},   // 目录前于其内容
		{f(".", true), f("x/y2", false)},          // 顶层 "." 恒最前
		{f(".", true), f("a.txt", false)},         // 顶层 "." 恒最前
		{f("", true), f("a.txt", false)},          // Path=""（cleanPath 归一化的 "."）恒最前
	}
	for _, c := range cases {
		if got := fNameCmp(c.a, c.b); got >= 0 {
			t.Errorf("fNameCmp(%q dir=%v, %q dir=%v) = %d，应 < 0", c.a.Path, c.a.IsDir, c.b.Path, c.b.IsDir, got)
		}
	}
}

// TestSortFlistEntries 验证完整排序：顶层文件字符序 → 目录 → 深层内容按 f_name_cmp。
func TestSortFlistEntries(t *testing.T) {
	entries := []FileEntry{
		{Path: "sub/", IsDir: true},
		{Path: "link1", IsDir: false},
		{Path: "a.txt", IsDir: false},
		{Path: "b.txt", IsDir: false},
	}
	out := SortFlistEntries(entries)
	// 排序保留原始 Path（含尾斜杠）；落库时 applyStaticEntry 才 TrimSuffix
	want := []string{"a.txt", "b.txt", "link1", "sub/"}
	for i, w := range want {
		if out[i].Path != w {
			t.Fatalf("排序[%d] = %q，应 %q；完整: %v", i, out[i].Path, w, paths(out))
		}
	}
	// 含顶层 "."（Path=""）：恒排最前
	entries2 := []FileEntry{
		{Path: "", IsDir: true},
		{Path: "a.txt", IsDir: false},
		{Path: "sub/", IsDir: true},
	}
	out2 := SortFlistEntries(entries2)
	if out2[0].Path != "" {
		t.Fatalf("顶层 . 应排最前: %v", paths(out2))
	}
}

func paths(es []FileEntry) []string {
	ps := make([]string, len(es))
	for i, e := range es {
		ps[i] = e.Path
	}
	return ps
}

// TestParseChecksumMode P0#4：--checksum（-c）模式下客户端每条 REGULAR 条目尾部
// 附加 flist_csum_len=16 字节纯内容 MD5（flist.c:757-766 always_checksum && S_ISREG），
// 目录/符号链接不附。解析器必须读掉该段保持流同步（v1 不使用其值）。
func TestParseChecksumMode(t *testing.T) {
	var buf bytes.Buffer
	// 条目 1：普通文件 "a.txt"（SAME_UID|SAME_GID 简化字段流）
	buf.WriteByte(byte(XmitSameUID | XmitSameGID))
	buf.WriteByte(5)
	buf.WriteString("a.txt")
	WriteVarlong30(&buf, 3)
	WriteVarlong(&buf, 1e9, 4)
	WriteInt32(&buf, 0o100644)
	cs := md5.Sum([]byte("abc")) // 任意 16 字节（解析器不校验值）
	buf.Write(cs[:])
	// 条目 2：目录 "sub"（无校验和）
	dirXflags := XmitNoContentDir | XmitSameUID | XmitSameGID
	WriteShortint(&buf, uint16(dirXflags|XmitExtended))
	buf.WriteByte(3)
	buf.WriteString("sub")
	WriteVarlong30(&buf, 0)
	WriteVarlong(&buf, 1e9, 4)
	WriteInt32(&buf, 0o040755)
	// 哨兵
	buf.WriteByte(0)

	p := NewFlistParser()
	r := bytes.NewReader(buf.Bytes())
	e1, err := p.Parse(r, true, true, true, false, false, true)
	if err != nil {
		t.Fatalf("条目 1 解析失败: %v", err)
	}
	if e1.Path != "a.txt" || e1.IsDir {
		t.Fatalf("条目 1 解析错误: %+v", e1)
	}
	e2, err := p.Parse(r, true, true, true, false, false, true)
	if err != nil {
		t.Fatalf("条目 2（目录，无校验和）解析失败: %v", err)
	}
	if e2.Path != "sub" || !e2.IsDir {
		t.Fatalf("条目 2 解析错误: %+v", e2)
	}
	if _, err := p.Parse(r, true, true, true, false, false, true); err != ErrFlistEnd {
		t.Fatalf("应读到哨兵，得到 %v", err)
	}
}

// TestParseDeviceSpecial P0#5：设备/特殊条目的 rdev 字段流（flist.c:1023-1042 接收侧，
// protocol 31）：preserve_devices 且 CHR/BLK 才有 rdev 段——major 仅在未置
// XMIT_SAME_RDEV_MAJOR(1<<8) 时读 varint，minor 恒读 varint；FIFO/SOCK（IS_SPECIAL）
// 完全无 rdev 字节。接收侧语义：设备条目 file_length 清零。三条目 + 哨兵验证流位置精确。
func TestParseDeviceSpecial(t *testing.T) {
	var buf bytes.Buffer
	// 条目 1：CHR "chr0"，rdev major+minor 都发（SAME_RDEV_MAJOR 未置，高字节 0 → 1 字节 xflags）
	buf.WriteByte(byte(XmitSameTime | XmitSameUID | XmitSameGID))
	buf.WriteByte(4)
	buf.WriteString("chr0")
	WriteVarlong30(&buf, 7)            // F_LENGTH=7（接收侧应清零，flist.c:1042）
	WriteInt32(&buf, int32(sIfChr|0o600)) // mode（非 SAME）
	WriteVarint(&buf, 1)               // rdev major
	WriteVarint(&buf, 5)               // rdev minor
	// 条目 2：BLK "blk0"，SAME_RDEV_MAJOR 置位（1<<8 → 2 字节 xflags）只发 minor
	blkXflags := XmitSameTime | XmitSameUID | XmitSameGID | XmitSameRdevMajor
	WriteShortint(&buf, uint16(blkXflags|XmitExtended))
	buf.WriteByte(4)
	buf.WriteString("blk0")
	WriteVarlong30(&buf, 0)
	WriteInt32(&buf, int32(sIfBlk|0o600))
	WriteVarint(&buf, 7) // rdev minor（major 复用上一条）
	// 条目 3：FIFO "fifo0"，无任何 rdev 字节（IS_SPECIAL，protocol 31）
	buf.WriteByte(byte(XmitSameTime | XmitSameUID | XmitSameGID))
	buf.WriteByte(5)
	buf.WriteString("fifo0")
	WriteVarlong30(&buf, 0)
	WriteInt32(&buf, int32(sIfFifo|0o600))
	// 哨兵
	buf.WriteByte(0)

	p := NewFlistParser()
	r := bytes.NewReader(buf.Bytes())
	e1, err := p.Parse(r, true, true, true, true, false, false)
	if err != nil {
		t.Fatalf("条目 1（CHR）解析失败: %v", err)
	}
	if e1.Path != "chr0" || e1.Size != 0 || e1.Mode != sIfChr|0o600 || e1.IsDir || e1.IsSymlink {
		t.Fatalf("条目 1 解析错误: %+v", e1)
	}
	e2, err := p.Parse(r, true, true, true, true, false, false)
	if err != nil {
		t.Fatalf("条目 2（BLK，SAME_RDEV_MAJOR）解析失败: %v", err)
	}
	if e2.Path != "blk0" || e2.Size != 0 || e2.Mode != sIfBlk|0o600 {
		t.Fatalf("条目 2 解析错误: %+v", e2)
	}
	e3, err := p.Parse(r, true, true, true, true, false, false)
	if err != nil {
		t.Fatalf("条目 3（FIFO，无 rdev）解析失败: %v", err)
	}
	if e3.Path != "fifo0" || e3.Mode != sIfFifo|0o600 {
		t.Fatalf("条目 3 解析错误: %+v", e3)
	}
	if _, err := p.Parse(r, true, true, true, true, false, false); err != ErrFlistEnd {
		t.Fatalf("应读到哨兵，得到 %v", err)
	}
}

// TestParseIoErrorEndlist P1#8：发送侧出错（源目录消失/vanished 文件）时 flist
// 结尾用 XMIT_EXTENDED_FLAGS|XMIT_IO_ERROR_ENDLIST(0x1004) 短整型 + varint
// io_error 替代单字节 0（flist.c:2388-2391 发送侧 / 2960-2970 接收侧）。解析器
// 必须识别为列表结束并读掉 io_error，否则字段流错位、会话卡死。
func TestParseIoErrorEndlist(t *testing.T) {
	var buf bytes.Buffer
	// 一条正常条目
	buf.WriteByte(byte(XmitSameTime | XmitSameUID | XmitSameGID))
	buf.WriteByte(5)
	buf.WriteString("a.txt")
	WriteVarlong30(&buf, 3)
	WriteInt32(&buf, 0o100644)
	// IO_ERROR_ENDLIST 哨兵：shortint 0x1004 + varint io_error=3
	WriteShortint(&buf, uint16(XmitExtended|XmitIoErrorEndlist))
	WriteVarint(&buf, 3)

	p := NewFlistParser()
	r := bytes.NewReader(buf.Bytes())
	e, err := p.Parse(r, true, true, true, true, false, false)
	if err != nil {
		t.Fatalf("条目 1 解析失败: %v", err)
	}
	if e.Path != "a.txt" {
		t.Fatalf("条目 1 解析错误: %+v", e)
	}
	if _, err := p.Parse(r, true, true, true, true, false, false); err != ErrFlistEnd {
		t.Fatalf("应识别 IO_ERROR_ENDLIST 为列表结束，得到 %v", err)
	}
	if p.IoError != 3 {
		t.Fatalf("应记录 io_error=3，得到 %d", p.IoError)
	}
	// 无 io_error 的普通哨兵不受影响
	p2 := NewFlistParser()
	r2 := bytes.NewReader([]byte{0})
	if _, err := p2.Parse(r2, false, false, false, false, false, false); err != ErrFlistEnd {
		t.Fatalf("普通哨兵应返回 ErrFlistEnd，得到 %v", err)
	}
	if p2.IoError != 0 {
		t.Fatalf("普通哨兵不应设置 io_error: %d", p2.IoError)
	}
}

// TestParseDeviceNoPreserveDevices preserve_devices off（如 `rsync -r`）时 CHR 条目
// 无 rdev 字节（防御分支，flist.c:1043-1051：设备条目 file_length 仍清零）。
func TestParseDeviceNoPreserveDevices(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(byte(XmitSameTime | XmitSameUID | XmitSameGID))
	buf.WriteByte(4)
	buf.WriteString("chr0")
	WriteVarlong30(&buf, 0)
	WriteInt32(&buf, int32(sIfChr|0o600))
	buf.WriteByte(0) // 哨兵

	p := NewFlistParser()
	e, err := p.Parse(bytes.NewReader(buf.Bytes()), true, true, true, false, false, false)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if e.Path != "chr0" || e.Size != 0 || e.Mode != sIfChr|0o600 {
		t.Fatalf("解析错误: %+v", e)
	}
}

// TestParseAtimeField（P2#13）：--atimes（组合短选项 'U'）时非目录条目在 mode
// 之后、uid 之前携带 varlong(4) atime 字段（flist.c:985-986）；SAME_ATIME(1<<14)
// 置位时省略。不读该字段则 uid/gid/后续条目全部错位。v1 读掉丢弃（不落库）。
func TestParseAtimeField(t *testing.T) {
	var buf bytes.Buffer
	// 条目 1：SAME_TIME|SAME_UID|SAME_GID，mode 非 SAME → mode 后跟 atime varlong(4)
	xflags := XmitSameTime | XmitSameUID | XmitSameGID
	buf.WriteByte(byte(xflags))
	buf.WriteByte(5)
	buf.WriteString("f.txt")
	WriteVarlong30(&buf, 7)
	WriteInt32(&buf, int32(0o644))
	WriteVarlong(&buf, 1700000000, 4) // atime
	// 条目 2：SAME_ATIME(1<<14) 置位 → 无 atime 字段（xflags > 0xFF 走 2 字节
	// 扩展格式：低 8 位 | XMIT_EXTENDED_FLAGS + 高 8 位）
	x2 := xflags | XmitSameMode | XmitSameAtime
	buf.WriteByte(byte(x2&0xFF) | 0x04)
	buf.WriteByte(byte(x2 >> 8))
	buf.WriteByte(5)
	buf.WriteString("g.txt")
	WriteVarlong30(&buf, 9)
	buf.WriteByte(0) // 哨兵

	p := NewFlistParser()
	r := bytes.NewReader(buf.Bytes())
	e1, err := p.Parse(r, true, true, true, false, true, false)
	if err != nil {
		t.Fatalf("含 atime 字段条目应解析成功（读掉 atime 保持流同步）: %v", err)
	}
	if e1.Path != "f.txt" || e1.Size != 7 || e1.Mode != 0o644 {
		t.Fatalf("条目 1 解析错误: %+v", e1)
	}
	e2, err := p.Parse(r, true, true, true, false, true, false)
	if err != nil {
		t.Fatalf("SAME_ATIME 条目应解析成功: %v", err)
	}
	if e2.Path != "g.txt" || e2.Size != 9 || e2.Mode != 0o644 {
		t.Fatalf("条目 2 解析错误（atime 未读导致字段错位）: %+v", e2)
	}
	if _, err := p.Parse(r, true, true, true, false, true, false); err != ErrFlistEnd {
		t.Fatalf("应以哨兵结束: %v", err)
	}
}
