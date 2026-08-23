package rsyncproto

import (
	"bytes"
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
	e, err := p.Parse(bytes.NewReader(data))
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
	e1, err := p.Parse(bytes.NewReader(buildRegularFileEntry(t, "a.txt", 0o644, 3, 0)))
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
	e2, err := p.Parse(bytes.NewReader(buf.Bytes()))
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
	e, err := p.Parse(bytes.NewReader(buf.Bytes()))
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
	e, err := p.Parse(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if !e.IsSymlink || e.LinkTarget != "target" || e.Path != "link1" {
		t.Fatalf("符号链接解析错误: %+v", e)
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
