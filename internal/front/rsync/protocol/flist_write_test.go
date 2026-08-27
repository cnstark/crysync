package protocol

import (
	"bytes"
	"testing"
)

// golden 字节按 rsync 3.4.1 send_file_entry 旧式路径（compat=0）手工推算：
// 普通文件 "a.txt" mode 0644 size 3 mtime 1700000000 uid/gid 1000，preserve 全开。
// xflags：preserve 开 → 0；非目录兜底 TOP_DIR(1) → 单字节 0x01。
// F_LENGTH=3 → varlong30 [03 00 00]；mtime → varlong4 [65 00 FA 53]；
// mode → int32 [A4 01 00 00]；uid/gid → varint [E8 03]。
func TestFlistWriterRegularGolden(t *testing.T) {
	e := FileEntry{Path: "a.txt", Mode: 0o644, Size: 3, MTimeNs: 1700000000 * 1e9, UID: 1000, GID: 1000}
	want := []byte{
		0x01, 0x05, 'a', '.', 't', 'x', 't',
		0x00, 0x03, 0x00,
		0x65, 0x00, 0xF1, 0x53,
		0xA4, 0x01, 0x00, 0x00,
		0x83, 0xE8,
		0x83, 0xE8,
	}
	var buf bytes.Buffer
	if err := NewFlistWriter().WriteEntry(&buf, e, true, true, true); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("编码不符:\n got %x\nwant %x", buf.Bytes(), want)
	}
}

// preserve 关闭：xflags 必须置 SAME_UID|SAME_GID(0x18)，uid/gid 字段不出现。
func TestFlistWriterNoPreserve(t *testing.T) {
	e := FileEntry{Path: "a.txt", Mode: 0o644, Size: 3, MTimeNs: 1700000000 * 1e9, UID: 1000, GID: 1000}
	want := []byte{
		0x18, 0x05, 'a', '.', 't', 'x', 't',
		0x00, 0x03, 0x00,
		0x65, 0x00, 0xF1, 0x53,
		0xA4, 0x01, 0x00, 0x00,
	}
	var buf bytes.Buffer
	if err := NewFlistWriter().WriteEntry(&buf, e, false, false, true); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("编码不符:\n got %x\nwant %x", buf.Bytes(), want)
	}
}

// 目录：xflags=0（有内容目录非 top，不兜底 TOP_DIR）→ EXTENDED_FLAGS shortint
// [04 00]；mode 带 S_IFDIR(040000) 全量发送。
func TestFlistWriterDirGolden(t *testing.T) {
	e := FileEntry{Path: "sub", IsDir: true, Mode: 0o40755, MTimeNs: 1700000000 * 1e9, UID: 1000, GID: 1000}
	want := []byte{
		0x04, 0x00, // EXTENDED_FLAGS | 0
		0x03, 's', 'u', 'b',
		0x00, 0x00, 0x00, // F_LENGTH=0
		0x65, 0x00, 0xF1, 0x53,
		0xED, 0x41, 0x00, 0x00, // 0o040755
		0x83, 0xE8,
		0x83, 0xE8,
	}
	var buf bytes.Buffer
	if err := NewFlistWriter().WriteEntry(&buf, e, true, true, true); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("编码不符:\n got %x\nwant %x", buf.Bytes(), want)
	}
}

// 符号链接：F_LENGTH = target 长度；target 用 varint 长度 + 字节。
func TestFlistWriterSymlinkGolden(t *testing.T) {
	e := FileEntry{Path: "link1", IsSymlink: true, Mode: 0o120777, MTimeNs: 1700000000 * 1e9,
		UID: 1000, GID: 1000, LinkTarget: "a.txt"}
	want := []byte{
		0x01, // 非目录兜底 TOP_DIR
		0x05, 'l', 'i', 'n', 'k', '1',
		0x00, 0x05, 0x00, // F_LENGTH=5
		0x65, 0x00, 0xF1, 0x53,
		0xFF, 0xA1, 0x00, 0x00, // 0o120777
		0x83, 0xE8,
		0x83, 0xE8,
		0x05, 'a', '.', 't', 'x', 't', // symlink target
	}
	var buf bytes.Buffer
	if err := NewFlistWriter().WriteEntry(&buf, e, true, true, true); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("编码不符:\n got %x\nwant %x", buf.Bytes(), want)
	}
}

// MOD_NSEC：纳秒非零 → xflags 置 1<<13（高字节非零 → EXTENDED shortint [04 20]），
// mtime 秒之后写 varint 纳秒 [E7 15 CD 5B]（123456789）。
func TestFlistWriterModNsec(t *testing.T) {
	e := FileEntry{Path: "n.txt", Mode: 0o644, Size: 1,
		MTimeNs: 1700000000*1e9 + 123456789, UID: 1000, GID: 1000}
	var buf bytes.Buffer
	if err := NewFlistWriter().WriteEntry(&buf, e, true, true, true); err != nil {
		t.Fatal(err)
	}
	got := buf.Bytes()
	want := []byte{
		0x04, 0x20, // EXTENDED_FLAGS | MOD_NSEC
		0x05, 'n', '.', 't', 'x', 't',
		0x00, 0x01, 0x00, // F_LENGTH=1
		0x65, 0x00, 0xF1, 0x53, // mtime 秒
		0xE7, 0x15, 0xCD, 0x5B, // nsec varint
		0xA4, 0x01, 0x00, 0x00,
		0x83, 0xE8,
		0x83, 0xE8,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("编码不符:\n got %x\nwant %x", got, want)
	}
}

// 目录路径去尾斜杠 + 与 FlistParser 的完整往返（文件/目录/链接混合，公共前缀干扰）。
// 注意：FlistParser 对 MOD_NSEC 读掉丢弃（备份方向同语义），故用整秒 mtime。
func TestFlistWriterRoundTrip(t *testing.T) {
	entries := []FileEntry{
		{Path: "a.txt", Mode: 0o644, Size: 13, MTimeNs: 1700000000 * 1e9, UID: 1000, GID: 1000},
		{Path: "a/", IsDir: true, Mode: 0o40755, MTimeNs: 1700000001 * 1e9, UID: 1000, GID: 1000},
		{Path: "a/b.txt", Mode: 0o600, Size: 200, MTimeNs: 1700000002 * 1e9, UID: 1001, GID: 1002},
		{Path: "link", IsSymlink: true, Mode: 0o120777, MTimeNs: 1700000003 * 1e9, UID: 0, GID: 0, LinkTarget: "/usr/bin/sh"},
		{Path: "emptydir", IsDir: true, Mode: 0o40700, MTimeNs: 1700000004 * 1e9, UID: 1000, GID: 1000},
	}
	for _, preserve := range []bool{true, false} {
		var buf bytes.Buffer
		w := NewFlistWriter()
		for _, e := range entries {
			if err := w.WriteEntry(&buf, e, preserve, preserve, preserve); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.WriteEndOfFlist(&buf); err != nil {
			t.Fatal(err)
		}
		p := NewFlistParser()
		rd := bytes.NewReader(buf.Bytes())
		var got []FileEntry
		for {
			e, err := p.Parse(rd, preserve, preserve, preserve, false, false, false)
			if err == ErrFlistEnd {
				break
			}
			if err != nil {
				t.Fatalf("解析失败 (preserve=%v): %v", preserve, err)
			}
			got = append(got, e)
		}
		if len(got) != len(entries) {
			t.Fatalf("条目数不符 (preserve=%v): got %d want %d", preserve, len(got), len(entries))
		}
		for i, want := range entries {
			g := got[i]
			wantPath := want.Path
			if want.IsDir && want.Path == "a/" {
				wantPath = "a" // 目录路径去尾斜杠（落库格式）
			}
			if g.Path != wantPath {
				t.Fatalf("[%d] 路径不符 (preserve=%v): got %q want %q", i, preserve, g.Path, wantPath)
			}
			if g.IsDir != want.IsDir || g.IsSymlink != want.IsSymlink {
				t.Fatalf("[%d] 类型不符 (preserve=%v): %+v", i, preserve, g)
			}
			if g.Mode != want.Mode {
				t.Fatalf("[%d] mode 不符 (preserve=%v): got %o want %o", i, preserve, g.Mode, want.Mode)
			}
			// 符号链接的 F_LENGTH = target 长度（协议语义，解析器读回 Size）
			wantSize := want.Size
			if want.IsSymlink {
				wantSize = int64(len(want.LinkTarget))
			}
			if g.Size != wantSize {
				t.Fatalf("[%d] size 不符 (preserve=%v): got %d want %d", i, preserve, g.Size, wantSize)
			}
			if g.MTimeNs != want.MTimeNs {
				t.Fatalf("[%d] mtime 不符 (preserve=%v): got %d want %d", i, preserve, g.MTimeNs, want.MTimeNs)
			}
			// preserve off 时 target 不发送，解析侧为空
			wantTarget := want.LinkTarget
			if !preserve {
				wantTarget = ""
			}
			if g.LinkTarget != wantTarget {
				t.Fatalf("[%d] link target 不符 (preserve=%v): got %q want %q", i, preserve, g.LinkTarget, wantTarget)
			}
			if preserve {
				if g.UID != want.UID || g.GID != want.GID {
					t.Fatalf("[%d] uid/gid 不符 (preserve=%v): got %d/%d want %d/%d",
						i, preserve, g.UID, g.GID, want.UID, want.GID)
				}
			}
		}
	}
}

// 结束哨兵：单字节 0。
func TestFlistWriterEndOfFlist(t *testing.T) {
	var buf bytes.Buffer
	if err := NewFlistWriter().WriteEndOfFlist(&buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{0}) {
		t.Fatalf("哨兵编码不符: %x", buf.Bytes())
	}
}

// id list 空段：varint 0。
func TestWriteIdList(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteIdList(&buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{0}) {
		t.Fatalf("id list 编码不符: %x", buf.Bytes())
	}
}
