// internal/rsyncproto/flist_write.go
package rsyncproto

import (
	"fmt"
	"io"
)

// FlistWriter 编码 flist 条目（旧式 xflags 路径，compat_flags=0，protocol 28+）。
// 蓝本：flist.c send_file_entry（flist.c:380-680）。
//
// 压缩决策（与 sender 方向调研笔记 §6 一致）：
//   - 不用 SAME_MODE/SAME_TIME/SAME_NAME 增量压缩（恒发全字段、全名）；
//   - 但 !preserve_uid/gid 时**必须**置 SAME_UID/SAME_GID（接收侧据此决定
//     是否读 uid/gid 字段，位不置则字段流错位，flist.c:880-902）；
//   - MOD_NSEC（1<<13）在纳秒非零且 protocol>=31 时置位。
type FlistWriter struct{}

func NewFlistWriter() *FlistWriter { return &FlistWriter{} }

// WriteEntry 编码一条 flist 记录。目录路径去尾斜杠（落库格式）；普通目录
// （content dir）xflags 为 0——接收侧 p>=30 时无 NO_CONTENT_DIR 位即视为有内容
// 目录（flist.c:1081-1086）。
func (w *FlistWriter) WriteEntry(dst io.Writer, e FileEntry, preserveUID, preserveGID bool) error {
	var xflags uint32
	if !preserveUID {
		xflags |= XmitSameUID
	}
	if !preserveGID {
		xflags |= XmitSameGID
	}
	// MOD_NSEC：mtime 纳秒非零时置位（protocol>=31 恒成立）
	var nsec int64
	if e.MTimeNs != 0 {
		nsec = e.MTimeNs % 1e9
		if nsec != 0 {
			xflags |= XmitModNsec
		}
	}

	name := e.Path
	if e.IsDir {
		for len(name) > 0 && name[len(name)-1] == '/' {
			name = name[:len(name)-1]
		}
	}
	if name == "" {
		return fmt.Errorf("flist 条目缺少路径")
	}
	l2 := len(name)
	if l2 > 1<<20 {
		return fmt.Errorf("flist 名字过长: %d", l2)
	}

	// 写出规则（flist.c:551-558，非 varint 路径）：
	//   0 值兜底：非目录加 XMIT_TOP_DIR（无害 hack，避免 0 被当哨兵）；
	//   高字节非零或为 0 → EXTENDED_FLAGS + shortint；否则单字节。
	if xflags == 0 && !e.IsDir {
		xflags |= XmitTopDir
	}
	if xflags&0xFF00 != 0 || xflags == 0 {
		xflags |= XmitExtended
		if err := WriteShortint(dst, uint16(xflags)); err != nil {
			return err
		}
	} else if err := writeByte(dst, byte(xflags)); err != nil {
		return err
	}

	if err := writeByte(dst, byte(l2)); err != nil {
		return err
	}
	if _, err := dst.Write([]byte(name)); err != nil {
		return err
	}

	// F_LENGTH = varlong30(3)（64 位，大文件 >2GB 也正确）。
	// 符号链接的 F_LENGTH 是 target 长度（发送侧 send_file_name 设置，flist.c:639 同值）。
	size := e.Size
	if e.IsSymlink {
		size = int64(len(e.LinkTarget))
	}
	if err := WriteVarlong(dst, size, 3); err != nil {
		return err
	}
	// mtime 秒 = varlong(4)
	if err := WriteVarlong(dst, e.MTimeNs/1e9, 4); err != nil {
		return err
	}
	// MOD_NSEC = varint
	if nsec != 0 {
		if err := WriteVarint(dst, int32(nsec)); err != nil {
			return err
		}
	}
	// mode = int32（to_wire_mode 在 Linux 上恒等：_S_IFLNK==0120000）
	if err := WriteInt32(dst, int32(e.Mode)); err != nil {
		return err
	}
	if preserveUID {
		if err := WriteVarint(dst, int32(e.UID)); err != nil {
			return err
		}
	}
	if preserveGID {
		if err := WriteVarint(dst, int32(e.GID)); err != nil {
			return err
		}
	}
	// symlink target：varint30(len) + 字节（flist.c:639-642；varint30 = varint）
	if e.IsSymlink {
		if err := WriteVarint(dst, int32(len(e.LinkTarget))); err != nil {
			return err
		}
		if _, err := dst.Write([]byte(e.LinkTarget)); err != nil {
			return err
		}
	}
	return nil
}

// WriteEndOfFlist 写列表结束哨兵：非 varint 路径无错 → 单字节 0（flist.c:2086）。
func (w *FlistWriter) WriteEndOfFlist(dst io.Writer) error {
	_, err := dst.Write([]byte{0})
	return err
}

// WriteIdList 写 uid/gid 映射表空段：仅终止 varint 0（uidlist.c send_one_list，
// xmit_id0_names=0）。空段 = 数值直通，客户端按 flist 中的数值恢复。
func WriteIdList(dst io.Writer) error {
	return WriteVarint(dst, 0)
}

func writeByte(w io.Writer, b byte) error {
	_, err := w.Write([]byte{b})
	return err
}
