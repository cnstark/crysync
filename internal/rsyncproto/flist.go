package rsyncproto

import (
	"errors"
	"io"
	"sort"
	"strings"
)

// XMIT 标志（rsync.h，低 16 位）
const (
	XmitTopDir           = uint32(1 << 0)
	XmitSameMode         = uint32(1 << 1)
	XmitExtended         = uint32(1 << 2) // EXTENDED_FLAGS：xflags 2 字节格式标记
	XmitSameUID          = uint32(1 << 3)
	XmitSameGID          = uint32(1 << 4)
	XmitSameName         = uint32(1 << 5)
	XmitLongName         = uint32(1 << 6)
	XmitSameTime         = uint32(1 << 7)
	XmitSameRdevMajor    = uint32(1 << 8) // 设备条目专用（protocols 28+）：rdev major 复用上一设备条目
	XmitNoContentDir     = uint32(1 << 8) // 目录专用，protocol 30+（与 SAME_RDEV_MAJOR 同位不同义，按 mode 类型区分，rsync.h:57-58）
	XmitHlinked          = uint32(1 << 9)
	XmitUserNameFollows  = uint32(1 << 10) // 仅 inc_recurse 模式（本方案降级下不出现，防御性读取）
	XmitGroupNameFollows = uint32(1 << 11)
	XmitModNsec          = uint32(1 << 13) // protocol 31+
	XmitSameAtime        = uint32(1 << 14)
	XmitIoErrorEndlist   = uint32(1 << 12) // flist 结尾哨兵变体（protocols 31+，w/ EXTENDED）：后跟 varint io_error（rsync.h:65）
)

// S_IFMT 等类型位（stat(2)）
const (
	sIfmt   = 0o170000
	sIfDir  = 0o040000
	sIfLnk  = 0o120000
	sIfReg  = 0o100000
	sIfChr  = 0o020000
	sIfBlk  = 0o060000
	sIfFifo = 0o010000
	sIfSock = 0o140000
)

// ErrFlistEnd 表示 flist 单个 0 字节哨兵（列表结束）。
var ErrFlistEnd = errors.New("flist 结束哨兵")

// flist 排序（f_name_cmp，flist.c:3217，protocol≥29）：generator 按排序后的
// sorted 数组发 ndx（generator.c:2316-2322），故传输顺序必须与真实 rsync 一致。
//
// 忠实复刻 f_name_cmp 的 {s_DIR,s_SLASH,s_BASE,s_TRAILING}×{t_PATH,t_ITEM} 状态机：
//   - dirname/basename 按路径最后一个 '/' 切分（make_file，flist.c:1373-1383），
//     顶层（无 '/'）dirname=NULL、basename=整名；目录的尾斜杠仅在比较时虚拟合成（"/"）
//   - 顶层 "."：t_ITEM/s_TRAILING/空串 → 恒排最前
//   - 初始 type：有 dirname → t_PATH；无 dirname → 按 S_ISDIR 定 t_PATH/t_ITEM
//   - 段边界（basename 起点）按 S_ISDIR 重判 type；t_PATH 恒排 t_ITEM 后
//     （type1 != type2 时 return type1 == t_PATH ? 1 : -1）
type fncState int

const (
	s_DIR fncState = iota
	s_SLASH
	s_BASE
	s_TRAILING
)

type fncType int

const (
	t_PATH fncType = iota
	t_ITEM
)

// fNameCmp 比较两个 flist 条目（<0 = a 排 b 前）。直接翻译 flist.c:3217 的 f_name_cmp。
func fNameCmp(a, b FileEntry) int {
	// dirname/basename：按最后一个 '/' 切分（make_file，flist.c:1373-1383）
	adirS, abase := splitDirBase(a.Path)
	bdirS, bbase := splitDirBase(b.Path)

	var c1, c2 []byte
	var state1, state2 fncState
	var type1, type2 fncType

	if adirS == "" {
		type1 = isDirType(a) // 无 dirname：按 S_ISDIR 定 t_PATH/t_ITEM
		c1 = []byte(abase)
		if type1 == t_PATH && (len(c1) == 0 || (len(c1) == 1 && c1[0] == '.')) {
			// 顶层 "."（Path="" 是 cleanPath 归一化的 "."）：t_ITEM/s_TRAILING/空串 → 恒排最前
			type1 = t_ITEM
			state1 = s_TRAILING
			c1 = []byte("")
		} else {
			state1 = s_BASE
		}
	} else {
		type1 = t_PATH
		state1 = s_DIR
		c1 = []byte(adirS)
	}
	if bdirS == "" {
		type2 = isDirType(b)
		c2 = []byte(bbase)
		if type2 == t_PATH && (len(c2) == 0 || (len(c2) == 1 && c2[0] == '.')) {
			type2 = t_ITEM
			state2 = s_TRAILING
			c2 = []byte("")
		} else {
			state2 = s_BASE
		}
	} else {
		type2 = t_PATH
		state2 = s_DIR
		c2 = []byte(bdirS)
	}

	if type1 != type2 {
		if type1 == t_PATH {
			return 1
		}
		return -1
	}

	for {
		// c1 段耗尽 → 状态转换（flist.c:3268-3297）
		if len(c1) == 0 {
			switch state1 {
			case s_DIR:
				state1 = s_SLASH
				c1 = []byte("/")
			case s_SLASH:
				// 段边界按 S_ISDIR 重判 type（flist.c:3274-3283）
				type1 = isDirType(a)
				c1 = []byte(abase)
				if type1 == t_PATH && len(c1) == 1 && c1[0] == '.' {
					type1 = t_ITEM
					state1 = s_TRAILING
					c1 = []byte("")
				} else {
					state1 = s_BASE
				}
			case s_BASE:
				if type1 == t_PATH {
					state1 = s_TRAILING
					c1 = []byte("/")
				} else {
					// C 的 FALL THROUGH → s_TRAILING：type1 = t_ITEM
					state1 = s_TRAILING
					type1 = t_ITEM
				}
			case s_TRAILING:
				type1 = t_ITEM
			}
			if len(c2) > 0 && type1 != type2 {
				if type1 == t_PATH {
					return 1
				}
				return -1
			}
		}
		// c2 段耗尽 → 状态转换（flist.c:3298-3330）
		if len(c2) == 0 {
			switch state2 {
			case s_DIR:
				state2 = s_SLASH
				c2 = []byte("/")
			case s_SLASH:
				type2 = isDirType(b)
				c2 = []byte(bbase)
				if type2 == t_PATH && len(c2) == 1 && c2[0] == '.' {
					type2 = t_ITEM
					state2 = s_TRAILING
					c2 = []byte("")
				} else {
					state2 = s_BASE
				}
			case s_BASE:
				if type2 == t_PATH {
					state2 = s_TRAILING
					c2 = []byte("/")
				} else {
					// C 的 FALL THROUGH → s_TRAILING（flist.c:3314-3326）
					state2 = s_TRAILING
					if len(c1) == 0 {
						return 0
					}
					type2 = t_ITEM
				}
			case s_TRAILING:
				if len(c1) == 0 {
					return 0
				}
				type2 = t_ITEM
			}
			if type1 != type2 {
				if type1 == t_PATH {
					return 1
				}
				return -1
			}
		}
		// 逐字节比较（C：(dif = *c1++ - *c2++)，空指针按 '\0' 计）
		if len(c1) > 0 && len(c2) > 0 {
			d := int(c1[0]) - int(c2[0])
			c1 = c1[1:]
			c2 = c2[1:]
			if d != 0 {
				return d
			}
		} else if len(c1) > 0 {
			return int(c1[0])
		} else if len(c2) > 0 {
			return -int(c2[0])
		}
	}
}

// isDirType 按条目是否为目录返回 fncType（S_ISDIR → t_PATH else t_ITEM）。
func isDirType(e FileEntry) fncType {
	if e.IsDir {
		return t_PATH
	}
	return t_ITEM
}

// splitDirBase 返回 (dirname, basename)：按最后一个 '/' 切分；无 '/' → ("", 整名)。
func splitDirBase(path string) (string, string) {
	p := strings.TrimSuffix(path, "/")
	if i := strings.LastIndexByte(p, '/'); i > 0 {
		return p[:i], p[i+1:]
	}
	return "", p
}

// SortFlistEntries 按 f_name_cmp 重排 flist 条目（返回新切片，不修改入参）。
func SortFlistEntries(entries []FileEntry) []FileEntry {
	idx := make([]int, len(entries))
	for i := range idx {
		idx[i] = i
	}
	less := func(a, b int) bool { return fNameCmp(entries[idx[a]], entries[idx[b]]) < 0 }
	sort.SliceStable(idx, less)
	out := make([]FileEntry, len(entries))
	for i, j := range idx {
		out[i] = entries[j]
	}
	return out
}

// FileEntry 一条 flist 记录（解析后）。
type FileEntry struct {
	Path       string
	IsDir      bool
	IsSymlink  bool
	Mode       uint32
	UID        int
	GID        int
	Size       int64
	MTimeNs    int64
	LinkTarget string
	// 协议层判定（供 receiver 使用）：
	// Deleted：删除项（xflags 全 SAME 位 + 只含名字）
	// NeedsContent：是否有内容要传输（由 generator 决定，flist 阶段恒 false——
	//               由传输阶段的 iflags & ITEM_TRANSFER 决定）
}

// FlistParser 解析旧式 flist 记录流（xfer_flags_as_varint=0，protocol≥28 的 byte/shortint xflags），
// 跟踪名字公共前缀（lastname）与 SAME_* 增量压缩的上一条字段值。
type FlistParser struct {
	lastname  []byte
	lastMode  uint32 // SAME_MODE 复用上一条 mode
	lastMTime int64  // SAME_TIME 复用上一条 mtime
	lastUID   int    // SAME_UID 复用上一条 uid
	lastGID   int    // SAME_GID 复用上一条 gid
	// IoError：列表以 IO_ERROR_ENDLIST 哨兵结束时携带的发送侧 io_error 位
	// （调用方读取——rsync 语义：io_error 非零时接收侧禁用删除，generator.c）
	IoError int32
}

func NewFlistParser() *FlistParser { return &FlistParser{} }

// Parse 解析一条记录；返回条目。遇到单个 0 字节哨兵返回 ErrFlistEnd。
// preserve 四参数对齐客户端 argv（-o/-g/-l/-D）：真实 rsync 的 recv_file_entry
// 对 uid/gid/symlink target/rdev 的读取均有 preserve 前提（flist.c:880-902/925-929/
// 1023-1042）——preserve off 时对端置 SAME 位且不发字段，无条件读取会错位/阻塞。
// checksumMode 对齐 -c/--checksum（always_checksum）：每条 REGULAR 条目尾部附加
// flist_csum_len=16 字节纯内容 MD5（flist.c:1365-1377 recv 侧无条件读，目录/链接
// 不附）——v1 不使用其值，读掉保持流同步。
func (p *FlistParser) Parse(r io.Reader, preserveUID, preserveGID, preserveLinks, preserveDevices, preserveAtimes, checksumMode bool) (FileEntry, error) {
	var e FileEntry

	// xflags：单字节；0 → 列表结束；& XMIT_EXTENDED_FLAGS(1<<2) → 高字节 <<8
	b, err := readByte(r)
	if err != nil {
		return e, err
	}
	if b == 0 {
		return e, ErrFlistEnd
	}
	xflags := uint32(b)
	if xflags&XmitExtended != 0 {
		b2, err := readByte(r)
		if err != nil {
			return e, err
		}
		xflags = (xflags & 0xFF) | uint32(b2)<<8
		// IO_ERROR_ENDLIST 哨兵（flist.c:2960-2970 接收侧）：发送侧曾出错
		// （源消失/vanished 文件等）时 flist 结尾以 EXTENDED|IO_ERROR_ENDLIST
		// 短整型 + varint io_error 替代单字节 0——视同列表结束，错误值记入
		// IoError 供调用方处置（会话继续处理已收条目，与 rsync 一致）。
		if xflags == XmitExtended|XmitIoErrorEndlist {
			ioErr, err := ReadVarint(r)
			if err != nil {
				return e, err
			}
			p.IoError = ioErr
			return e, ErrFlistEnd
		}
		xflags &^= XmitExtended // 清除格式标记位
	}

	// 防御：v1 不支持硬链接
	if xflags&XmitHlinked != 0 {
		return e, protocolErr("v1 暂不支持硬链接 (XMIT_HLINKED)")
	}

	// 名字：公共前缀（SAME_NAME）+ 剩余字节
	l1 := 0
	if xflags&XmitSameName != 0 {
		b, err := readByte(r)
		if err != nil {
			return e, err
		}
		l1 = int(b)
		if int(l1) > len(p.lastname) {
			return e, errShortName
		}
		p.lastname = p.lastname[:int(l1)]
	}
	var l2 int
	if xflags&XmitLongName != 0 {
		// LONG_NAME 长度用 varint30（flist.c:720 read_varint30 = read_varint）
		v, err := ReadVarint(r)
		if err != nil {
			return e, err
		}
		l2 = int(v)
	} else {
		b, err := readByte(r)
		if err != nil {
			return e, err
		}
		l2 = int(b)
	}
	if l2 > 1<<20 {
		return e, errNameTooLong
	}
	name := make([]byte, l2)
	if _, err := io.ReadFull(r, name); err != nil {
		return e, err
	}
	fullName := string(name)
	if xflags&XmitSameName != 0 {
		fullName = string(p.lastname) + string(name)
		p.lastname = append(p.lastname, name...)
	} else {
		p.lastname = append(p.lastname[:0], name...)
	}
	e.Path = cleanPath(fullName)

	// F_LENGTH = varlong30(3)
	length, err := ReadVarlong(r, 3)
	if err != nil {
		return e, err
	}
	e.Size = int64(length)

	// [非 SAME_TIME] mtime = varlong(4)；SAME_TIME 复用上一条
	if xflags&XmitSameTime == 0 {
		t, err := ReadVarlong(r, 4)
		if err != nil {
			return e, err
		}
		e.MTimeNs = t * 1e9
		p.lastMTime = e.MTimeNs
	} else {
		e.MTimeNs = p.lastMTime
	}
	// [MOD_NSEC] nsec = varint（并入 MTimeNs：恢复方向按原样还原纳秒）
	if xflags&XmitModNsec != 0 {
		nsec, err := ReadVarint(r)
		if err != nil {
			return e, err
		}
		e.MTimeNs += int64(nsec)
	}
	// [非 SAME_MODE] mode = int32；SAME_MODE 复用上一条
	if xflags&XmitSameMode == 0 {
		m, err := ReadInt32(r)
		if err != nil {
			return e, err
		}
		e.Mode = uint32(m)
		p.lastMode = e.Mode
	} else {
		e.Mode = p.lastMode
	}
	// [--atimes 且非目录且非 SAME_ATIME] atime = varlong(4)（flist.c:985-986：
	// atimes_ndx && !S_ISDIR(mode) && !(xflags & XMIT_SAME_ATIME)，位置在 mode
	// 之后 uid 之前）。v1 不持久化 atime，读掉保持字段流同步。
	if preserveAtimes && e.Mode&sIfmt != sIfDir && xflags&XmitSameAtime == 0 {
		if _, err := ReadVarlong(r, 4); err != nil {
			return e, err
		}
	}
	// [preserve_uid 且非 SAME_UID] uid = varint；SAME_UID 或 preserve off 复用上一条；
	// [USER_NAME_FOLLOWS] len=byte + len 字节名字（与 uid 同前提，flist.c:890-902）
	if preserveUID && xflags&XmitSameUID == 0 {
		v, err := ReadVarint(r)
		if err != nil {
			return e, err
		}
		e.UID = int(v)
		p.lastUID = e.UID
		if xflags&XmitUserNameFollows != 0 {
			if err := skipName(r); err != nil {
				return e, err
			}
		}
	} else {
		e.UID = p.lastUID
	}
	// [preserve_gid 且非 SAME_GID] gid = varint；[GROUP_NAME_FOLLOWS]（flist.c:905-917）
	if preserveGID && xflags&XmitSameGID == 0 {
		v, err := ReadVarint(r)
		if err != nil {
			return e, err
		}
		e.GID = int(v)
		p.lastGID = e.GID
		if xflags&XmitGroupNameFollows != 0 {
			if err := skipName(r); err != nil {
				return e, err
			}
		}
	} else {
		e.GID = p.lastGID
	}
	// 文件类型白名单（flist.c:967-983 真实 rsync 接受全部标准类型）：
	// 普通文件（mt==0，mode 0 仅 delete-missing-args 用）、目录、链接、设备/特殊。
	// 设备/特殊条目 v1 不落库（receiver 跳过），但字段流照常解析。
	mt := e.Mode & sIfmt
	switch mt {
	case 0, sIfReg, sIfDir, sIfLnk, sIfChr, sIfBlk, sIfFifo, sIfSock:
	default:
		return e, protocolErr("非法文件类型 (mode)")
	}
	// [preserve_devices 且 CHR/BLK] rdev 段（flist.c:1023-1042 接收侧，protocol 31）：
	// !(XMIT_SAME_RDEV_MAJOR) 时 major=varint，minor 恒 varint；设备条目接收侧
	// file_length 清零。FIFO/SOCK（IS_SPECIAL）在 protocol 31 无任何 rdev 字节
	// （preserve_specials 的 rdev 段仅 protocol<31 出现）。v1 不使用 rdev 值，
	// 读掉保持流同步。
	if mt == sIfChr || mt == sIfBlk {
		e.Size = 0
		if preserveDevices {
			if xflags&XmitSameRdevMajor == 0 {
				if _, err := ReadVarint(r); err != nil { // major
					return e, err
				}
			}
			if _, err := ReadVarint(r); err != nil { // minor
				return e, err
			}
		}
	}
	// [preserve_links 且 symlink] target_len = varint30（flist.c:640 write_varint30，
	// 不含 '\0'）。接收侧 read_varint30 得 symlink_len，linkname_len = len+1 为缓冲
	// 大小，read_sbuf(f, bp, linkname_len-1) 实际读 len 字节（flist.c:929/1153）。
	// preserve_links off 时对端不发该段（flist.c:925 前提），IsSymlink 仍按 mode 判定。
	if preserveLinks && mt == sIfLnk {
		l, err := ReadVarint(r)
		if err != nil {
			return e, err
		}
		target := make([]byte, l)
		if _, err := io.ReadFull(r, target); err != nil {
			return e, err
		}
		e.LinkTarget = string(target)
	}
	e.IsSymlink = mt == sIfLnk
	e.IsDir = mt == sIfDir
	// [--checksum 且 REGULAR] 尾部 flist_csum_len=16 字节内容 MD5（flist.c:1365-1377，
	// recv_file_entry 之后 read_buf；v1 丢弃值仅保持流同步）
	if checksumMode && !e.IsDir && !e.IsSymlink && isRegularMode(e.Mode) {
		if _, err := io.ReadFull(r, make([]byte, 16)); err != nil {
			return e, err
		}
	}
	return e, nil
}

// skipName 读取并丢弃一个 NAME_FOLLOWS 名字段（len=byte + len 字节）。
func skipName(r io.Reader) error {
	ln, err := readByte(r)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(r, make([]byte, ln))
	return err
}

// ReadIdList 读取非增量模式 flist 哨兵之后的 uid/gid 映射表（A1，uidlist.c recv_id_list）。
// 每段：while ((id = read_varint30()) != 0) { len = read_byte(); 读 len 字节名字 }；
// 段以单个 varint 0 终止（空段也发一个 varint 0）。仅当 preserve 标志为真时存在对应段。
// v1 丢弃名字内容（uid/gid 数值已在 flist 记录里）。
func ReadIdList(r io.Reader, preserveUID, preserveGID bool) error {
	if preserveUID {
		if err := readOneseg(r); err != nil {
			return err
		}
	}
	if preserveGID {
		if err := readOneseg(r); err != nil {
			return err
		}
	}
	return nil
}

func readOneseg(r io.Reader) error {
	for {
		id, err := ReadVarint(r)
		if err != nil {
			return err
		}
		if id == 0 {
			return nil
		}
		if err := skipName(r); err != nil {
			return err
		}
	}
}

var (
	errShortName   = protocolErr("名字公共前缀超过上一条长度")
	errNameTooLong = protocolErr("名字过长")
)

type protocolError struct{ msg string }

func (e *protocolError) Error() string { return e.msg }

func protocolErr(msg string) error { return &protocolError{msg: msg} }

// readByte 读取单个字节。
func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

// cleanPath 安全路径：拒绝空路径、绝对路径与 ".." 段；保留原始字节（含尾斜杠——目录名的协议语义）。
// 返回 "" 表示非法路径（调用方对空路径报错）。
func cleanPath(p string) string {
	if p == "" || p == "." || strings.HasPrefix(p, "/") {
		return ""
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return ""
		}
	}
	return p
}
