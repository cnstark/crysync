// internal/front/rsync/protocol/handshake.go
package protocol

import (
	"bufio"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"crysync/internal/config"
)

// greetingVersion：服务端宣告的协议版本（daemon 模式经 greeting 文本协商，
// 之后无二进制版本交换--remote_protocol != 0 时 compat.c:599 不走 write_int 分支）。
const greetingVersion = "31.0"

// greetingDigestList：greeting 第三段的 daemon-auth 校验和列表。
// 客户端从服务端列表中选第一个双方共有的算法（compat.c:857）；列表必须与
// authDigest 实现一致--v1 仅实现 md5，故列表固定 "md5"。
const greetingDigestList = "md5"

// WriteGreeting 发送服务端 greeting（连接后立即主动发送，与读取无关：
// clientserver.c:155 exchange_protocols 双方各自先发后读）。
func WriteGreeting(w io.Writer) error {
	_, err := fmt.Fprintf(w, "@RSYNCD: %s %s\n", greetingVersion, greetingDigestList)
	return err
}

var ErrClientClosed = errors.New("客户端已断开")
var ErrAuthFailed = errors.New("认证失败")

// HandleModuleRequest 处理 daemon 文本行阶段：
// 服务端先发 greeting -> 读客户端 greeting（版本校验）-> 读模块行 -> #list/未知模块/
// max connections 检查/认证 -> OK。
// 返回选定模块；#list 时写入模块清单并返回 ErrClientClosed（调用方关闭连接）。
// allowModule 非 nil 时在模块选定后、认证前调用（对齐 rsyncd 时序：clientserver.c:791
// claim_connection 在 allow_access 之后 auth_server 之前）：返回 false 表示连接数
// 超限/模块禁用，向客户端写 @ERROR 文本行（客户端以 code 5 退出）后拒绝。
func HandleModuleRequest(r *bufio.Reader, w io.Writer, cfg *config.Config, allowModule func(*config.ModuleConfig) bool) (*config.ModuleConfig, error) {
	// (a) 服务端 greeting 必须最先发出（不等待客户端）
	if err := WriteGreeting(w); err != nil {
		return nil, err
	}
	// 读客户端 greeting：@RSYNCD: <major>.<minor> <digest列表>
	cg, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if err := checkClientGreeting(cg); err != nil {
		fmt.Fprintf(w, "@ERROR: protocol startup error\n")
		return nil, err
	}
	// (c) 模块行
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	// 空模块名 = 列模块请求：rsync 3.4.1 客户端执行 "rsync host::" 时发送空模块名行
	// （非 "#list" 字面量），真实 rsyncd 对此返回 list 模块清单（clientserver.c
	// module_list_request，无需认证）；行为与 #list 分支一致，实测 rsyncd 3.4.1 验证。
	if line == "#list" || strings.TrimSpace(line) == "" {
		for _, m := range cfg.Modules {
			fmt.Fprintf(w, "%s\t%s\n", m.Name, m.Path)
		}
		fmt.Fprint(w, "@RSYNCD: EXIT\n")
		return nil, ErrClientClosed
	}
	name := strings.TrimSpace(line)
	if strings.HasPrefix(name, "#") {
		fmt.Fprintf(w, "@ERROR: Unknown command '%s'\n", name)
		return nil, fmt.Errorf("未知命令 %s", name)
	}
	var module *config.ModuleConfig
	for i := range cfg.Modules {
		if cfg.Modules[i].Name == name {
			module = &cfg.Modules[i]
			break
		}
	}
	if module == nil {
		fmt.Fprintf(w, "@ERROR: Unknown module '%s'\n", name)
		return nil, fmt.Errorf("未知模块 %s", name)
	}
	// max connections 检查（rsyncd 文本行，clientserver.c:796-799 io_printf）：
	// 超限/负值禁用时客户端原样打印该行并以 RERR_STARTCLIENT=5 退出
	if allowModule != nil && !allowModule(module) {
		fmt.Fprintf(w, "@ERROR: max connections (%d) reached -- try again later\n", module.MaxConnections)
		return nil, fmt.Errorf("模块 %s 连接数达到上限 (%d)", module.Name, module.MaxConnections)
	}
	// (d,e,f) 认证：配置存在认证用户时要求 challenge-response（备份工具单用户模型，
	// 用户名与模块名相互独立，口令按用户名查）
	if len(cfg.Auth.Users) > 0 {
		seed, err := randomSeed()
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(w, "@RSYNCD: AUTHREQD %s\n", seed)
		authLine, err := readLine(r)
		if err != nil {
			return nil, err
		}
		parts := strings.Fields(authLine)
		if len(parts) != 2 {
			fmt.Fprintf(w, "@ERROR: auth failed on module %s\n", name)
			return nil, ErrAuthFailed
		}
		user, digest := parts[0], parts[1]
		pass, ok := cfg.Auth.Users[user]
		if !ok || digest != authDigest(pass, seed) {
			fmt.Fprintf(w, "@ERROR: auth failed on module %s\n", name)
			return nil, ErrAuthFailed
		}
	}
	fmt.Fprint(w, "@RSYNCD: OK\n")
	return module, nil
}

// checkClientGreeting 校验客户端 greeting 的版本与 daemon-auth digest 列表：
// 版本须 >= 30（rl_nulls、varint 等前提）；digest 列表须含 md5（v1 仅支持 md5 认证）。
func checkClientGreeting(line string) error {
	if !strings.HasPrefix(line, "@RSYNCD: ") {
		return fmt.Errorf("非法 greeting: %q", line)
	}
	fields := strings.Fields(strings.TrimPrefix(line, "@RSYNCD: "))
	if len(fields) < 1 {
		return fmt.Errorf("greeting 缺少版本: %q", line)
	}
	major := strings.SplitN(fields[0], ".", 2)[0]
	var v int
	if _, err := fmt.Sscanf(major, "%d", &v); err != nil || v < 30 {
		return fmt.Errorf("不支持的协议版本: %q", fields[0])
	}
	// digest 列表（v1 服务端 greeting 只列 md5，客户端从服务端列表选 md5；
	// 此处校验客户端能力，避免认证必然失败才报错）
	for _, f := range fields[1:] {
		if f == "md5" {
			return nil
		}
	}
	return fmt.Errorf("客户端不支持 md5 认证摘要: %q", line)
}

func VerifyAuth(user, seed, password, digest string) bool {
	return authDigest(password, seed) == digest
}

// authDigest 计算 rsync 认证摘要：base64_nopad(MD5(password || seed))。
// 密码在前、challenge（seed）在后、不含用户名；base64 无 padding
// （authenticate.c:85-95 generate_hash / base64_encode pad=0）。
func authDigest(password, seed string) string {
	h := md5.Sum([]byte(password + seed))
	return base64.RawStdEncoding.EncodeToString(h[:])
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\n"), nil
}

// randomSeed 生成认证 seed：任意非空字符串，客户端原样拼入 digest
// （authenticate.c gen_challenge 为 addr+time+pid 哈希的 base64，长度与格式不影响两端一致性）。
func randomSeed() (string, error) {
	b := make([]byte, 4)
	if _, err := randRead(b); err != nil {
		return "", err
	}
	n := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	n &= 0x7FFFFFFF // 非负
	return fmt.Sprintf("%d", n), nil
}

// ReadArgvLine 读取客户端参数序列：每个参数 NUL 结尾，双 NUL 表示结束
// （clientserver.c:393-400 rl_nulls，protocol>=30 恒启用）。
func ReadArgvLine(r *bufio.Reader) ([]string, error) {
	var args []string
	for {
		s, err := r.ReadString(0)
		if err != nil {
			return nil, err
		}
		s = strings.TrimSuffix(s, "\x00")
		if s == "" {
			// 空参数 = 序列结束（双 NUL）
			return args, nil
		}
		if strings.HasPrefix(s, "@") {
			return nil, fmt.Errorf("意外协议参数: %s", s)
		}
		args = append(args, s)
	}
}

// Negotiation 二进制协商结果（mux 启动前 raw 阶段完成）。
type Negotiation struct {
	PreserveUID    bool // argv 含 -o（uid/gid 映射表 id list 是否存在）
	PreserveGID    bool // argv 含 -g
	PreserveLinks  bool // argv 含 -l（flist symlink target 字段段是否出现）
	PreserveDevices bool // argv 含 -D/--devices（CHR/BLK 条目 rdev 字段段是否出现；--specials 无 wire 影响）
	Recurse        bool // argv 含 -r（flist 深度：false 时仅传输根下一层，--list-only 无 -r 等）
	ChecksumMode   bool // argv 含 -c/--checksum（always_checksum：flist 每条 REGULAR 条目尾部附 16 字节内容 MD5）
	PreserveAtimes bool // argv 组合短选项含 'U'（--atimes）：非目录条目 mode 后有 varlong(4) atime 字段（v1 读掉丢弃，不落库）
	AppendMode     int  // argv 独立项 "--append" 出现次数（server_options：--append 发 1 个、--append-verify 发 2 个；v1 不支持须拒绝）
	Compression    bool // argv 含 -z/--old-compress/--new-compress/--compress-choice（v1 不支持，须拒绝）
	DeleteMode     bool // argv 含 --delete*（filter 列表是否在网络上出现）
	PruneEmptyDirs bool // argv 含 --prune-empty-dirs/-m（同上）
	NumericIDs     bool // argv 含 --numeric-ids（id list 是否发送）
	SenderMode     bool // argv 含 --sender（恢复方向，服务端为 sender）
	DryRun         bool // argv 短包含 'n'（options.c:2800 !do_xfers，唯一来源 dry_run）：传输请求只回显/只发 ndx+iflags，不传数据不落库
	IoTimeout      int  // argv 含 --timeout=N（秒）：会话空闲超时，0 = 无（daemon 模式下客户端 server_options 会透传给服务端）
	ChecksumSeed   int32
	ModuleArg      string
	Argv           []string // 原始客户端参数（日志/调试用）
}

// CF_* compat flags（compat.c:117，服务端单方面发送，客户端据此决定会话行为）。
// v1 裁决（修复方案 §0）：compat_flags = 0 保守降级--客户端走旧式 flist 标志、
// 非增量 flist、无字符串协商（校验和 fallback md5，xfer_sum_len=16）。
const (
	cfIncRecurse        = uint32(1 << 0)
	cfSymlinkTimes      = uint32(1 << 1)
	cfSymlinkIconv      = uint32(1 << 2)
	cfSafeFlist         = uint32(1 << 3)
	cfAvoidXattrOptim   = uint32(1 << 4)
	cfChksumSeedFix     = uint32(1 << 5)
	cfInplacePartialDir = uint32(1 << 6)
	cfVarintFlistFlags  = uint32(1 << 7)
	cfID0Names          = uint32(1 << 8)
)

// NegotiateBinary 完成 argv 读取与二进制协商（daemon 协议，mux 启动前的 raw 字节）：
// 读 argv（NUL 分隔）-> 解析选项 -> 写 compat_flags varint（v1 恒 0）-> 写 checksum_seed int32。
// 无版本 int（greeting 已协商）、无 mux 标记字节（隐式）、无字符串协商（compat=0 不含 v 位）。
func NegotiateBinary(r *bufio.Reader, w io.Writer) (*Negotiation, error) {
	argv, err := ReadArgvLine(r)
	if err != nil {
		return nil, err
	}
	if len(argv) == 0 || argv[0] != "--server" {
		return nil, fmt.Errorf("非法参数行: %v", argv)
	}
	neg := parseServerArgs(argv)
	neg.Argv = argv

	// compat_flags（varint；0 = 单字节 0x00）
	if err := WriteVarint(w, int32(0)); err != nil {
		return nil, err
	}
	// checksum_seed（int32 小端，compat.c:813 协议>=30 恒发送；值同时用于整文件
	// 强校验和 MD5(content‖seed) 与块校验和，见 sender 方向调研笔记 §4）
	seed := int32(time.Now().UnixNano() & 0x7FFFFFFF)
	if err := WriteInt32(w, seed); err != nil {
		return nil, err
	}
	neg.ChecksumSeed = seed
	return neg, nil
}

// rejectCompression 检测压缩选项并拒绝会话（mux 建立后调用）：v1 未实现 zlib
// 解压（压缩 token 流为 deflate 编码，与普通 int32 token 流完全不同，误当普通
// 流解析必然错乱崩溃）。错误经 mux MSG_ERROR_XFER 发送，客户端 stderr 显示。
func rejectCompression(mw *MuxWriter, neg *Negotiation) error {
	if !neg.Compression {
		return nil
	}
	_ = mw.WriteMsg("ERROR: compression is not supported by this server; remove -z/--compress options\n")
	return fmt.Errorf("客户端请求压缩传输（-z/--compress-choice），暂不支持")
}

// rejectWait 拒绝类错误返回前等待（对齐 rsyncd cleanup 的 noop_io_until_death）：
// 错误帧已写出，立即关闭连接会 RST 丢弃客户端未读缓冲。err 为 nil 时原样返回。
func rejectWait(err error) error {
	if err != nil {
		time.Sleep(time.Second)
	}
	return err
}

// rejectWithExit 拒绝路径三连帧（对齐 rsyncd exit_cleanup 链）：错误文本 +
// log_exit 行（log.c:955-957 经 MSG_ERROR 送达客户端）+ MSG_ERROR_EXIT(code)
//（cleanup.c send_msg_int，payload 4 字节 LE，客户端以该码退出）。
func rejectWithExit(mw *MuxWriter, text string, code int32) {
	_ = mw.writeFrame(msgError, []byte(text))
	_ = mw.writeFrame(msgError, []byte(fmt.Sprintf("rsync error: syntax or usage error (code %d) [Receiver=crysync]\n", code)))
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(code))
	_ = mw.writeFrame(msgErrorExit, b[:])
}

// rejectAppend 检测 --append/--append-verify 并拒绝会话：append 语义是"数据从客户端
// 已有偏移开始"（receiver offset=sum.flength），与本项目重组假设"从 0 全量重组"冲突，
// 误接受会产生错误数据。经 MSG_ERROR(3) 告知客户端（用法错误不计 io_error）。
func rejectAppend(mw *MuxWriter, neg *Negotiation) error {
	if neg.AppendMode == 0 {
		return nil
	}
	rejectWithExit(mw, "ERROR: --append is not supported by this server; remove --append (or --append-verify) options\n", 1)
	return fmt.Errorf("客户端请求 --append/--append-verify 增量追加传输，暂不支持")
}

// rejectRestoreAtimes 恢复方向（服务端为 sender）拒绝 --atimes：atime 未持久化，
// 无法在 flist 中提供该字段；不拒绝会导致客户端解码错位。备份方向不拒绝
// （flist 解析读掉 atime 字段保持流同步，值不落库）。
func rejectRestoreAtimes(mw *MuxWriter, neg *Negotiation) error {
	if !neg.SenderMode || !neg.PreserveAtimes {
		return nil
	}
	rejectWithExit(mw, "ERROR: --atimes is not supported on restore; remove -U/--atimes options\n", 1)
	return fmt.Errorf("客户端恢复方向请求 --atimes，快照未持久化 atime，暂不支持")
}

// parseServerArgs 从服务端 argv 解析会话相关选项。
// 真实客户端示例：["--server","--sender","-vlogDtpre.iLsfxCIvu",".","home/"]。
// 短选项包中 'e' 带参数（其后 '.'+FLAGS 为 client_info），'o'/'g' 表示 preserve uid/gid。
func parseServerArgs(argv []string) *Negotiation {
	neg := &Negotiation{}
	for _, a := range argv[1:] {
		switch {
		case a == "--server":
			continue
		case a == "--sender":
			neg.SenderMode = true
		case strings.HasPrefix(a, "--"):
			if a == "--delete" || strings.HasPrefix(a, "--delete-") {
				neg.DeleteMode = true
			}
			if a == "--prune-empty-dirs" {
				neg.PruneEmptyDirs = true
			}
			if a == "--numeric-ids" {
				neg.NumericIDs = true
			}
			// --timeout=N：客户端 io_timeout 透传（server_options 生成 --timeout=N
			// 单项），daemon 侧同样启用空闲超时（io.c check_timeout）
			if strings.HasPrefix(a, "--timeout=") {
				if v, err := strconv.Atoi(strings.TrimPrefix(a, "--timeout=")); err == nil && v > 0 {
					neg.IoTimeout = v
				}
			}
			if a == "--checksum" {
				neg.ChecksumMode = true
			}
			// 设备文件保留（options.c:2843-2844，-D/--devices；--specials 在
			// protocol 31 无 wire 影响，特殊条目本就无 rdev 段）
			if a == "--devices" {
				neg.PreserveDevices = true
			}
			// 压缩：短包 'z'（CPRES_ZLIB，options.c:2888-2889）或独立长选项
			// --old-compress/--new-compress/--compress-choice（options.c:2984-2989）；
			// --compress-level 仅在已开压缩时随 argv 发送（options.c:2921），一并防御。
			if a == "--old-compress" || a == "--new-compress" ||
				strings.HasPrefix(a, "--compress-choice") || strings.HasPrefix(a, "--compress-level") {
				neg.Compression = true
			}
			// --append：精确全等匹配（options.c:3122-3125，server_options 生成独立项；
			// --append-verify 在 wire 上是两个连续 "--append"，不出现 "--append-verify"
			// 字面量）。服务端 OPT_APPEND 每见一次 append_mode++（两次复原 verify 语义）。
			if a == "--append" {
				neg.AppendMode++
			}
		case strings.HasPrefix(a, "-") && len(a) > 1:
			// 短选项包：逐字符；'e' 之后的剩余字符是 -e 的参数（client_info），停止扫描
			for _, c := range a[1:] {
				switch c {
				case 'o':
					neg.PreserveUID = true
				case 'g':
					neg.PreserveGID = true
				case 'l':
					neg.PreserveLinks = true
				case 'D':
					neg.PreserveDevices = true // -D = --devices --specials（options.c:2843-2844）
				case 'r':
					neg.Recurse = true
				case 'c':
					neg.ChecksumMode = true
				case 'z':
					neg.Compression = true
				case 'm':
					neg.PruneEmptyDirs = true
				case 'U':
					neg.PreserveAtimes = true // --atimes（options.c:2847-2851；两次 -U 为 "UU"）
				case 'n':
					neg.DryRun = true // dry_run（options.c:2800，!do_xfers；server_options 不生成独立长选项）
				case 'e':
					goto nextArg
				}
			}
		default:
			neg.ModuleArg = a // 位置参数：最后一个即模块路径（如 "home/"）
		}
	nextArg:
	}
	return neg
}
