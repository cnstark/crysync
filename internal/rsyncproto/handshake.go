// internal/rsyncproto/handshake.go
package rsyncproto

import (
	"bufio"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
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
// 服务端先发 greeting -> 读客户端 greeting（版本校验）-> 读模块行 -> #list/未知模块/认证 -> OK。
// 返回选定模块；#list 时写入模块清单并返回 ErrClientClosed（调用方关闭连接）。
func HandleModuleRequest(r *bufio.Reader, w io.Writer, cfg *config.Config) (*config.ModuleConfig, error) {
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
	DeleteMode     bool // argv 含 --delete*（filter 列表是否在网络上出现）
	PruneEmptyDirs bool // argv 含 --prune-empty-dirs/-m（同上）
	NumericIDs     bool // argv 含 --numeric-ids（id list 是否发送）
	SenderMode     bool // argv 含 --sender（恢复方向，服务端为 sender）
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
		case strings.HasPrefix(a, "-") && len(a) > 1:
			// 短选项包：逐字符；'e' 之后的剩余字符是 -e 的参数（client_info），停止扫描
			for _, c := range a[1:] {
				switch c {
				case 'o':
					neg.PreserveUID = true
				case 'g':
					neg.PreserveGID = true
				case 'm':
					neg.PruneEmptyDirs = true
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
