// internal/rsyncproto/handshake_test.go
package rsyncproto

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"crysync/internal/config"
)

// clientGreeting 真实 rsync 3.4.1 客户端的 greeting（含 daemon-auth digest 列表）。
const clientGreeting = "@RSYNCD: 32.0 sha512 sha256 sha1 md5 md4\n"

func TestWriteGreeting(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteGreeting(&buf); err != nil {
		t.Fatal(err)
	}
	// 服务端 greeting：版本 + daemon-auth digest 列表（v1 仅 md5，与 authDigest 实现一致）
	if buf.String() != "@RSYNCD: 31.0 md5\n" {
		t.Fatalf("greeting 错误: %q", buf.String())
	}
}

func TestHandleModuleList(t *testing.T) {
	cfg := &config.Config{Modules: []config.ModuleConfig{
		{Name: "home", Path: "/"},
		{Name: "www", Path: "/srv/www"},
	}}
	r := bufio.NewReader(strings.NewReader(clientGreeting + "#list\n"))
	var buf bytes.Buffer
	if _, err := HandleModuleRequest(r, &buf, cfg); err == nil {
		t.Fatal("#list 应返回错误（调用方关闭连接）")
	}
	if !strings.Contains(buf.String(), "home\t/") || !strings.Contains(buf.String(), "www\t/srv/www") ||
		!strings.Contains(buf.String(), "@RSYNCD: EXIT") {
		t.Fatalf("#list 响应错误: %q", buf.String())
	}
	// 服务端 greeting 必须最先发出（与读取无关）
	if !strings.HasPrefix(buf.String(), "@RSYNCD: 31.0 md5\n") {
		t.Fatalf("服务端应先发 greeting: %q", buf.String())
	}
}

// TestHandleModuleListEmptyName 空模块名 = 列模块请求：rsync 3.4.1 客户端
// "rsync host::" 发送空模块名行（真实 rsyncd 据此返回模块清单）。
func TestHandleModuleListEmptyName(t *testing.T) {
	cfg := &config.Config{Modules: []config.ModuleConfig{
		{Name: "home", Path: "/"},
	}}
	for _, line := range []string{"\n", "  \n"} { // 空行与纯空白行
		r := bufio.NewReader(strings.NewReader(clientGreeting + line))
		var buf bytes.Buffer
		if _, err := HandleModuleRequest(r, &buf, cfg); err == nil {
			t.Fatalf("空模块名 %q 应返回错误（调用方关闭连接）", line)
		}
		if !strings.Contains(buf.String(), "home\t/") || !strings.Contains(buf.String(), "@RSYNCD: EXIT") {
			t.Fatalf("空模块名 %q 响应错误: %q", line, buf.String())
		}
	}
}

func TestHandleModuleAuthFlow(t *testing.T) {
	cfg := &config.Config{Auth: config.AuthConfig{Users: map[string]string{"backup": "secret"}},
		Modules: []config.ModuleConfig{{Name: "home", Path: "/"}}}

	// 固定 seed 生成器：4 字节 0x12 -> 十进制 "303174162"（0x12121212 & 0x7FFFFFFF）
	// （服务端 seed 是十进制数字字符串，测试必须覆写 randRead seam 才能预知 digest）
	orig := randRead
	randRead = func(b []byte) (int, error) {
		for i := range b {
			b[i] = 0x12
		}
		return len(b), nil
	}
	defer func() { randRead = orig }()

	// 认证通过：base64_nopad(MD5("secret" + "303174162"))
	seed := "303174162"
	digest := authDigest("secret", seed)
	r := bufio.NewReader(strings.NewReader(clientGreeting + "home\nbackup " + digest + "\n"))
	var buf bytes.Buffer
	m, err := HandleModuleRequest(r, &buf, cfg)
	if err != nil {
		t.Fatalf("认证失败: %v", err)
	}
	if m.Name != "home" {
		t.Fatalf("模块错误: %v", m.Name)
	}
	if !strings.Contains(buf.String(), "@RSYNCD: AUTHREQD") || !strings.HasSuffix(buf.String(), "@RSYNCD: OK\n") {
		t.Fatalf("认证握手响应错误: %q", buf.String())
	}
}

func TestHandleModuleAuthReject(t *testing.T) {
	cfg := &config.Config{Auth: config.AuthConfig{Users: map[string]string{"backup": "secret"}},
		Modules: []config.ModuleConfig{{Name: "home", Path: "/"}}}
	seed := "12345678"
	r := bufio.NewReader(strings.NewReader(clientGreeting + "home\nbackup " + authDigest("WRONG", seed) + "\n"))
	var buf bytes.Buffer
	if _, err := HandleModuleRequest(r, &buf, cfg); err == nil {
		t.Fatal("错误密码应报错")
	}
	if !strings.Contains(buf.String(), "@ERROR") {
		t.Fatalf("应返回 @ERROR: %q", buf.String())
	}
}

func TestHandleModuleUnknown(t *testing.T) {
	cfg := &config.Config{Modules: []config.ModuleConfig{{Name: "home", Path: "/"}}}
	r := bufio.NewReader(strings.NewReader(clientGreeting + "nope\n"))
	var buf bytes.Buffer
	if _, err := HandleModuleRequest(r, &buf, cfg); err == nil {
		t.Fatal("未知模块应报错")
	}
	if !strings.Contains(buf.String(), "@ERROR: Unknown module") {
		t.Fatalf("应返回 Unknown module: %q", buf.String())
	}
}

func TestHandleModuleRejectsOldClient(t *testing.T) {
	// 协议 <30 的客户端（无 NUL argv/varint 前提）应被拒绝
	cfg := &config.Config{Modules: []config.ModuleConfig{{Name: "home", Path: "/"}}}
	r := bufio.NewReader(strings.NewReader("@RSYNCD: 29.0 md5\nhome\n"))
	var buf bytes.Buffer
	if _, err := HandleModuleRequest(r, &buf, cfg); err == nil {
		t.Fatal("旧协议客户端应报错")
	}
	if !strings.Contains(buf.String(), "@ERROR: protocol startup error") {
		t.Fatalf("应返回 protocol startup error: %q", buf.String())
	}
}

func TestVerifyAuth(t *testing.T) {
	digest := authDigest("secret", "12345678")
	if !VerifyAuth("backup", "12345678", "secret", digest) {
		t.Fatal("正确密码应通过")
	}
	if VerifyAuth("backup", "12345678", "nope", digest) {
		t.Fatal("错误密码应失败")
	}
}

func TestReadArgvLine(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("--server\x00--sender\x00-vlogDtpre.iLsfxCIvu\x00.\x00home/\x00\x00"))
	args, err := ReadArgvLine(r)
	if err != nil || len(args) != 5 || args[0] != "--server" || !contains(args, "--sender") || args[4] != "home/" {
		t.Fatalf("argv 解析错误: %v %v", args, err)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestNegotiateBinary(t *testing.T) {
	var in bytes.Buffer
	in.WriteString("--server\x00--sender\x00-vlogDtpre.iLsfxCIvu\x00.\x00home/\x00\x00")

	var out bytes.Buffer
	neg, err := NegotiateBinary(bufio.NewReader(&in), &out)
	if err != nil {
		t.Fatal(err)
	}
	// argv 解析：-vlogDtpre.iLsfxCIvu 中 'e' 前有 'o''g'（preserve uid/gid）；'e' 之后是 client_info
	if !neg.PreserveUID || !neg.PreserveGID {
		t.Fatalf("preserve 解析错误: %+v", neg)
	}
	if neg.DeleteMode || neg.PruneEmptyDirs {
		t.Fatalf("不应有 delete/prune: %+v", neg)
	}
	if neg.ModuleArg != "home/" {
		t.Fatalf("模块参数错误: %+v", neg)
	}
	// 服务端输出：compat_flags varint（v1 恒 0）+ checksum_seed int32，共 5 字节
	if out.Len() != 5 {
		t.Fatalf("协商输出应恰好 5 字节（varint 0 + int32 seed），实际 %d", out.Len())
	}
	br := bufio.NewReader(bytes.NewReader(out.Bytes()))
	flags, err := ReadVarint(br)
	if err != nil {
		t.Fatal(err)
	}
	if flags != 0 {
		t.Fatalf("v1 compat_flags 应为 0（保守降级）: %d", flags)
	}
	seed, err := ReadInt32(br)
	if err != nil {
		t.Fatal(err)
	}
	_ = seed // 值随机，仅需存在且为 4 字节
}

// rsync 3.4+ 双向 greeting：客户端发版本行 -> 服务端已先发自己的版本行 -> 客户端发模块名
func TestHandleModuleRequest34Greeting(t *testing.T) {
	cfg := &config.Config{Modules: []config.ModuleConfig{{Name: "home", Path: "/"}}}
	r := bufio.NewReader(strings.NewReader(clientGreeting + "home\n"))
	var buf bytes.Buffer
	m, err := HandleModuleRequest(r, &buf, cfg)
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	if m.Name != "home" {
		t.Fatalf("模块错误: %v", m.Name)
	}
	// 服务端 greeting 最先发出（带 digest 列表），会话以 OK 结束
	if !strings.HasPrefix(buf.String(), "@RSYNCD: 31.0 md5\n") {
		t.Fatalf("服务端应先发 greeting: %q", buf.String())
	}
	if !strings.HasSuffix(buf.String(), "@RSYNCD: OK\n") {
		t.Fatalf("应以 OK 结束: %q", buf.String())
	}
}

func TestNegotiateBinaryRejectsServerFlag(t *testing.T) {
	// 客户端参数必须以 --server 开头
	in := bufio.NewReader(bytes.NewBufferString("-v\x00.\x00home/\x00\x00"))
	if _, err := NegotiateBinary(in, &bytes.Buffer{}); err == nil {
		t.Fatal("缺少 --server 应报错")
	}
}

func TestParseServerArgs(t *testing.T) {
	// 典型 -a 推送：preserve uid/gid、无 delete
	neg := parseServerArgs([]string{"--server", "--sender", "-vlogDtpre.iLsfxCIvu", ".", "home/"})
	if !neg.PreserveUID || !neg.PreserveGID || neg.DeleteMode || neg.ModuleArg != "home/" {
		t.Fatalf("-a 解析错误: %+v", neg)
	}
	// --delete + 长选项 + -m
	neg = parseServerArgs([]string{"--server", "-vltD", "--delete-during", "--prune-empty-dirs", ".", "home/"})
	if !neg.DeleteMode || !neg.PruneEmptyDirs {
		t.Fatalf("delete 解析错误: %+v", neg)
	}
	// -e 的参数（client_info）中的字母不得误认为选项："-vgeXX" 应只识别 e 前的 o/g
	neg = parseServerArgs([]string{"--server", "-voge.iLsfxCIvu", ".", "home/"})
	if !neg.PreserveUID || !neg.PreserveGID {
		t.Fatalf("-e 参数截断解析错误: %+v", neg)
	}
}
