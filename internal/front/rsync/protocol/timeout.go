// internal/front/rsync/protocol/timeout.go
// io_timeout 支持（对齐 rsyncd 语义，源码核实 rsync 3.4.1 + 真实 rsyncd 实验）：
//   - 客户端 argv 含 --timeout=N（server_options 透传，options.c:2957-2962）时，
//     daemon 侧同样启用空闲超时（io.c check_timeout）。
//   - idle 语义：读写任一方向有帧流动即刷新计时，now - MAX(lastRead,lastWrite)
//     >= N 才超时（io.c:243-244）——不是会话总时长，数据持续流动永不超时。
//   - 协商完成后 daemon 立即主动发 MSG_IO_TIMEOUT(33) 帧告知客户端自己的超时值
//     （main.c start_server，payload = 4 字节 LE int32 秒数；客户端收到后只缩
//     不放大本地 --timeout，io.c:1712-1732）。真实 rsyncd 实验验证：
//     timeout=3 时帧字节 04 00 00 28 03 00 00 00。
//   - 服务端本地慢工作（分块/加密/慢后端 Put，期间不读不写 socket）由干活点
//     显式 KeepAlive 防对端误断：续期自身 deadline 并发空 MSG_DATA 帧（len=0，
//     对齐 io.c maybe_send_keepalive 的空帧心跳）。注意不能常驻心跳——服务端
//     等读（客户端挂死）时心跳会无限续期，真实 rsyncd 实验证实等读场景服务端
//     在数倍 timeout 内断连（[Receiver] io timeout after N seconds -- exiting）。
//   - 超时触发：rprintf(FERROR)（MSG_ERROR code 3）文本 "[server] io timeout
//     after %d seconds -- exiting" 后直接断连退出（io.c:248，cleanup.c 对
//     RERR_TIMEOUT 特判不再发 MSG_ERROR_EXIT）。
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

// 协议消息码（rsync.h:294-308）。
const (
	msgIoTimeout  byte = 33 // MSG_IO_TIMEOUT：daemon 宣告自身超时值（非超时告警）
	msgError      byte = 3  // MSG_ERROR：FERROR 级错误文本（stderr，不计 io_error）
	msgErrorExit  byte = 86 // MSG_ERROR_EXIT：payload 4 字节 LE exit code，客户端收到后以该码退出（read_only 等用法错误路径）
)

// maxSessionDuration：未协商 --timeout 时的连接级兜底上限（server 层同值）。
const maxSessionDuration = 24 * time.Hour

// idleKeeper：空闲超时管理。mux 帧成功读/写即重置连接 deadline（读写双向）；
// 服务端本地干活点（块存储循环）调用 KeepAlive 续期并向客户端发空帧心跳。
type idleKeeper struct {
	conn net.Conn
	d    time.Duration
	mw   *MuxWriter
}

func (k *idleKeeper) touch() { _ = k.conn.SetDeadline(time.Now().Add(k.d)) }

// KeepAlive 服务端本地慢工作期间调用（每块存储前）：续期自身 idle deadline
// 并发一帧空 MSG_DATA（4 字节头 00 00 00 07）——客户端收到即刷新它的
// last_io_in，不按自己的 --timeout 误判死连接。
func (k *idleKeeper) KeepAlive() {
	k.touch()
	_ = k.mw.WriteData(nil)
}

// applyIoTimeout 按协商结果配置连接超时，返回空闲管理器（nil = 未启用，
// 此时恢复连接级 24h 兜底上限以清除握手阶段的短 deadline）：
// 立即主动发 MSG_IO_TIMEOUT(N) 宣告帧，并挂载 mux 帧读写重置回调。
// 返回的 keeper 供会话在本地慢工作点调用 KeepAlive。
func applyIoTimeout(w io.Writer, mr *MuxReader, mw *MuxWriter, neg *Negotiation) *idleKeeper {
	conn, ok := w.(net.Conn)
	if !ok {
		return nil
	}
	if neg.IoTimeout <= 0 {
		_ = conn.SetDeadline(time.Now().Add(maxSessionDuration))
		return nil
	}
	d := time.Duration(neg.IoTimeout) * time.Second
	k := &idleKeeper{conn: conn, d: d, mw: mw}
	mr.OnFrame = k.touch
	mw.OnWrite = k.touch
	k.touch() // 初始 deadline：协商完成时刻起算
	// 宣告帧：payload = 4 字节 LE int32 秒数（io.c send_msg_int）
	var num [4]byte
	binary.LittleEndian.PutUint32(num[:], uint32(neg.IoTimeout))
	_ = mw.writeFrame(msgIoTimeout, num[:])
	return k
}

// wrapIoTimeout 在会话返回处包装空闲超时错误：识别 deadline 到点引发的
// i/o timeout，先给客户端补发 FERROR 文本（对齐 rsyncd rprintf(FERROR)，
// 客户端 stderr 可见明确原因）再断连，并转为中文错误上抛日志。
func wrapIoTimeout(err error, w io.Writer, mw *MuxWriter, neg *Negotiation) error {
	if err == nil || neg.IoTimeout <= 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		return err
	}
	if conn, ok := w.(net.Conn); ok {
		// deadline 已过，先放宽写超时保证错误文本送达再断连
		_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	}
	_ = mw.writeFrame(msgError, []byte(fmt.Sprintf("[server] io timeout after %d seconds -- exiting\n", neg.IoTimeout)))
	return fmt.Errorf("io 空闲超时（--timeout=%d 秒内无数据流动）: %w", neg.IoTimeout, err)
}
