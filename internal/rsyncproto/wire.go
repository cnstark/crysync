// internal/rsyncproto/wire.go
package rsyncproto

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

// 字节序：rsync 协议为 native endian；本项目目标平台 amd64/arm64 均为小端。

func ReadInt32(r io.Reader) (int32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b[:])), nil
}

func WriteInt32(w io.Writer, v int32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(v))
	_, err := w.Write(b[:])
	return err
}

func ReadShortint(r io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b[:]), nil
}

func WriteShortint(w io.Writer, v uint16) error {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	_, err := w.Write(b[:])
	return err
}

// ReadVarlong30 读取协议≥30 的 varlong（min_bytes=3）。
// （io.h read_varlong30：protocol<30 用 read_longint，≥30 用 read_varlong(f,3)；本项目固定 p31。）
func ReadVarlong30(r io.Reader) (int32, error) {
	v, err := ReadVarlong(r, 3)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func WriteVarlong30(w io.Writer, v int32) error {
	return WriteVarlong(w, int64(v), 3)
}

// --- varint / varlong（io.c write_varint / write_varlong）---
//
// 首字节高位 1 的个数 = 后续数据字节数；值按小端布局从后续字节 + 首字节低位恢复。
// 首字节：若总长 > 1，首字节 = b[cnt] | ~(bit*2-1)（即高位前缀全 1、低位为最高数据字节的低位部分）。

// readVarintLen 读取首字节并返回（后续字节数, 首字节值, 已消费的首字节）。
func readVarintLen(r io.Reader) (int, byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, 0, err
	}
	extra := 0
	for extra < 4 && b[0]&(0x80>>extra) != 0 {
		extra++
	}
	return extra, b[0], nil
}

func ReadVarint(r io.Reader) (int32, error) {
	extra, first, err := readVarintLen(r)
	if err != nil {
		return 0, err
	}
	// varint 等价 min=1：总字节 = extra+1；首字节低位（8-extra 位）在值的位偏移 8*extra
	var val uint32
	val |= uint32(first&(0xFF>>uint(extra))) << uint(8*extra)
	if extra > 0 {
		rest := make([]byte, extra)
		if _, err := io.ReadFull(r, rest); err != nil {
			return 0, err
		}
		for i := 0; i < extra; i++ {
			val |= uint32(rest[i]) << uint(8*i)
		}
	}
	return int32(val), nil
}

func WriteVarint(w io.Writer, v int32) error {
	x := uint32(v)
	var b [5]byte
	binary.LittleEndian.PutUint32(b[1:], x)
	cnt := 4
	for cnt > 1 && b[cnt] == 0 {
		cnt--
	}
	bit := byte(1 << (7 - cnt + 1))
	if b[cnt] >= bit {
		cnt++
		b[0] = ^(bit - 1)
	} else if cnt > 1 {
		b[0] = b[cnt] | ^(bit*2 - 1)
	} else {
		b[0] = b[1]
	}
	_, err := w.Write(b[:cnt])
	return err
}

// ReadVarlong 读取 varlong（前缀位编码，min_bytes..8 字节）。
func ReadVarlong(r io.Reader, minBytes int) (int64, error) {
	extra, first, err := readVarintLen(r)
	if err != nil {
		return 0, err
	}
	// 总字节 = extra+minBytes；首字节低位（8-extra 位）在值的位偏移 8*(extra+minBytes-1)
	dataBytes := extra + minBytes - 1
	var val uint64
	val |= uint64(first&(0xFF>>uint(extra))) << uint(8*dataBytes)
	if dataBytes > 0 {
		rest := make([]byte, dataBytes)
		if _, err := io.ReadFull(r, rest); err != nil {
			return 0, err
		}
		for i := 0; i < dataBytes; i++ {
			val |= uint64(rest[i]) << uint(8*i)
		}
	}
	return int64(val), nil
}

func WriteVarlong(w io.Writer, v int64, minBytes int) error {
	x := uint64(v)
	var b [9]byte
	binary.LittleEndian.PutUint64(b[1:], x)
	cnt := 8
	for cnt > minBytes && b[cnt] == 0 {
		cnt--
	}
	bit := byte(1 << (7 - cnt + minBytes))
	if b[cnt] >= bit {
		cnt++
		b[0] = ^(bit - 1)
	} else if cnt > minBytes {
		b[0] = b[cnt] | ^(bit*2 - 1)
	} else {
		b[0] = b[cnt]
	}
	_, err := w.Write(b[:cnt])
	return err
}

// --- mux 帧（io.c mplex_write / mplex_read）---
//
// 帧头 int32 小端：((MPLEX_BASE+code)<<24) | len
// 即字节序 [len_lo][len_mid][len_hi][tag]；len 为载荷字节数（不含帧头）；无流结束标记帧。

const mplexBase = 7

const (
	muxTypeData byte = 0 // MSG_DATA（code 0）
	// muxTypeMsg 语义修正：非 0 code 均为消息帧（MSG_INFO/MSG_ERROR/MSG_NOOP 等）
	muxTypeMsg byte = 1 // 任意非 0 code 即消息帧（用于命名对齐）
)

// msgNoSend = MSG_NO_SEND（rsync.h:308，code 102）：客户端 sender open 失败
// （权限/ vanished 等）时发此消息帧跳过该文件（sender.c:722-724 send_msg_int
// (MSG_NO_SEND, ndx) 后 continue），载荷为 4 字节 LE int32 ndx。
const msgNoSend byte = 102

// noSendError 表示客户端以 MSG_NO_SEND 跳过了 ndx 指示的文件（不发回显与
// token 流）。上层据此放弃该文件并继续下一 ndx——协议随之重新同步。
type noSendError struct{ ndx int32 }

func (e *noSendError) Error() string {
	return fmt.Sprintf("客户端跳过文件 (MSG_NO_SEND, ndx=%d)", e.ndx)
}

type MuxReader struct {
	r io.Reader
	// OnFrame 每次成功读出一帧后调用（io_timeout 空闲计时重置点；nil 无操作）。
	OnFrame func()
}

func NewMuxReader(r io.Reader) (*MuxReader, error) { return &MuxReader{r: r}, nil }

// Next 读取一帧；返回（载荷, tag, 错误）；EOF 返回 io.EOF。tag 为协议消息码
// （0 = 数据帧 MSG_DATA，非 0 为消息帧，如 MSG_INFO=2/MSG_NO_SEND=102）。
func (m *MuxReader) Next() ([]byte, byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(m.r, hdr[:]); err != nil {
		return nil, 0, err
	}
	raw := binary.LittleEndian.Uint32(hdr[:])
	length := raw & 0xFFFFFF
	tag := byte(int32(raw>>24) - mplexBase)
	if length > 64<<20 {
		return nil, 0, fmt.Errorf("非法帧长度: %d", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(m.r, payload); err != nil {
		return nil, 0, err
	}
	if m.OnFrame != nil {
		m.OnFrame()
	}
	return payload, tag, nil
}

type MuxWriter struct {
	w io.Writer
	// mu 保护写互斥：keepalive 心跳 goroutine 会与会话主流程并发写帧，
	// 无锁交错会损坏帧边界
	mu sync.Mutex
	// OnWrite 每次成功写出一帧后调用（io_timeout 空闲计时重置点；nil 无操作）。
	OnWrite func()
}

func NewMuxWriter(w io.Writer) *MuxWriter { return &MuxWriter{w: w} }

func (m *MuxWriter) writeFrame(typ byte, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(data) > 0xFFFFFF {
		return fmt.Errorf("帧载荷过大: %d", len(data))
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(mplexBase+typ)<<24|uint32(len(data)))
	if _, err := m.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := m.w.Write(data); err != nil {
		return err
	}
	if m.OnWrite != nil {
		m.OnWrite()
	}
	return nil
}

func (m *MuxWriter) WriteData(data []byte) error { return m.writeFrame(muxTypeData, data) }
func (m *MuxWriter) WriteMsg(msg string) error   { return m.writeFrame(muxTypeMsg, []byte(msg)) }

// WriteInfoMsg 发送 MSG_INFO（code 2=FINFO，rsync.h:295）提示消息：客户端打印但不
// 计入 io_error（MSG_ERROR_XFER=1 会使客户端 rc=23）。跳过设备/特殊文件等非错误
// 场景的客户端可见提示用此通道。
func (m *MuxWriter) WriteInfoMsg(msg string) error { return m.writeFrame(2, []byte(msg)) }

// MuxStream 将 mux 帧按序拼接为连续字节流（真实 rsync 的帧可合并/拆分逻辑记录）。
type MuxStream struct {
	mr     *MuxReader
	remain []byte
	eof    bool
	// Logger 非 nil 时，跳过的 mux 消息帧记 debug 日志（code+摘要，P2#11：
	// MSG_INFO/MSG_ERROR 等静默丢弃排障困难）；nil 无日志。
	Logger *slog.Logger
}

func NewMuxStream(mr *MuxReader) *MuxStream { return &MuxStream{mr: mr} }

func (s *MuxStream) Read(p []byte) (int, error) {
	for len(s.remain) == 0 && !s.eof {
		data, tag, err := s.mr.Next()
		if err == io.EOF {
			s.eof = true
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		if tag != muxTypeData {
			// MSG_NO_SEND（sender open 失败跳过文件）：向上传播打破对回显/token
			// 流的阻塞——客户端此后不再为该 ndx 发任何数据帧，静默跳过会死锁
			if tag == msgNoSend && len(data) == 4 {
				return 0, &noSendError{ndx: int32(binary.LittleEndian.Uint32(data))}
			}
			// 其余消息帧（MSG_INFO/MSG_ERROR/MSG_NOOP 等）：跳过，不得拼入数据流
			if s.Logger != nil {
				s.Logger.Debug("mux_msg_frame",
					"code", tag, "payload", fmt.Sprintf("%.64q", data))
			}
			continue
		}
		s.remain = data
	}
	if len(s.remain) == 0 {
		return 0, io.EOF
	}
	n := copy(p, s.remain)
	s.remain = s.remain[n:]
	return n, nil
}

// --- ndx 差分编码（io.c write_ndx / read_ndx，protocol 30+）---

const ndxDone = int32(-1)

type ndxCodec struct {
	prevPositive int32
	prevNegative int32
}

func newNdxCodec() *ndxCodec {
	return &ndxCodec{prevPositive: -1, prevNegative: 1}
}

// Write 编码并写出 ndx（差分字节编码）。
func (c *ndxCodec) Write(w io.Writer, ndx int32) error {
	if ndx == ndxDone {
		_, err := w.Write([]byte{0})
		return err
	}
	var b [6]byte
	cnt := 0
	var diff, num int32
	if ndx >= 0 {
		diff = ndx - c.prevPositive
		c.prevPositive = ndx
		num = ndx
	} else {
		b[cnt] = 0xFF
		cnt++
		ndx = -ndx
		diff = ndx - c.prevNegative
		c.prevNegative = ndx
		num = ndx
	}
	if diff < 0xFE && diff > 0 {
		b[cnt] = byte(diff)
		cnt++
	} else if diff < 0 || diff > 0x7FFF {
		b[cnt] = 0xFE
		b[cnt+1] = byte((num >> 24) | 0x80)
		b[cnt+2] = byte(num)
		b[cnt+3] = byte(num >> 8)
		b[cnt+4] = byte(num >> 16)
		cnt += 5
	} else {
		b[cnt] = 0xFE
		b[cnt+1] = byte(diff >> 8)
		b[cnt+2] = byte(diff)
		cnt += 3
	}
	_, err := w.Write(b[:cnt])
	return err
}

// Read 读取并解码一个 ndx。
func (c *ndxCodec) Read(r io.Reader) (int32, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	neg := false
	prev := &c.prevPositive
	switch b[0] {
	case 0x00:
		return ndxDone, nil
	case 0xFF:
		neg = true
		prev = &c.prevNegative
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
	}
	var num int32
	if b[0] == 0xFE {
		var bb [4]byte
		if _, err := io.ReadFull(r, bb[:2]); err != nil {
			return 0, err
		}
		if bb[0]&0x80 != 0 {
			bb[3] = bb[0] &^ 0x80
			bb[0] = bb[1]
			if _, err := io.ReadFull(r, bb[1:3]); err != nil {
				return 0, err
			}
			num = int32(uint32(bb[3])<<24 | uint32(bb[2])<<16 | uint32(bb[1])<<8 | uint32(bb[0]))
		} else {
			num = int32(uint32(bb[0])<<8|uint32(bb[1])) + *prev
		}
	} else {
		num = int32(b[0]) + *prev
	}
	if neg {
		num = -num
	}
	*prev = num
	return num, nil
}

// 随机字节（握手 seed 用）
var randRead = func(b []byte) (int, error) { return rand.Read(b) }
