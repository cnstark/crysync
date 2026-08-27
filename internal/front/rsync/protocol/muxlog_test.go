package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"strings"
	"testing"
)

// TestMuxStreamLogsMsgFrames（P2#11）：MuxStream 跳过的 mux 消息帧（MSG_INFO/
// MSG_ERROR 等非 0 code）应记 debug 日志（code + 内容摘要），不得拼入数据流。
func TestMuxStreamLogsMsgFrames(t *testing.T) {
	var input bytes.Buffer
	frame := func(code byte, payload []byte) {
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(mplexBase+code)<<24|uint32(len(payload)))
		input.Write(hdr[:])
		input.Write(payload)
	}
	frame(2, []byte("hello info"))   // MSG_INFO
	frame(0, []byte("real-data"))    // MSG_DATA（正常数据）
	frame(3, []byte("boom"))         // MSG_ERROR
	frame(0, []byte("more-data"))    // MSG_DATA

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	mr, _ := NewMuxReader(&input)
	s := NewMuxStream(mr)
	s.Logger = logger

	var got []byte
	buf := make([]byte, 64)
	for {
		n, err := s.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if string(got) != "real-datamore-data" {
		t.Fatalf("数据流不应混入消息帧负载: %q", got)
	}
	out := logs.String()
	if !strings.Contains(out, "mux_msg_frame") || !strings.Contains(out, "hello info") || !strings.Contains(out, "boom") {
		t.Fatalf("跳过的消息帧应记 debug 日志: %q", out)
	}
}
