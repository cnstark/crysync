// internal/core/repo/chunker_test.go
package repo

import (
	"bytes"
	"errors"
	"testing"
)

func collect(t *testing.T, data []byte, chunkSize int) [][]byte {
	t.Helper()
	var out [][]byte
	err := Split(bytes.NewReader(data), chunkSize, func(c []byte) error {
		out = append(out, append([]byte(nil), c...))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSplitExact(t *testing.T) {
	data := bytes.Repeat([]byte{0x01}, 8)
	chunks := collect(t, data, 4)
	if len(chunks) != 2 || len(chunks[0]) != 4 || len(chunks[1]) != 4 {
		t.Fatalf("整分块错误: %v", chunks)
	}
}

func TestSplitWithTail(t *testing.T) {
	data := []byte("0123456789")
	chunks := collect(t, data, 4)
	if len(chunks) != 3 || string(chunks[2]) != "89" {
		t.Fatalf("末块不足错误: %v", chunks)
	}
}

func TestSplitEmpty(t *testing.T) {
	chunks := collect(t, nil, 4)
	if len(chunks) != 0 {
		t.Fatalf("空输入不应有块: %v", chunks)
	}
}

func TestSplitSmallerThanChunk(t *testing.T) {
	chunks := collect(t, []byte("abc"), 4)
	if len(chunks) != 1 || string(chunks[0]) != "abc" {
		t.Fatalf("小块错误: %v", chunks)
	}
}

func TestSplitCallbackErrorPropagates(t *testing.T) {
	want := errors.New("stop")
	err := Split(bytes.NewReader([]byte("12345678")), 4, func([]byte) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("回调错误未传播: %v", err)
	}
}
