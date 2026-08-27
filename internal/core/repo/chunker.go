// internal/core/repo/chunker.go
package repo

import "io"

// Split 流式读取并按 chunkSize 固定分块，逐块调用 fn。
// 空输入不调用 fn；末块允许小于 chunkSize。
func Split(r io.Reader, chunkSize int, fn func(chunk []byte) error) error {
	buf := make([]byte, chunkSize)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if err := fn(buf[:n]); err != nil {
				return err
			}
		}
		if err == io.ErrUnexpectedEOF {
			return nil
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
