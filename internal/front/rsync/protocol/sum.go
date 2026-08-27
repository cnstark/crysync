// internal/front/rsync/protocol/sum.go
package protocol

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
)

// 块校验和常量（rsync.h：BLOCK_SIZE=700、MAX_BLOCK_SIZE=1<<17 协议>=30 生效；
// xfer_sum_len 协商 md5 = 16）。
const (
	blockSize    = 700
	maxBlockSize = 1 << 17
	strongSumLen = 16
)

// BlockSum 单块校验和：sum1（rolling 4B）+ sum2（strong 16B）。
type BlockSum struct {
	Sum1 uint32
	Sum2 [16]byte
}

// SumTable 块校验和表（generator.c sum_sizes_sqroot 结果，描述 basis 文件块结构）。
type SumTable struct {
	Count     int32
	Blength   int32
	S2Length  int32
	Remainder int32
	Sums      []BlockSum
}

// CalcSizes 按文件长度计算块结构（sum_sizes_sqroot，generator.c:677-756）：
// len<=700² → blength=700；否则二进制平方根上取整到 8 的倍数（下限 700，
// 上限 MAX_BLOCK_SIZE=131072）；remainder=len%blength（可为 0，不做调整）；
// count=len/blength + (remainder!=0)。
// 注意：BLOCKSUM_BIAS 只参与 s2length 启发式，本项目 csum_length==SUM_LENGTH
// 恒走 s2length=16 分支，不涉及。
func CalcSizes(len int64) (count, blength, remainder int32, err error) {
	if len < 0 {
		return 0, 0, 0, fmt.Errorf("非法文件长度: %d", len)
	}
	b := int64(blockSize)
	if len > int64(blockSize)*blockSize {
		c := int64(1)
		// C 源码: for (c=1, l=len, cnt=0; l >>= 2; c <<= 1, cnt++) {}
		// C 的移位赋值可作条件（先 l>>=2 再判非零）；Go 语法中赋值不能作条件，
		// 故拆为 init 先移位 + 循环体判断，语义与 C 完全一致（先移后判）。
		for l := len; ; {
			l >>= 2
			if l == 0 {
				break
			}
			c <<= 1
		}
		if c >= maxBlockSize {
			b = maxBlockSize
		} else {
			b = 0
			for ; c >= 8; c >>= 1 {
				b |= c
				if len < b*b {
					b &^= c
				}
			}
			if b < blockSize {
				b = blockSize
			}
		}
	}
	rem := len % b
	cnt := len / b
	if rem != 0 {
		cnt++
	}
	if cnt > 1<<24 {
		return 0, 0, 0, fmt.Errorf("文件过大，块数超限: %d", cnt)
	}
	return int32(cnt), int32(b), int32(rem), nil
}

// RollingSum1 复刻 get_checksum1（checksum.c:75-91）：adler 变体，数据按
// signed char 处理（CHAR_OFFSET=0），uint32 自然回绕。返回值即 rsync 的
// (s1 & 0xffff) + (s2 << 16)，uint32 截断。
func RollingSum1(data []byte) uint32 {
	var s1, s2 uint32
	i := 0
	for ; i < len(data)-4; i += 4 {
		s2 += 4*(s1+uint32(int8(data[i]))) + 3*uint32(int8(data[i+1])) +
			2*uint32(int8(data[i+2])) + uint32(int8(data[i+3]))
		s1 += uint32(int8(data[i])) + uint32(int8(data[i+1])) +
			uint32(int8(data[i+2])) + uint32(int8(data[i+3]))
	}
	for ; i < len(data); i++ {
		s1 += uint32(int8(data[i]))
		s2 += s1
	}
	return (s1 & 0xffff) + (s2 << 16)
}

// StrongSum2 复刻 get_checksum2 的 CSUM_MD5 分支（checksum.c:338-358）。
// proper_seed_order = compat_flags & CF_CHKSUM_SEED_FIX：本项目 compat_flags=0
// → proper_seed_order=0 → seed 追加在数据之后：MD5(data || LE32(seed))，
// seed==0 时纯 MD5(data)。注意与整文件校验和的区别（sum_end 不混 seed）。
func StrongSum2(data []byte, seed int32) [16]byte {
	h := md5.New()
	h.Write(data)
	if seed != 0 {
		var sb [4]byte
		binary.LittleEndian.PutUint32(sb[:], uint32(seed))
		h.Write(sb[:])
	}
	var out [16]byte
	copy(out[:], h.Sum(nil))
	return out
}

// CalcBlockSums 流式读 r 的 len 字节计算整个块校验和表。r 必须恰好产出
// len 字节（多/少均报错）；块数须与 CalcSizes 一致。
func CalcBlockSums(len int64, r io.Reader, seed int32) (SumTable, error) {
	count, blength, remainder, err := CalcSizes(len)
	if err != nil {
		return SumTable{}, err
	}
	t := SumTable{Count: count, Blength: blength, S2Length: strongSumLen, Remainder: remainder}
	if len == 0 {
		return t, nil
	}
	buf := make([]byte, blength)
	var total, sums int64
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			t.Sums = append(t.Sums, BlockSum{RollingSum1(buf[:n]), StrongSum2(buf[:n], seed)})
			total += int64(n)
			sums++
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return SumTable{}, err
		}
	}
	if total != len {
		return SumTable{}, fmt.Errorf("校验和计算: 读 %d/%d 字节", total, len)
	}
	// 参数名 len 遮蔽内建 len 函数，用循环计数代替 len(t.Sums)。
	if sums != int64(count) {
		return SumTable{}, fmt.Errorf("校验和块数不符: %d/%d", sums, count)
	}
	return t, nil
}

// WriteSumTable 发送 sum_head + 逐块校验和（generate_and_send_sums，
// generator.c:763-815）：头 4×int32（count/blength/s2length/remainder），
// 正文逐块 write_int(sum1) + write_buf(sum2, s2length)。随 mux MSG_DATA 流送出，
// 非 mux 独立帧。
func WriteSumTable(out *MuxWriter, t *SumTable) error {
	var buf bytesBuffer
	for _, v := range [...]int32{t.Count, t.Blength, t.S2Length, t.Remainder} {
		if err := WriteInt32(&buf, v); err != nil {
			return err
		}
	}
	for _, s := range t.Sums {
		if err := WriteInt32(&buf, int32(s.Sum1)); err != nil {
			return err
		}
		buf.Write(s.Sum2[:])
	}
	return out.WriteData(buf.Bytes())
}
