package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

const (
	metaMagic     = "CRYM"
	metaVersion   = byte(1)
	metaFrameSize = 1024 * 1024
	metaHeaderLen = 4 + 1 + 16 + 4
)

func (k *Key) metaAEAD() (cipher.AEAD, error) {
	mac := hmac.New(sha256.New, k.data[:])
	_, _ = mac.Write([]byte("crysync/meta-backup/v1"))
	block, err := aes.NewCipher(mac.Sum(nil))
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func metaAAD(repoID []byte, name string, index uint64, final bool) []byte {
	aad := make([]byte, 0, 32+len(name))
	aad = append(aad, []byte("crysync/meta/v1\x00")...)
	aad = append(aad, repoID...)
	aad = append(aad, 0)
	aad = append(aad, name...)
	var seq [9]byte
	binary.BigEndian.PutUint64(seq[:8], index)
	if final {
		seq[8] = 1
	}
	return append(aad, seq[:]...)
}

// EncryptMeta 把 SQLite 快照流式编码为带认证终止帧的 CRYM v1 容器。
func (k *Key) EncryptMeta(dst io.Writer, src io.Reader, backupName, repoIDHex string) error {
	repoID, err := hex.DecodeString(repoIDHex)
	if err != nil || len(repoID) != 16 {
		return fmt.Errorf("repository ID 非法")
	}
	aead, err := k.metaAEAD()
	if err != nil {
		return err
	}
	header := make([]byte, metaHeaderLen)
	copy(header[:4], metaMagic)
	header[4] = metaVersion
	copy(header[5:21], repoID)
	binary.BigEndian.PutUint32(header[21:25], metaFrameSize)
	if _, err := dst.Write(header); err != nil {
		return err
	}
	buf := make([]byte, metaFrameSize)
	var index uint64
	for {
		n, readErr := io.ReadFull(src, buf)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return readErr
		}
		if n > 0 {
			if err := writeMetaFrame(dst, aead, repoID, backupName, index, false, buf[:n]); err != nil {
				return err
			}
			index++
		}
		if readErr != nil {
			break
		}
	}
	return writeMetaFrame(dst, aead, repoID, backupName, index, true, nil)
}

func writeMetaFrame(dst io.Writer, aead cipher.AEAD, repoID []byte, name string, index uint64, final bool, plaintext []byte) error {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(plaintext)))
	if _, err := dst.Write(length[:]); err != nil {
		return err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	if _, err := dst.Write(nonce); err != nil {
		return err
	}
	sealed := aead.Seal(nil, nonce, plaintext, metaAAD(repoID, name, index, final))
	_, err := dst.Write(sealed)
	return err
}

// DecryptMeta 验证并解密 CRYM v1，返回头部中经过认证的 repository ID。
func (k *Key) DecryptMeta(dst io.Writer, src io.Reader, backupName string) (string, error) {
	header := make([]byte, metaHeaderLen)
	if _, err := io.ReadFull(src, header); err != nil {
		return "", fmt.Errorf("读取 Meta 头: %w", err)
	}
	if string(header[:4]) != metaMagic || header[4] != metaVersion {
		return "", errors.New("Meta 备份格式错误")
	}
	repoID := append([]byte(nil), header[5:21]...)
	frameSize := binary.BigEndian.Uint32(header[21:25])
	if frameSize != metaFrameSize {
		return "", errors.New("Meta 备份帧大小非法")
	}
	aead, err := k.metaAEAD()
	if err != nil {
		return "", err
	}
	var index uint64
	for {
		var length [4]byte
		if _, err := io.ReadFull(src, length[:]); err != nil {
			return "", fmt.Errorf("Meta 备份缺少认证终止帧: %w", err)
		}
		n := binary.BigEndian.Uint32(length[:])
		if n > frameSize {
			return "", errors.New("Meta 备份数据帧过大")
		}
		nonce := make([]byte, aead.NonceSize())
		if _, err := io.ReadFull(src, nonce); err != nil {
			return "", err
		}
		sealed := make([]byte, int(n)+aead.Overhead())
		if _, err := io.ReadFull(src, sealed); err != nil {
			return "", err
		}
		final := n == 0
		plaintext, err := aead.Open(nil, nonce, sealed, metaAAD(repoID, backupName, index, final))
		if err != nil {
			return "", fmt.Errorf("Meta 备份认证失败: %w", err)
		}
		if final {
			var extra [1]byte
			if n, err := io.ReadFull(src, extra[:]); err != io.EOF || n != 0 {
				return "", errors.New("Meta 备份含尾随数据")
			}
			return hex.EncodeToString(repoID), nil
		}
		if _, err := dst.Write(plaintext); err != nil {
			return "", err
		}
		index++
	}
}
