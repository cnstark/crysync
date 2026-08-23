// internal/crypto/crypto.go
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
)

const (
	blobMagic   = "CRYB"
	blobVersion = byte(1)
	keyMagic    = "CRYK"
	keyVersion  = byte(1)
	keyLen      = 32
	nonceLen    = 12
)

type Key struct{ data [32]byte }

func GenerateKey() (*Key, error) {
	var k Key
	if _, err := rand.Read(k.data[:]); err != nil {
		return nil, err
	}
	return &k, nil
}

func LoadKeyFile(path string) (*Key, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取密钥文件: %w", err)
	}
	if len(b) != 4+1+keyLen || string(b[:4]) != keyMagic || b[4] != keyVersion {
		return nil, errors.New("密钥文件格式错误")
	}
	var k Key
	copy(k.data[:], b[5:])
	return &k, nil
}

func SaveKeyFile(path string, k *Key) error {
	b := make([]byte, 0, 4+1+keyLen)
	b = append(b, keyMagic...)
	b = append(b, keyVersion)
	b = append(b, k.data[:]...)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return fmt.Errorf("写入密钥文件: %w", err)
	}
	return os.Chmod(path, 0o600)
}

func (k *Key) Encrypt(plaintext []byte, blobName string) ([]byte, error) {
	block, err := aes.NewCipher(k.data[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 4+1+nonceLen+len(plaintext)+gcm.Overhead())
	out = append(out, blobMagic...)
	out = append(out, blobVersion)
	out = append(out, nonce...)
	sealed := gcm.Seal(nil, nonce, plaintext, []byte(blobName))
	out = append(out, sealed...)
	return out, nil
}

func (k *Key) Decrypt(blob []byte, blobName string) ([]byte, error) {
	if len(blob) < 4+1+nonceLen+16 || string(blob[:4]) != blobMagic || blob[4] != blobVersion {
		return nil, errors.New("blob 格式错误")
	}
	block, err := aes.NewCipher(k.data[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := blob[5 : 5+nonceLen]
	sealed := blob[5+nonceLen:]
	pt, err := gcm.Open(nil, nonce, sealed, []byte(blobName))
	if err != nil {
		return nil, fmt.Errorf("blob 解密失败（可能被篡改或名称不匹配）: %w", err)
	}
	return pt, nil
}

func RandomBlobName() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

var _ = binary.LittleEndian // 预留：后续 blob 内嵌长度字段时使用
