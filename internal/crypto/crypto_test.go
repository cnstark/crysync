// internal/crypto/crypto_test.go
package crypto

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEncryptDecryptRoundtrip(t *testing.T) {
	k, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte{0xAB}, 100_000)
	blob, err := k.Encrypt(plain, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(blob, plain) {
		t.Fatal("密文不应与明文相同")
	}
	got, err := k.Decrypt(blob, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatal("解密结果与明文不一致")
	}
}

func TestDecryptFailsOnAnyCorruption(t *testing.T) {
	k, _ := GenerateKey()
	name := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	blob, _ := k.Encrypt([]byte("hello world"), name)
	for _, i := range []int{4, 20, len(blob) / 2, len(blob) - 1} { // nonce/密文/tag 各位置
		corrupted := append([]byte(nil), blob...)
		corrupted[i] ^= 0x01
		if _, err := k.Decrypt(corrupted, name); err == nil {
			t.Fatalf("篡改字节 %d 应解密失败", i)
		}
	}
	// AAD 不匹配（换 blob 名）
	if _, err := k.Decrypt(blob, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"); err == nil {
		t.Fatal("blob 名（AAD）不匹配应失败")
	}
	// magic 错误
	if _, err := k.Decrypt([]byte("XXXX"), name); err == nil {
		t.Fatal("magic 错误应失败")
	}
}

func TestKeyFileRoundtrip(t *testing.T) {
	k, _ := GenerateKey()
	p := filepath.Join(t.TempDir(), "test.key")
	if err := SaveKeyFile(p, k); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("密钥文件权限应为 0600，实际 %o", fi.Mode().Perm())
	}
	k2, err := LoadKeyFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if k2.data != k.data {
		t.Fatal("密钥不一致")
	}
	if _, err := LoadKeyFile(filepath.Join(t.TempDir(), "nope.key")); err == nil {
		t.Fatal("缺失密钥文件应报错")
	}
}

func TestRandomBlobName(t *testing.T) {
	a, _ := RandomBlobName()
	b, _ := RandomBlobName()
	if len(a) != 64 || a == b {
		t.Fatalf("blob 名应为 64 hex 且互不相同: %s %s", a, b)
	}
}
