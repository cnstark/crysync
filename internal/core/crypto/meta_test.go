package crypto

import (
	"bytes"
	"testing"
)

func TestMetaEncryptionRoundTripAndAuthentication(t *testing.T) {
	key, _ := GenerateKey()
	plain := bytes.Repeat([]byte("sqlite-page-data"), 100000)
	repoID := "00112233445566778899aabbccddeeff"
	var encrypted bytes.Buffer
	if err := key.EncryptMeta(&encrypted, bytes.NewReader(plain), "backup.cmeta", repoID); err != nil {
		t.Fatal(err)
	}
	var restored bytes.Buffer
	gotID, err := key.DecryptMeta(&restored, bytes.NewReader(encrypted.Bytes()), "backup.cmeta")
	if err != nil || gotID != repoID || !bytes.Equal(restored.Bytes(), plain) {
		t.Fatalf("Meta roundtrip 失败: id=%s err=%v", gotID, err)
	}
	bad := append([]byte(nil), encrypted.Bytes()...)
	bad[len(bad)/2] ^= 1
	if _, err := key.DecryptMeta(&bytes.Buffer{}, bytes.NewReader(bad), "backup.cmeta"); err == nil {
		t.Fatal("篡改的 Meta 备份应认证失败")
	}
	truncated := encrypted.Bytes()[:len(encrypted.Bytes())-32]
	if _, err := key.DecryptMeta(&bytes.Buffer{}, bytes.NewReader(truncated), "backup.cmeta"); err == nil {
		t.Fatal("截断的 Meta 备份应失败")
	}
	if _, err := key.DecryptMeta(&bytes.Buffer{}, bytes.NewReader(encrypted.Bytes()), "renamed.cmeta"); err == nil {
		t.Fatal("更改备份名应导致 AAD 认证失败")
	}
}
