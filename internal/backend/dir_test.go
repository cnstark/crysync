// internal/backend/dir_test.go
package backend

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestDirBackendRoundtrip(t *testing.T) {
	b, err := NewDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x42}, 4096)
	if err := b.Put("abc", data); err != nil {
		t.Fatal(err)
	}
	got, err := b.Get("abc")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("数据不一致")
	}
	list, err := b.List()
	if err != nil || len(list) != 1 || list[0] != "abc" {
		t.Fatalf("List: %v %v", list, err)
	}
	if err := b.Delete("abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get("abc"); err == nil {
		t.Fatal("删除后应无法读取")
	}
}

func TestDirBackendGetMissing(t *testing.T) {
	b, _ := NewDir(t.TempDir())
	if _, err := b.Get("nope"); err == nil {
		t.Fatal("读取缺失 blob 应报错")
	}
}

func TestDirBackendAtomicPut(t *testing.T) {
	b, _ := NewDir(t.TempDir())
	if err := b.Put("x", []byte("content")); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(b.Path(), "*"))
	if len(files) != 1 || filepath.Base(files[0]) != "x" {
		t.Fatalf("不应残留临时文件: %v", files)
	}
}
