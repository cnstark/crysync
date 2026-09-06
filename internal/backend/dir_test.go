// internal/backend/dir_test.go
package backend

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestDirBackendRoundtrip(t *testing.T) {
	b, err := NewDir(t.TempDir(), 2)
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
	b, _ := NewDir(t.TempDir(), 2)
	if _, err := b.Get("nope"); err == nil {
		t.Fatal("读取缺失 blob 应报错")
	}
}

func TestDirBackendAtomicPut(t *testing.T) {
	b, _ := NewDir(t.TempDir(), 2)
	if err := b.Put("x", []byte("content")); err != nil {
		t.Fatal(err)
	}
	parts, _ := bucketParts("x", 2)
	files, _ := filepath.Glob(filepath.Join(append([]string{b.Path()}, append(parts, "*")...)...))
	if len(files) != 1 || filepath.Base(files[0]) != "x" {
		t.Fatalf("不应残留临时文件: %v", files)
	}
}

func TestDirBackendBucketLayout(t *testing.T) {
	root := t.TempDir()
	b, err := NewDir(root, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("abc", []byte("content")); err != nil {
		t.Fatal(err)
	}
	parts, _ := bucketParts("abc", 2)
	physical := filepath.Join(append([]string{root}, append(parts, "abc")...)...)
	if _, err := os.Stat(physical); err != nil {
		t.Fatalf("物理路径错误 %s: %v", physical, err)
	}
	if _, err := os.Stat(filepath.Join(root, "abc")); !os.IsNotExist(err) {
		t.Fatalf("根目录不应存在平铺 blob")
	}
}

func TestDirBackendBucketDepths(t *testing.T) {
	for _, depth := range []int{1, 2, 4} {
		root := t.TempDir()
		b, err := NewDir(root, depth)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put("abc", []byte("content")); err != nil {
			t.Fatal(err)
		}
		parts, _ := bucketParts("abc", depth)
		physical := filepath.Join(append([]string{root}, append(parts, "abc")...)...)
		if _, err := os.Stat(physical); err != nil {
			t.Fatalf("深度 %d 物理路径错误: %v", depth, err)
		}
	}
}

func TestDirBackendConcurrentPut(t *testing.T) {
	b, err := NewDir(t.TempDir(), 2)
	if err != nil {
		t.Fatal(err)
	}
	const count = 32
	errCh := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("blob-%02d", i)
			if err := b.Put(name, []byte(name)); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	names, err := b.List()
	if err != nil || len(names) != count {
		t.Fatalf("并发 Put 后 List = %v，err=%v", names, err)
	}
}

func TestDirBackendListIgnoresObjectsOutsideStrictLayout(t *testing.T) {
	root := t.TempDir()
	b, err := NewDir(root, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("valid", []byte("valid")); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(".valid-hidden-name", []byte("hidden")); err != nil {
		t.Fatal(err)
	}

	validParts, _ := bucketParts("valid", 2)
	leafDir := filepath.Join(append([]string{root}, validParts...)...)
	if err := os.WriteFile(filepath.Join(root, "old-flat-blob"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(leafDir, ".tmp-interrupted"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	misplaced := "misplaced"
	for blobBelongsToBucket(misplaced, validParts, 2) {
		misplaced += "x"
	}
	if err := os.WriteFile(filepath.Join(leafDir, misplaced), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(leafDir, "extra"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(leafDir, "extra", "too-deep"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	names, err := b.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || !containsName(names, "valid") || !containsName(names, ".valid-hidden-name") {
		t.Fatalf("List 应只返回严格布局中的两个 blob，得到 %v", names)
	}
}

func containsName(names []string, want string) bool {
	for _, name := range names {
		if name == want {
			return true
		}
	}
	return false
}
