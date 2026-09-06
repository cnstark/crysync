package backend

import (
	"reflect"
	"testing"
)

func TestBucketPartsVectors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		depth int
		want  []string
	}{
		{"abc", 1, []string{"ba"}},
		{"abc", 2, []string{"ba", "78"}},
		{"abc", 4, []string{"ba", "78", "16", "bf"}},
		{"abc", 0, []string{"ba", "78"}},
	} {
		got, err := bucketParts(tc.name, tc.depth)
		if err != nil {
			t.Fatalf("bucketParts(%q, %d): %v", tc.name, tc.depth, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("bucketParts(%q, %d) = %v, want %v", tc.name, tc.depth, got, tc.want)
		}
	}
}

func TestNormalizeBucketDepth(t *testing.T) {
	for _, tc := range []struct {
		input int
		want  int
		ok    bool
	}{
		{0, 2, true}, {1, 1, true}, {2, 2, true}, {4, 4, true}, {-1, 0, false}, {5, 0, false},
	} {
		got, err := normalizeBucketDepth(tc.input)
		if (err == nil) != tc.ok || tc.ok && got != tc.want {
			t.Errorf("normalizeBucketDepth(%d) = %d, %v; want %d, ok=%v", tc.input, got, err, tc.want, tc.ok)
		}
	}
}

func TestValidateBlobName(t *testing.T) {
	for _, name := range []string{"", ".", "..", "/tmp/blob", `a/b`, `a\\b`} {
		if err := validateBlobName(name); err == nil {
			t.Errorf("validateBlobName(%q) 应失败", name)
		}
	}
	for _, name := range []string{"abc", "blob name", "a%2Fb"} {
		if err := validateBlobName(name); err != nil {
			t.Errorf("validateBlobName(%q) 不应失败: %v", name, err)
		}
	}
}

func TestBlobBelongsToBucket(t *testing.T) {
	parts, err := bucketParts("abc", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !blobBelongsToBucket("abc", parts, 2) {
		t.Fatal("正确分桶未被识别")
	}
	wrong := append([]string(nil), parts...)
	if wrong[0] == "00" {
		wrong[0] = "01"
	} else {
		wrong[0] = "00"
	}
	if blobBelongsToBucket("abc", wrong, 2) {
		t.Fatal("错误分桶不应被识别")
	}
}
