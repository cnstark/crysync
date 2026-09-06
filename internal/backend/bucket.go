package backend

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
)

const DefaultBucketDepth = 2

// NormalizeBucketDepth 返回统一的分桶深度。0 表示使用默认的两级分桶。
func NormalizeBucketDepth(depth int) (int, error) {
	if depth == 0 {
		return DefaultBucketDepth, nil
	}
	if depth < 1 || depth > 4 {
		return 0, fmt.Errorf("backend.bucket_depth 必须为 0 或 1～4，得到 %d", depth)
	}
	return depth, nil
}

func normalizeBucketDepth(depth int) (int, error) { return NormalizeBucketDepth(depth) }

// validateBlobName 校验逻辑 blob 名，避免名称逃逸后端根目录。
func validateBlobName(name string) error {
	if name == "" {
		return fmt.Errorf("blob 名不能为空")
	}
	if name == "." || name == ".." || path.IsAbs(name) || strings.ContainsAny(name, `/\\`) {
		return fmt.Errorf("非法 blob 名 %q", name)
	}
	return nil
}

// bucketParts 根据逻辑 blob 名生成物理分桶目录片段。
func bucketParts(name string, depth int) ([]string, error) {
	if err := validateBlobName(name); err != nil {
		return nil, err
	}
	depth, err := normalizeBucketDepth(depth)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(name))
	hexDigest := hex.EncodeToString(digest[:])
	parts := make([]string, depth)
	for i := range parts {
		parts[i] = hexDigest[i*2 : i*2+2]
	}
	return parts, nil
}

func isBucketPart(s string) bool {
	if len(s) != 2 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func isTemporaryBlob(name string) bool { return strings.HasPrefix(name, ".tmp-") }

// blobBelongsToBucket 校验叶子资源是否确实位于其逻辑名对应的桶中。
// List 必须做这一步，避免把手工放错位置或损坏布局中的对象交给 GC。
func blobBelongsToBucket(name string, parts []string, depth int) bool {
	want, err := bucketParts(name, depth)
	if err != nil || len(parts) != len(want) {
		return false
	}
	for i := range want {
		if parts[i] != want[i] {
			return false
		}
	}
	return true
}
