package repo

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"crysync/internal/backend"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
)

var metaMaintenanceLocks sync.Map // Meta 路径或后端实例标识 -> *sync.Mutex

func maintenanceLock(key string) func() {
	v, _ := metaMaintenanceLocks.LoadOrStore(key, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func metaBackupName(now time.Time) (string, error) {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return now.UTC().Format("20060102T150405.000000000Z") + "-" + hex.EncodeToString(suffix[:]) + ".cmeta", nil
}

// CreateMetaBackup 创建、加密、上传并回读验证一个不可变 Meta 快照。
func CreateMetaBackup(ctx context.Context, db *meta.DB, be backend.Backend, key *crypto.Key, retain int) (string, error) {
	unlock := maintenanceLock(db.DBPath())
	defer unlock()
	if retain < 1 {
		return "", fmt.Errorf("Meta 备份保留数必须为正数")
	}
	repositoryID, err := db.EnsureRepositoryID()
	if err != nil {
		return "", err
	}
	name, err := metaBackupName(time.Now())
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(db.DBPath())
	snapshot, err := tempPath(dir, ".meta-snapshot-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(snapshot)
	if err := db.BackupTo(ctx, snapshot); err != nil {
		return "", fmt.Errorf("创建 SQLite 快照: %w", err)
	}
	if _, err := meta.ValidateSnapshot(snapshot, repositoryID); err != nil {
		return "", fmt.Errorf("校验 SQLite 快照: %w", err)
	}

	encrypted, err := os.CreateTemp(dir, ".meta-encrypted-*")
	if err != nil {
		return "", err
	}
	encryptedPath := encrypted.Name()
	defer os.Remove(encryptedPath)
	source, err := os.Open(snapshot)
	if err != nil {
		encrypted.Close()
		return "", err
	}
	err = key.EncryptMeta(encrypted, source, name, repositoryID)
	source.Close()
	if err == nil {
		err = encrypted.Sync()
	}
	if closeErr := encrypted.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	if err := uploadAndVerifyMeta(ctx, be, name, encryptedPath); err != nil {
		_ = be.DeleteMetaContext(context.Background(), name)
		return "", err
	}
	if err := pruneMetaBackups(ctx, be, retain); err != nil {
		return name, err
	}
	return name, nil
}

func tempPath(dir, pattern string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	if err := os.Remove(name); err != nil {
		return "", err
	}
	return name, nil
}

func uploadAndVerifyMeta(ctx context.Context, be backend.Backend, name, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	localHash := sha256.New()
	if _, err := io.Copy(localHash, f); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return err
	}
	if err := be.PutMetaContext(ctx, name, f, stat.Size()); err != nil {
		f.Close()
		return err
	}
	f.Close()
	remote, err := be.GetMetaContext(ctx, name)
	if err != nil {
		return err
	}
	remoteHash := sha256.New()
	_, copyErr := io.Copy(remoteHash, remote)
	closeErr := remote.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if !equalBytes(localHash.Sum(nil), remoteHash.Sum(nil)) {
		return fmt.Errorf("远端 Meta 回读摘要不一致")
	}
	return nil
}

func pruneMetaBackups(ctx context.Context, be backend.Backend, retain int) error {
	names, err := be.ListMetaContext(ctx)
	if err != nil {
		return err
	}
	names = validMetaNames(names)
	if len(names) <= retain {
		return nil
	}
	for _, name := range names[:len(names)-retain] {
		if err := be.DeleteMetaContext(ctx, name); err != nil {
			return fmt.Errorf("删除旧 Meta 备份 %s: %w", name, err)
		}
	}
	return nil
}

func validMetaNames(names []string) []string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		if strings.HasSuffix(name, ".cmeta") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// RestoreMeta 恢复最新可解密、数据库有效且全部引用 blob 完整的备份。
func RestoreMeta(ctx context.Context, target string, be backend.Backend, key *crypto.Key) (string, error) {
	unlock := maintenanceLock(target)
	defer unlock()
	names, err := be.ListMetaContext(ctx)
	if err != nil {
		return "", err
	}
	names = validMetaNames(names)
	if len(names) == 0 {
		return "", fmt.Errorf("远端没有 Meta 备份")
	}
	var failures []string
	for i := len(names) - 1; i >= 0; i-- {
		if err := restoreCandidate(ctx, target, be, key, names[i], true); err == nil {
			return names[i], nil
		} else {
			failures = append(failures, fmt.Sprintf("%s: %v", names[i], err))
		}
	}
	return "", fmt.Errorf("没有可恢复的 Meta 备份: %s", strings.Join(failures, "; "))
}

func restoreCandidate(ctx context.Context, target string, be backend.Backend, key *crypto.Key, name string, verifyData bool) error {
	dir := filepath.Dir(target)
	encryptedPath, err := tempPath(dir, ".meta-download-*")
	if err != nil {
		return err
	}
	defer os.Remove(encryptedPath)
	remote, err := be.GetMetaContext(ctx, name)
	if err != nil {
		return err
	}
	encrypted, err := os.OpenFile(encryptedPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		remote.Close()
		return err
	}
	_, copyErr := io.Copy(encrypted, &contextReader{ctx: ctx, r: remote})
	remote.Close()
	closeErr := encrypted.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}

	restoredPath, err := tempPath(dir, ".meta-restore-*")
	if err != nil {
		return err
	}
	defer os.Remove(restoredPath)
	src, err := os.Open(encryptedPath)
	if err != nil {
		return err
	}
	dst, err := os.OpenFile(restoredPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		src.Close()
		return err
	}
	repositoryID, decryptErr := key.DecryptMeta(dst, src, name)
	src.Close()
	if decryptErr == nil {
		decryptErr = dst.Sync()
	}
	if closeErr := dst.Close(); decryptErr == nil {
		decryptErr = closeErr
	}
	if decryptErr != nil {
		return decryptErr
	}
	chunks, err := meta.ValidateSnapshot(restoredPath, repositoryID)
	if err != nil {
		return err
	}
	if verifyData {
		if err := verifyBackupChunks(ctx, be, key, chunks); err != nil {
			return err
		}
	}
	if err := os.Chmod(restoredPath, 0o600); err != nil {
		return err
	}
	_ = os.Remove(target + "-wal")
	_ = os.Remove(target + "-shm")
	if err := os.Rename(restoredPath, target); err != nil {
		return err
	}
	return syncDir(dir)
}

func verifyBackupChunks(ctx context.Context, be backend.Backend, key *crypto.Key, chunks []meta.BackupChunk) error {
	for _, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		ciphertext, err := be.Get(chunk.BlobName)
		if err != nil {
			return fmt.Errorf("读取引用 blob %s: %w", chunk.BlobName, err)
		}
		plaintext, err := key.Decrypt(ciphertext, chunk.BlobName)
		if err != nil {
			return fmt.Errorf("解密引用 blob %s: %w", chunk.BlobName, err)
		}
		hash := sha256.Sum256(plaintext)
		if hash != chunk.Hash || int64(len(plaintext)) != chunk.Size {
			return fmt.Errorf("引用 blob %s 的 hash 或大小不匹配", chunk.BlobName)
		}
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// ProtectedMetaBlobs 返回全部可保留恢复点引用的 blob。任一备份失败时不返回部分集合。
func ProtectedMetaBlobs(ctx context.Context, lockKey string, be backend.Backend, key *crypto.Key) (map[string]bool, error) {
	unlock := maintenanceLock(lockKey)
	defer unlock()
	names, err := be.ListMetaContext(ctx)
	if err != nil {
		return nil, err
	}
	protected := map[string]bool{}
	for _, name := range validMetaNames(names) {
		tmpDir, err := os.MkdirTemp("", "crysync-meta-gc-*")
		if err != nil {
			return nil, err
		}
		target := filepath.Join(tmpDir, "snapshot.db")
		err = restoreCandidate(ctx, target, be, key, name, false)
		if err == nil {
			var chunks []meta.BackupChunk
			chunks, err = meta.ValidateSnapshot(target, "")
			for _, chunk := range chunks {
				protected[chunk.BlobName] = true
			}
		}
		os.RemoveAll(tmpDir)
		if err != nil {
			return nil, fmt.Errorf("读取 GC 保护快照 %s: %w", name, err)
		}
	}
	return protected, nil
}
