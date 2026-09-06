package repo

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"crysync/internal/backend"
	"crysync/internal/core/crypto"
	"crysync/internal/core/meta"
)

func newMetaBackupRepo(t *testing.T) (*Repo, *meta.DB, *backend.Dir, *crypto.Key, string) {
	t.Helper()
	root := t.TempDir()
	metaPath := filepath.Join(root, "meta", "repo.db")
	db, err := meta.Open(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.EnsureRepositoryID(); err != nil {
		t.Fatal(err)
	}
	be, err := backend.NewDir(filepath.Join(root, "data"), 2)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := crypto.GenerateKey()
	return New(db, be, key, 64), db, be, key, metaPath
}

func attachTestFile(t *testing.T, r *Repo, path string, content []byte) {
	t.Helper()
	id, _, err := r.StoreChunk(content)
	if err != nil {
		t.Fatal(err)
	}
	unlock := r.WriteSessionLock()
	err = r.UpsertFile(meta.FileRow{Path: path, Mode: 0o644, Size: int64(len(content))}, []meta.ChunkRef{{ChunkID: id, IDX: 0}})
	unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMetaBackupRestoreAndVerifyData(t *testing.T) {
	r, db, be, key, metaPath := newMetaBackupRepo(t)
	content := []byte("recoverable content")
	attachTestFile(t, r, "file.txt", content)
	name, err := CreateMetaBackup(context.Background(), db, be, key, 24)
	if err != nil || name == "" {
		t.Fatalf("创建 Meta 备份失败: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(metaPath); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreMeta(context.Background(), metaPath, be, key); err != nil {
		t.Fatalf("恢复 Meta 失败: %v", err)
	}
	restoredDB, err := meta.OpenExisting(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer restoredDB.Close()
	restored := New(restoredDB, be, key, 64)
	var got bytes.Buffer
	if err := restored.ReadFile("file.txt", &got); err != nil || !bytes.Equal(got.Bytes(), content) {
		t.Fatalf("恢复后文件不完整: got=%q err=%v", got.Bytes(), err)
	}
}

func TestMetaRestoreFallsBackFromCorruptNewest(t *testing.T) {
	r, db, be, key, metaPath := newMetaBackupRepo(t)
	attachTestFile(t, r, "file.txt", []byte("valid"))
	if _, err := CreateMetaBackup(context.Background(), db, be, key, 24); err != nil {
		t.Fatal(err)
	}
	bad := bytes.NewReader([]byte("corrupt"))
	if err := be.PutMetaContext(context.Background(), "99991231T235959.000000000Z-deadbeef.cmeta", bad, int64(bad.Len())); err != nil {
		t.Fatal(err)
	}
	db.Close()
	os.Remove(metaPath)
	name, err := RestoreMeta(context.Background(), metaPath, be, key)
	if err != nil || name == "99991231T235959.000000000Z-deadbeef.cmeta" {
		t.Fatalf("应回退到较旧有效备份: name=%s err=%v", name, err)
	}
}

func TestMetaRestoreRejectsMissingReferencedBlob(t *testing.T) {
	r, db, be, key, metaPath := newMetaBackupRepo(t)
	attachTestFile(t, r, "file.txt", []byte("must exist"))
	if _, err := CreateMetaBackup(context.Background(), db, be, key, 24); err != nil {
		t.Fatal(err)
	}
	names, err := be.List()
	if err != nil || len(names) != 1 {
		t.Fatalf("数据 blob 异常: %v %v", names, err)
	}
	if err := be.Delete(names[0]); err != nil {
		t.Fatal(err)
	}
	db.Close()
	os.Remove(metaPath)
	if _, err := RestoreMeta(context.Background(), metaPath, be, key); err == nil {
		t.Fatal("引用 blob 缺失时恢复应失败")
	}
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Fatal("校验失败时不应发布本地 Meta")
	}
}

func TestMetaRestoreRejectsWrongKey(t *testing.T) {
	r, db, be, key, metaPath := newMetaBackupRepo(t)
	attachTestFile(t, r, "file.txt", []byte("encrypted metadata"))
	if _, err := CreateMetaBackup(context.Background(), db, be, key, 24); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(metaPath); err != nil {
		t.Fatal(err)
	}
	wrongKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreMeta(context.Background(), metaPath, be, wrongKey); err == nil {
		t.Fatal("错误 key 不应恢复 Meta")
	}
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Fatal("认证失败时不应发布本地 Meta")
	}
}

func TestGCProtectsBlobsReferencedByRetainedMeta(t *testing.T) {
	r, db, be, key, _ := newMetaBackupRepo(t)
	attachTestFile(t, r, "file.txt", []byte("old version"))
	if _, err := CreateMetaBackup(context.Background(), db, be, key, 24); err != nil {
		t.Fatal(err)
	}
	attachTestFile(t, r, "file.txt", []byte("new version"))
	deleted, err := r.GC()
	if err != nil || deleted != 0 {
		t.Fatalf("GC 不应删除恢复点引用的旧 blob: deleted=%d err=%v", deleted, err)
	}
	if names, err := be.List(); err != nil || len(names) != 2 {
		t.Fatalf("新旧 blob 都应保留: %v %v", names, err)
	}
	db.Close()
}

func TestGCFailClosedWhenRetainedMetaIsCorrupt(t *testing.T) {
	r, db, be, _, _ := newMetaBackupRepo(t)
	orphanID, _, err := r.StoreChunk([]byte("orphan that must survive failed GC"))
	if err != nil {
		t.Fatal(err)
	}
	var blobName string
	if err := db.QueryRow(`SELECT blob_name FROM chunks WHERE id = ?`, orphanID).Scan(&blobName); err != nil {
		t.Fatalf("读取孤儿 chunk: %v", err)
	}
	bad := bytes.NewReader([]byte("corrupt"))
	if err := be.PutMetaContext(context.Background(), "99991231T235959.000000000Z-deadbeef.cmeta", bad, int64(bad.Len())); err != nil {
		t.Fatal(err)
	}
	if deleted, err := r.GC(); err == nil || deleted != 0 {
		t.Fatalf("保护快照损坏时 GC 应失败且不删除数据: deleted=%d err=%v", deleted, err)
	}
	if _, err := be.Get(blobName); err != nil {
		t.Fatalf("GC 失败后孤儿 blob 不应被删除: %v", err)
	}
	db.Close()
}

func TestMetaBackupRetention(t *testing.T) {
	r, db, be, key, _ := newMetaBackupRepo(t)
	attachTestFile(t, r, "file.txt", []byte("content"))
	for i := 0; i < 3; i++ {
		if _, err := CreateMetaBackup(context.Background(), db, be, key, 2); err != nil {
			t.Fatal(err)
		}
	}
	names, err := be.ListMetaContext(context.Background())
	if err != nil || len(names) != 2 {
		t.Fatalf("应只保留两个 Meta 版本: %v %v", names, err)
	}
	db.Close()
}
