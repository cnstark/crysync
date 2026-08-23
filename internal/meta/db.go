// internal/meta/db.go
package meta

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type DB struct {
	db *sql.DB
}

type FileRow struct {
	Path       string
	IsDir      bool
	IsSymlink  bool
	Mode       uint32
	UID        int
	GID        int
	Size       int64
	MTimeNs    int64
	Xattrs     []byte
	LinkTarget string
}

const schema = `
CREATE TABLE IF NOT EXISTS snapshots (
	id          INTEGER PRIMARY KEY,
	created_at  TEXT NOT NULL,
	label       TEXT,
	complete    INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS files (
	id          INTEGER PRIMARY KEY,
	snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
	path        TEXT NOT NULL,
	is_dir      INTEGER NOT NULL,
	is_symlink  INTEGER NOT NULL DEFAULT 0,
	mode        INTEGER NOT NULL,
	uid         INTEGER,
	gid         INTEGER,
	size        INTEGER NOT NULL DEFAULT 0,
	mtime_ns    INTEGER NOT NULL DEFAULT 0,
	xattrs      BLOB,
	link_target TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_snapshot ON files(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
CREATE TABLE IF NOT EXISTS chunks (
	id        INTEGER PRIMARY KEY,
	hash      BLOB NOT NULL UNIQUE,
	size      INTEGER NOT NULL,
	refcount  INTEGER NOT NULL DEFAULT 0,
	blob_name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_chunks (
	file_id  INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
	chunk_id INTEGER NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
	idx      INTEGER NOT NULL,
	PRIMARY KEY (file_id, idx)
);
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT
);
`

func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("创建元数据目录: %w", err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("打开 SQLite: %w", err)
	}
	db.SetMaxOpenConns(1) // 单写者：SQLite WAL 下由应用串行化
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("初始化 schema: %w", err)
	}
	return &DB{db: db}, nil
}

func (d *DB) Close() error { return d.db.Close() }

func (d *DB) CreateSnapshot(createdAt time.Time) (int64, error) {
	res, err := d.db.Exec(`INSERT INTO snapshots (created_at) VALUES (?)`, createdAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d *DB) CopyFiles(fromSnapshot, toSnapshot int64) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 复制文件清单
	if _, err := tx.Exec(`INSERT INTO files (snapshot_id, path, is_dir, is_symlink, mode, uid, gid, size, mtime_ns, xattrs, link_target)
		SELECT ?, path, is_dir, is_symlink, mode, uid, gid, size, mtime_ns, xattrs, link_target FROM files WHERE snapshot_id = ?`,
		toSnapshot, fromSnapshot); err != nil {
		return err
	}
	// 复制 chunk 关联（快照自包含的关键：继承文件必须可读）——通过 path 关联新旧快照的 file 行
	if _, err := tx.Exec(`INSERT INTO file_chunks (file_id, chunk_id, idx)
		SELECT t.id, fc.chunk_id, fc.idx
		FROM file_chunks fc
		JOIN files s ON s.id = fc.file_id AND s.snapshot_id = ?
		JOIN files t ON t.snapshot_id = ? AND t.path = s.path`,
		fromSnapshot, toSnapshot); err != nil {
		return err
	}
	// 继承引用递增 refcount（同一 chunk 被多个快照引用时计数正确）
	if _, err := tx.Exec(`UPDATE chunks SET refcount = refcount + (
		SELECT COUNT(*) FROM file_chunks fc JOIN files f ON f.id = fc.file_id
		WHERE f.snapshot_id = ? AND fc.chunk_id = chunks.id)`,
		toSnapshot); err != nil {
		return err
	}
	return tx.Commit()
}

// UpsertFile 覆盖写入文件行：旧行的 chunk 引用递减（归零删除 chunk 行——blob
// 随之成为孤儿，由 GC 回收），再写入新行。chunk 引用递增由调用方负责。
func (d *DB) UpsertFile(snapshotID int64, f FileRow) (int64, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE chunks SET refcount = refcount - (
		SELECT COUNT(*) FROM file_chunks fc JOIN files f ON f.id = fc.file_id
		WHERE f.snapshot_id = ? AND f.path = ? AND fc.chunk_id = chunks.id)`,
		snapshotID, f.Path); err != nil {
		return 0, err
	}
	// 只清理"旧文件曾引用、本次递减后归零"的 chunk——新插入尚未关联的
	// chunk（refcount=0）不能被误删
	if _, err := tx.Exec(`DELETE FROM chunks WHERE refcount <= 0 AND id IN (
		SELECT DISTINCT fc.chunk_id FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id WHERE f.snapshot_id = ? AND f.path = ?)`,
		snapshotID, f.Path); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`DELETE FROM files WHERE snapshot_id = ? AND path = ?`, snapshotID, f.Path); err != nil {
		return 0, err
	}
	res, err := tx.Exec(`INSERT INTO files (snapshot_id, path, is_dir, is_symlink, mode, uid, gid, size, mtime_ns, xattrs, link_target)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshotID, f.Path, boolToInt(f.IsDir), boolToInt(f.IsSymlink), f.Mode, f.UID, f.GID, f.Size, f.MTimeNs, f.Xattrs, nullString(f.LinkTarget))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// DeleteFile 删除文件行，其 chunk 引用递减（归零删除 chunk 行）。
func (d *DB) DeleteFile(snapshotID int64, path string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE chunks SET refcount = refcount - (
		SELECT COUNT(*) FROM file_chunks fc JOIN files f ON f.id = fc.file_id
		WHERE f.snapshot_id = ? AND f.path = ? AND fc.chunk_id = chunks.id)`,
		snapshotID, path); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM chunks WHERE refcount <= 0 AND id IN (
		SELECT DISTINCT fc.chunk_id FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id WHERE f.snapshot_id = ? AND f.path = ?)`,
		snapshotID, path); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM files WHERE snapshot_id = ? AND path = ?`, snapshotID, path); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) GetFiles(snapshotID int64) ([]FileRow, error) {
	rows, err := d.db.Query(`SELECT path, is_dir, is_symlink, mode, uid, gid, size, mtime_ns, xattrs, link_target
		FROM files WHERE snapshot_id = ? ORDER BY path`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FileRow
	for rows.Next() {
		var f FileRow
		var isDir, isSym int
		var uid, gid sql.NullInt64
		var xattrs []byte
		var link sql.NullString
		if err := rows.Scan(&f.Path, &isDir, &isSym, &f.Mode, &uid, &gid, &f.Size, &f.MTimeNs, &xattrs, &link); err != nil {
			return nil, err
		}
		f.IsDir = isDir != 0
		f.IsSymlink = isSym != 0
		f.UID, f.GID = int(uid.Int64), int(gid.Int64)
		f.Xattrs = xattrs
		f.LinkTarget = link.String
		out = append(out, f)
	}
	return out, rows.Err()
}

func (d *DB) SetMeta(key, value string) error {
	_, err := d.db.Exec(`INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

func (d *DB) GetMeta(key string) (string, bool, error) {
	var v string
	err := d.db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

type ChunkInfo struct {
	ID       int64
	BlobName string
	Size     int64
}

// SnapshotInfo 快照列表条目（prune 策略计算用）。
type SnapshotInfo struct {
	ID        int64
	CreatedAt time.Time
}

// SnapshotFileCount 返回快照中的文件条目数。
func (d *DB) SnapshotFileCount(snapshotID int64) (int, error) {
	var n int
	err := d.db.QueryRow(`SELECT COUNT(*) FROM files WHERE snapshot_id = ?`, snapshotID).Scan(&n)
	return n, err
}

// SnapshotList 返回全部快照（按创建时间升序，即 id 序）。
func (d *DB) SnapshotList() ([]SnapshotInfo, error) {
	rows, err := d.db.Query(`SELECT id, created_at FROM snapshots ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotInfo
	for rows.Next() {
		var s SnapshotInfo
		var created string
		if err := rows.Scan(&s.ID, &created); err != nil {
			return nil, err
		}
		if t, err := time.Parse(time.RFC3339Nano, created); err == nil {
			s.CreatedAt = t
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

type ChunkRef struct {
	ChunkID int64
	IDX     int
}

func (d *DB) FindChunkByHash(hash [32]byte) (ChunkInfo, bool, error) {
	var ci ChunkInfo
	err := d.db.QueryRow(`SELECT id, blob_name, size FROM chunks WHERE hash = ?`, hash[:]).Scan(&ci.ID, &ci.BlobName, &ci.Size)
	if err == sql.ErrNoRows {
		return ChunkInfo{}, false, nil
	}
	if err != nil {
		return ChunkInfo{}, false, err
	}
	return ci, true, nil
}

func (d *DB) InsertChunk(hash [32]byte, blobName string, size int64) (int64, error) {
	res, err := d.db.Exec(`INSERT INTO chunks (hash, size, refcount, blob_name) VALUES (?, ?, 0, ?)`, hash[:], size, blobName)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (d *DB) IncrRefcount(chunkID int64) error {
	_, err := d.db.Exec(`UPDATE chunks SET refcount = refcount + 1 WHERE id = ?`, chunkID)
	return err
}

func (d *DB) AttachChunks(fileID int64, refs []ChunkRef) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, r := range refs {
		if _, err := tx.Exec(`INSERT INTO file_chunks (file_id, chunk_id, idx) VALUES (?, ?, ?)`, fileID, r.ChunkID, r.IDX); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// DeleteSnapshot 删除快照及其文件行；快照引用的全部 chunk refcount 递减
// （按每个文件关联计数，去重同 chunk 多文件引用），归零者删除 chunk 行——
// 对应 blob 成为孤儿，由 GC 回收。
func (d *DB) DeleteSnapshot(snapshotID int64) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE chunks SET refcount = refcount - (
		SELECT COUNT(*) FROM file_chunks fc JOIN files f ON f.id = fc.file_id
		WHERE f.snapshot_id = ? AND fc.chunk_id = chunks.id)`,
		snapshotID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM chunks WHERE refcount <= 0 AND id IN (
		SELECT DISTINCT fc.chunk_id FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id WHERE f.snapshot_id = ?)`,
		snapshotID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM snapshots WHERE id = ?`, snapshotID); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) QueryRow(q string, args ...any) *sql.Row { return d.db.QueryRow(q, args...) }

func (d *DB) Query(q string, args ...any) (*sql.Rows, error) { return d.db.Query(q, args...) }
