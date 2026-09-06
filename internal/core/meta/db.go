// internal/core/meta/db.go
package meta

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"modernc.org/sqlite"
)

type DB struct {
	db   *sql.DB
	path string
}

// DBPath 返回元数据库文件路径（repo 按模块共享写锁的互斥键）。
func (d *DB) DBPath() string { return d.path }

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

// schema：v0.5 单一当前状态模型——files 表即仓库清单（无 snapshots 表、
// 无 snapshot_id 列），写操作原地 upsert；chunks 按内容寻址跨文件共享
// （refcount = 文件引用计数）；file_chunks 关联文件与块。
const schema = `
CREATE TABLE IF NOT EXISTS files (
	id          INTEGER PRIMARY KEY,
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
-- chunk_id 索引：refcount 关联子查询（UpsertFile/DeleteFile 的
-- UPDATE chunks ... fc.chunk_id = chunks.id）没有它时对 chunks 每行全表扫
-- 描 file_chunks，千级行清单上单请求累计 >1s；有索引后毫秒级
CREATE INDEX IF NOT EXISTS idx_file_chunks_chunk ON file_chunks(chunk_id);
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT
);
`

func Open(path string) (*DB, error) {
	return open(path, true)
}

// OpenExisting 打开已有 Meta，绝不创建文件或补建缺失 schema。
func OpenExisting(path string) (*DB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("打开已有元数据库: %w", err)
	}
	return open(path, false)
}

func open(path string, create bool) (*DB, error) {
	if !create {
		if _, err := os.Stat(path); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("创建元数据目录: %w", err)
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("打开 SQLite: %w", err)
	}
	db.SetMaxOpenConns(1) // 单写者：SQLite WAL 下由应用串行化
	// 旧版检测先行（旧 schema 有 snapshots 表）：v0.5 删除快照功能且不提供
	// 迁移——报错提示人工处理，避免在旧库上新建一套新表造成两套清单并存。
	oldDB, err := isOldSchema(db)
	if err != nil {
		db.Close()
		return nil, err
	}
	if oldDB {
		db.Close()
		return nil, fmt.Errorf("检测到 v0.4.x 旧版元数据库（含快照表）：v0.5 已删除快照功能且不提供迁移。"+
			"请删除 %s 后重新初始化（需对新架构重新备份；旧后端 blob 将不可见）", path)
	}
	// schema 建库包在单事务内：rsync 前端 EnsureModuleInit 与 webdav 前端
	// OpenModule 启动时并发打开同一模块库（main 起 goroutine 各自执行），
	// 多语句 Exec 曾在 NAS 慢盘上交错触发 SQLITE_BUSY（busy_timeout 计的是
	// 单条语句等待，事务化后并发方整体排队，幂等且无窗口）
	if create {
		err = ensureSchema(db)
	} else {
		err = validateRequiredSchema(db)
	}
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("校验 schema: %w", err)
	}
	return &DB{db: db, path: path}, nil
}

func validateRequiredSchema(db *sql.DB) error {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('files','chunks','file_chunks','meta')`).Scan(&count); err != nil {
		return err
	}
	if count != 4 {
		return fmt.Errorf("缺少必需表")
	}
	return nil
}

// isOldSchema 检测是否为 v0.4.x 旧版库（存在 snapshots 表）。
func isOldSchema(db *sql.DB) (bool, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='snapshots'`).Scan(&n)
	return n > 0, err
}

// ensureSchema 在单事务内执行全部 DDL（IF NOT EXISTS 幂等）。
func ensureSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // 提交后 Rollback 是 no-op
	if _, err := tx.Exec(schema); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) Close() error { return d.db.Close() }

type sqliteBackuper interface {
	NewBackup(string) (*sqlite.Backup, error)
}

// BackupTo 使用 SQLite Online Backup API 生成包含 WAL 已提交内容的一致快照。
func (d *DB) BackupTo(ctx context.Context, dstPath string) error {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(driverConn any) error {
		b, ok := driverConn.(sqliteBackuper)
		if !ok {
			return fmt.Errorf("SQLite 驱动不支持在线备份")
		}
		backup, err := b.NewBackup(dstPath)
		if err != nil {
			return err
		}
		more, err := backup.Step(-1)
		if err != nil {
			_ = backup.Finish()
			return err
		}
		if more {
			_ = backup.Finish()
			return fmt.Errorf("SQLite 在线备份未完成")
		}
		return backup.Finish()
	})
}

// EnsureRepositoryID 返回稳定仓库 ID；旧库首次备份时补写。
func (d *DB) EnsureRepositoryID() (string, error) {
	if err := d.SetMeta("repository_schema", "1"); err != nil {
		return "", err
	}
	if value, ok, err := d.GetMeta("repository_id"); err != nil {
		return "", err
	} else if ok {
		if raw, err := hex.DecodeString(value); err != nil || len(raw) != 16 {
			return "", fmt.Errorf("repository_id 格式错误")
		}
		return value, nil
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	value := hex.EncodeToString(id[:])
	if err := d.SetMeta("repository_id", value); err != nil {
		return "", err
	}
	return value, nil
}

// UpsertFile 原子替换文件行：事务内 递减旧引用 → 递增新 refs 引用 →
// 删除归零的旧 chunk 行 → 替换 files 行 → 附加 file_chunks。
// refs 递增必须与旧引用递减同事务：相同内容的覆盖（新旧引用同一 chunk）若
// 把递增推迟到事务外，递减归零会在 attach 前删掉行 → AttachChunks 外键失败。
func (d *DB) UpsertFile(f FileRow, refs []ChunkRef) (int64, error) {
	tx, err := d.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	// 1) 旧引用递减
	if _, err := tx.Exec(`UPDATE chunks SET refcount = refcount - (
		SELECT COUNT(*) FROM file_chunks fc JOIN files f ON f.id = fc.file_id
		WHERE f.path = ? AND fc.chunk_id = chunks.id)`,
		f.Path); err != nil {
		return 0, err
	}
	// 2) 新引用先递增（与旧引用相同的 chunk 递增回正，避免步骤 3 误删）
	for _, r := range refs {
		if _, err := tx.Exec(`UPDATE chunks SET refcount = refcount + 1 WHERE id = ?`, r.ChunkID); err != nil {
			return 0, err
		}
	}
	// 3) 只清理"旧文件曾引用、递减后仍归零"的 chunk（其 blob 成孤儿，由 GC 回收）
	if _, err := tx.Exec(`DELETE FROM chunks WHERE refcount <= 0 AND id IN (
		SELECT DISTINCT fc.chunk_id FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id WHERE f.path = ?)`,
		f.Path); err != nil {
		return 0, err
	}
	// 4) 替换文件行
	if _, err := tx.Exec(`DELETE FROM files WHERE path = ?`, f.Path); err != nil {
		return 0, err
	}
	res, err := tx.Exec(`INSERT INTO files (path, is_dir, is_symlink, mode, uid, gid, size, mtime_ns, xattrs, link_target)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		f.Path, boolToInt(f.IsDir), boolToInt(f.IsSymlink), f.Mode, f.UID, f.GID, f.Size, f.MTimeNs, f.Xattrs, nullString(f.LinkTarget))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	// 5) 附加 file_chunks（refs 已在步骤 2 递增，此处只建关联行）
	for _, r := range refs {
		if _, err := tx.Exec(`INSERT INTO file_chunks (file_id, chunk_id, idx) VALUES (?, ?, ?)`, id, r.ChunkID, r.IDX); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

// DeleteFile 删除文件行，其 chunk 引用递减（归零删除 chunk 行）。
func (d *DB) DeleteFile(path string) error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE chunks SET refcount = refcount - (
		SELECT COUNT(*) FROM file_chunks fc JOIN files f ON f.id = fc.file_id
		WHERE f.path = ? AND fc.chunk_id = chunks.id)`,
		path); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM chunks WHERE refcount <= 0 AND id IN (
		SELECT DISTINCT fc.chunk_id FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id WHERE f.path = ?)`,
		path); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM files WHERE path = ?`, path); err != nil {
		return err
	}
	return tx.Commit()
}

// GetFiles 返回全部文件清单（按 path 排序）。单一状态模型下即完整仓库状态。
func (d *DB) GetFiles() ([]FileRow, error) {
	rows, err := d.db.Query(`SELECT path, is_dir, is_symlink, mode, uid, gid, size, mtime_ns, xattrs, link_target
		FROM files ORDER BY path`)
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

// GetFileRow 按路径查询单行文件元数据；不存在返回 ok=false。
// quick check 用：字段扫描与 GetFiles 一致（is_dir/is_symlink 为 int、uid/gid 可空）。
func (d *DB) GetFileRow(path string) (FileRow, bool, error) {
	var f FileRow
	var isDir, isSym int
	var uid, gid sql.NullInt64
	var xattrs []byte
	var link sql.NullString
	err := d.db.QueryRow(`SELECT path, is_dir, is_symlink, mode, uid, gid, size, mtime_ns, xattrs, link_target
		FROM files WHERE path = ?`, path).
		Scan(&f.Path, &isDir, &isSym, &f.Mode, &uid, &gid, &f.Size, &f.MTimeNs, &xattrs, &link)
	if err == sql.ErrNoRows {
		return FileRow{}, false, nil
	}
	if err != nil {
		return FileRow{}, false, err
	}
	f.IsDir = isDir != 0
	f.IsSymlink = isSym != 0
	f.UID, f.GID = int(uid.Int64), int(gid.Int64)
	f.Xattrs = xattrs
	f.LinkTarget = link.String
	return f, true, nil
}

// GetFileChunks 返回文件中 (chunk_id, idx) 有序列表（MovePath 复制引用用）。
func (d *DB) GetFileChunks(path string) ([]ChunkRef, error) {
	rows, err := d.db.Query(`SELECT fc.chunk_id, fc.idx FROM file_chunks fc
		JOIN files f ON f.id = fc.file_id
		WHERE f.path = ? ORDER BY fc.idx`, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChunkRef
	for rows.Next() {
		var c ChunkRef
		if err := rows.Scan(&c.ChunkID, &c.IDX); err != nil {
			return nil, err
		}
		out = append(out, c)
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

// DeleteChunk 按 ID 删除 chunk 行（GC 回收用：行已按 file_chunks JOIN files
// 真引用判定为无引用残留，不再看 refcount）。
func (d *DB) DeleteChunk(chunkID int64) error {
	_, err := d.db.Exec(`DELETE FROM chunks WHERE id = ?`, chunkID)
	return err
}

// DeleteChunkIfZero 删除 refcount 为 0 的 chunk 行（未附任何文件引用的孤儿）。
// 写失败收口用：本次新建但未 AttachChunks 的 chunk 行残留（其 blob 因
// chunks 表仍引用而 GC 无法回收），出错路径显式清理。绝不误删有引用的 chunk。
func (d *DB) DeleteChunkIfZero(chunkID int64) error {
	_, err := d.db.Exec(`DELETE FROM chunks WHERE id = ? AND refcount <= 0`, chunkID)
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

func (d *DB) QueryRow(q string, args ...any) *sql.Row { return d.db.QueryRow(q, args...) }

func (d *DB) Query(q string, args ...any) (*sql.Rows, error) { return d.db.Query(q, args...) }
