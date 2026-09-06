package meta

import (
	"database/sql"
	"fmt"
)

type BackupChunk struct {
	Hash     [32]byte
	BlobName string
	Size     int64
}

// ValidateSnapshot 对只读快照做物理、关系和仓库语义校验，并返回被文件引用的唯一 chunk。
func ValidateSnapshot(path, expectedRepositoryID string) ([]BackupChunk, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := validateRequiredSchema(db); err != nil {
		return nil, err
	}
	rows, err := db.Query(`PRAGMA integrity_check`)
	if err != nil {
		return nil, err
	}
	var results []string
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			rows.Close()
			return nil, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(results) != 1 || results[0] != "ok" {
		return nil, fmt.Errorf("SQLite integrity_check 失败: %v", results)
	}
	rows, err = db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		return nil, err
	}
	if rows.Next() {
		rows.Close()
		return nil, fmt.Errorf("SQLite foreign_key_check 失败")
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	var repositoryID string
	if err := db.QueryRow(`SELECT value FROM meta WHERE key='repository_id'`).Scan(&repositoryID); err != nil {
		return nil, fmt.Errorf("读取 repository_id: %w", err)
	}
	if expectedRepositoryID != "" && repositoryID != expectedRepositoryID {
		return nil, fmt.Errorf("repository_id 不匹配")
	}
	var repositorySchema string
	if err := db.QueryRow(`SELECT value FROM meta WHERE key='repository_schema'`).Scan(&repositorySchema); err != nil {
		return nil, fmt.Errorf("读取 repository_schema: %w", err)
	}
	if repositorySchema != "1" {
		return nil, fmt.Errorf("repository_schema 不受支持: %q", repositorySchema)
	}
	checks := []struct {
		name string
		q    string
	}{
		{"文件路径重复", `SELECT COUNT(*) FROM (SELECT path FROM files GROUP BY path HAVING COUNT(*) > 1)`},
		{"chunk 引用计数不一致", `SELECT COUNT(*) FROM chunks c WHERE c.refcount != (SELECT COUNT(*) FROM file_chunks fc WHERE fc.chunk_id=c.id)`},
		{"chunk 序号不连续", `SELECT COUNT(*) FROM (SELECT file_id FROM file_chunks GROUP BY file_id HAVING MIN(idx) != 0 OR MAX(idx) != COUNT(*)-1)`},
		{"文件大小与 chunk 不一致", `SELECT COUNT(*) FROM (SELECT f.id FROM files f LEFT JOIN file_chunks fc ON fc.file_id=f.id LEFT JOIN chunks c ON c.id=fc.chunk_id WHERE f.is_dir=0 AND f.is_symlink=0 GROUP BY f.id HAVING f.size != COALESCE(SUM(c.size),0))`},
	}
	for _, check := range checks {
		var count int
		if err := db.QueryRow(check.q).Scan(&count); err != nil {
			return nil, err
		}
		if count != 0 {
			return nil, fmt.Errorf("%s: %d", check.name, count)
		}
	}

	rows, err = db.Query(`SELECT DISTINCT c.hash, c.blob_name, c.size FROM chunks c JOIN file_chunks fc ON fc.chunk_id=c.id JOIN files f ON f.id=fc.file_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chunks []BackupChunk
	for rows.Next() {
		var chunk BackupChunk
		var hash []byte
		if err := rows.Scan(&hash, &chunk.BlobName, &chunk.Size); err != nil {
			return nil, err
		}
		if len(hash) != len(chunk.Hash) {
			return nil, fmt.Errorf("chunk %s hash 长度错误", chunk.BlobName)
		}
		copy(chunk.Hash[:], hash)
		chunks = append(chunks, chunk)
	}
	return chunks, rows.Err()
}
