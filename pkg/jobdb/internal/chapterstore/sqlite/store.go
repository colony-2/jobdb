package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
)

const driverName = "sqlite"

const storyColumns = "anthology_id, story_id, created_at_ns, updated_at_ns, finalized, deleted, deleted_at_ns, base_anthology_id, base_story_id, base_fork_ordinal, chapter_count, latest_ordinal"

// Store persists JobDB chapter rows using SQLite.
type Store struct {
	db      *sql.DB
	closeFn func() error
}

// Open initializes a SQLite rowstore at path.
func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("sqlite chapterstore: path is required")
	}
	dsn := pathToDSN(path)
	return OpenDSN(context.Background(), dsn)
}

// OpenDSN initializes a SQLite rowstore from a DSN and owns the database handle.
func OpenDSN(ctx context.Context, dsn string) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("sqlite chapterstore: dsn is required")
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite chapterstore: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &Store{db: db, closeFn: db.Close}
	if err := store.configureOwned(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := store.Migrate(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// New wraps an existing database handle. The caller retains ownership.
func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite chapterstore: db is required")
	}
	store := &Store{db: db}
	if err := store.Migrate(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}

// Close releases resources owned by this store.
func (s *Store) Close() error {
	if s == nil || s.closeFn == nil {
		return nil
	}
	return s.closeFn()
}

// Health verifies the database is reachable.
func (s *Store) Health(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite chapterstore: store not initialized")
	}
	return s.db.PingContext(ctx)
}

// Migrate installs the rowstore schema.
func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("sqlite chapterstore: store not initialized")
	}
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("sqlite chapterstore: migrate: %w", err)
	}
	if err := s.ensureStoryColumn(ctx, "deleted", "deleted INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0, 1))"); err != nil {
		return err
	}
	if err := s.ensureStoryColumn(ctx, "deleted_at_ns", "deleted_at_ns INTEGER"); err != nil {
		return err
	}
	if err := s.ensureStoryColumn(ctx, "base_anthology_id", "base_anthology_id TEXT"); err != nil {
		return err
	}
	if err := s.ensureStoryColumn(ctx, "base_story_id", "base_story_id TEXT"); err != nil {
		return err
	}
	if err := s.ensureStoryColumn(ctx, "base_fork_ordinal", "base_fork_ordinal INTEGER"); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureStoryColumn(ctx context.Context, name, definition string) error {
	exists, err := s.storyColumnExists(ctx, name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, "ALTER TABLE jobdb_chapter_stories ADD COLUMN "+definition); err != nil {
		return fmt.Errorf("sqlite chapterstore: add %s column: %w", name, err)
	}
	return nil
}

func (s *Store) storyColumnExists(ctx context.Context, name string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(jobdb_chapter_stories)`)
	if err != nil {
		return false, fmt.Errorf("sqlite chapterstore: inspect story columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid          int
			columnName   string
			columnType   string
			notNull      int
			defaultValue sql.NullString
			primaryKey   int
		)
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, fmt.Errorf("sqlite chapterstore: scan story column: %w", err)
		}
		if columnName == name {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("sqlite chapterstore: inspect story columns: %w", err)
	}
	return false, nil
}

func (s *Store) configureOwned(ctx context.Context) error {
	pragmas := []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	}
	for _, pragma := range pragmas {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("sqlite chapterstore: %s: %w", pragma, err)
		}
	}
	return nil
}

// CreateStory inserts a fresh metadata row when absent.
func (s *Store) CreateStory(ctx context.Context, key storage.StoryKey, now time.Time) (storage.StoryRecord, error) {
	if err := s.Health(ctx); err != nil {
		return storage.StoryRecord{}, err
	}
	rec := storage.StoryRecord{
		Key:           key,
		CreatedAt:     now.UTC(),
		UpdatedAt:     now.UTC(),
		LatestOrdinal: -1,
	}
	if err := storage.ValidateStoryRecord(rec); err != nil {
		return storage.StoryRecord{}, err
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO jobdb_chapter_stories (
	anthology_id, story_id, created_at_ns, updated_at_ns, finalized, deleted, deleted_at_ns,
	base_anthology_id, base_story_id, base_fork_ordinal, chapter_count, latest_ordinal
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		key.AnthologyID, key.StoryID, timeToNS(rec.CreatedAt), timeToNS(rec.UpdatedAt), boolToInt(rec.Finalized), boolToInt(rec.Deleted), nil,
		nil, nil, nil, rec.ChapterCount, rec.LatestOrdinal,
	)
	if err != nil {
		if isConstraint(err) {
			return storage.StoryRecord{}, storage.ErrStoryExists
		}
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: create story: %w", err)
	}
	return rec, nil
}

// CreateLinkedStory inserts a fresh metadata row that inherits pre-fork chapters from another story.
func (s *Store) CreateLinkedStory(ctx context.Context, key storage.StoryKey, link storage.StoryLink, now time.Time) (storage.StoryRecord, error) {
	if err := s.Health(ctx); err != nil {
		return storage.StoryRecord{}, err
	}
	if err := storage.ValidateStoryLink(key, link); err != nil {
		return storage.StoryRecord{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: begin create linked story: %w", err)
	}
	defer rollback(tx)

	base, err := scanStory(tx.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = ? AND story_id = ?`,
		link.Key.AnthologyID, link.Key.StoryID,
	))
	if err != nil {
		return storage.StoryRecord{}, err
	}
	if link.ForkOrdinal > base.LatestOrdinal {
		return storage.StoryRecord{}, fmt.Errorf("%w: fork ordinal exceeds base latest ordinal", storage.ErrInvalidRecord)
	}

	rec := storage.StoryRecord{
		Key:           key,
		CreatedAt:     now.UTC(),
		UpdatedAt:     now.UTC(),
		Link:          &storage.StoryLink{Key: link.Key, ForkOrdinal: link.ForkOrdinal},
		ChapterCount:  link.ForkOrdinal + 1,
		LatestOrdinal: link.ForkOrdinal,
	}
	if err := storage.ValidateStoryRecord(rec); err != nil {
		return storage.StoryRecord{}, err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO jobdb_chapter_stories (
	anthology_id, story_id, created_at_ns, updated_at_ns, finalized, deleted, deleted_at_ns,
	base_anthology_id, base_story_id, base_fork_ordinal, chapter_count, latest_ordinal
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		key.AnthologyID, key.StoryID, timeToNS(rec.CreatedAt), timeToNS(rec.UpdatedAt), boolToInt(rec.Finalized), boolToInt(rec.Deleted), nil,
		link.Key.AnthologyID, link.Key.StoryID, link.ForkOrdinal, rec.ChapterCount, rec.LatestOrdinal,
	)
	if err != nil {
		if isConstraint(err) {
			return storage.StoryRecord{}, storage.ErrStoryExists
		}
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: create linked story: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: commit create linked story: %w", err)
	}
	return rec, nil
}

// GetStory loads metadata for the provided key.
func (s *Store) GetStory(ctx context.Context, key storage.StoryKey) (storage.StoryRecord, error) {
	if err := s.Health(ctx); err != nil {
		return storage.StoryRecord{}, err
	}
	return scanStory(s.db.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = ? AND story_id = ? AND deleted = 0`,
		key.AnthologyID, key.StoryID,
	))
}

// GetStoryIncludingDeleted loads metadata for active and tombstoned stories.
func (s *Store) GetStoryIncludingDeleted(ctx context.Context, key storage.StoryKey) (storage.StoryRecord, error) {
	if err := s.Health(ctx); err != nil {
		return storage.StoryRecord{}, err
	}
	return scanStory(s.db.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = ? AND story_id = ?`,
		key.AnthologyID, key.StoryID,
	))
}

// Publish marks a story as finalized.
func (s *Store) Publish(ctx context.Context, key storage.StoryKey, now time.Time) (storage.StoryRecord, error) {
	if err := s.Health(ctx); err != nil {
		return storage.StoryRecord{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: begin publish: %w", err)
	}
	defer rollback(tx)

	rec, err := scanStory(tx.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = ? AND story_id = ? AND deleted = 0`,
		key.AnthologyID, key.StoryID,
	))
	if err != nil {
		return storage.StoryRecord{}, err
	}
	if rec.Finalized {
		return storage.StoryRecord{}, storage.ErrStoryFinalized
	}
	rec.Finalized = true
	rec.UpdatedAt = now.UTC()

	if _, err := tx.ExecContext(ctx, `
UPDATE jobdb_chapter_stories
SET updated_at_ns = ?, finalized = 1
WHERE anthology_id = ? AND story_id = ? AND deleted = 0`,
		timeToNS(rec.UpdatedAt), key.AnthologyID, key.StoryID,
	); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: publish: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: commit publish: %w", err)
	}
	return rec, nil
}

// TombstoneStory marks an already-finalized story as deleted while retaining its rows.
func (s *Store) TombstoneStory(ctx context.Context, key storage.StoryKey, now time.Time) (storage.StoryRecord, error) {
	if err := s.Health(ctx); err != nil {
		return storage.StoryRecord{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: begin tombstone: %w", err)
	}
	defer rollback(tx)

	rec, err := scanStory(tx.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = ? AND story_id = ? AND deleted = 0`,
		key.AnthologyID, key.StoryID,
	))
	if err != nil {
		return storage.StoryRecord{}, err
	}
	if !rec.Finalized {
		return storage.StoryRecord{}, fmt.Errorf("%w: story must be finalized before tombstone", storage.ErrInvalidRecord)
	}

	deletedAt := now.UTC()
	rec.Deleted = true
	rec.DeletedAt = deletedAt
	rec.UpdatedAt = deletedAt
	if err := storage.ValidateStoryTombstone(rec); err != nil {
		return storage.StoryRecord{}, err
	}

	result, err := tx.ExecContext(ctx, `
UPDATE jobdb_chapter_stories
SET updated_at_ns = ?, deleted = 1, deleted_at_ns = ?
WHERE anthology_id = ? AND story_id = ? AND deleted = 0`,
		timeToNS(rec.UpdatedAt), timeToNS(rec.DeletedAt), key.AnthologyID, key.StoryID,
	)
	if err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: tombstone story: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return storage.StoryRecord{}, storage.ErrStoryNotFound
	}
	if err := tx.Commit(); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: commit tombstone: %w", err)
	}
	return rec, nil
}

// AppendChapter writes a chapter when the ordinal is unused.
func (s *Store) AppendChapter(ctx context.Context, rec storage.ChapterRecord, now time.Time) (storage.StoryRecord, error) {
	if err := s.Health(ctx); err != nil {
		return storage.StoryRecord{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: begin append: %w", err)
	}
	defer rollback(tx)

	meta, err := scanStory(tx.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = ? AND story_id = ? AND deleted = 0`,
		rec.Key.AnthologyID, rec.Key.StoryID,
	))
	if err != nil {
		return storage.StoryRecord{}, err
	}
	if meta.Finalized {
		return storage.StoryRecord{}, storage.ErrStoryFinalized
	}

	var exists int
	err = tx.QueryRowContext(ctx, `
SELECT 1 FROM jobdb_chapter_chapters
WHERE anthology_id = ? AND story_id = ? AND ordinal = ?`,
		rec.Key.AnthologyID, rec.Key.StoryID, rec.Ordinal,
	).Scan(&exists)
	if err == nil {
		return storage.StoryRecord{}, storage.ErrChapterExists
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: check chapter: %w", err)
	}
	if err := storage.ValidateChapterAppend(meta, rec); err != nil {
		return storage.StoryRecord{}, err
	}

	rec.CreatedAt = now.UTC()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO jobdb_chapter_chapters (anthology_id, story_id, ordinal, body, created_at_ns)
VALUES (?, ?, ?, ?, ?)`,
		rec.Key.AnthologyID, rec.Key.StoryID, rec.Ordinal, []byte(rec.Body), timeToNS(rec.CreatedAt),
	); err != nil {
		if isConstraint(err) {
			return storage.StoryRecord{}, storage.ErrChapterExists
		}
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: insert chapter: %w", err)
	}

	for i, art := range rec.Artifacts {
		var blobPath any
		if art.BlobPath != "" {
			blobPath = art.BlobPath
		}
		var inlineData any
		if art.InlineData != nil {
			inlineData = art.InlineData
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO jobdb_chapter_artifacts (
	anthology_id, story_id, ordinal, position, id, name, content_type, size_bytes, sha256, inline_data, blob_path
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			rec.Key.AnthologyID, rec.Key.StoryID, rec.Ordinal, i, art.ID, art.Name, art.ContentType, art.SizeBytes, art.Sha256, inlineData, blobPath,
		); err != nil {
			return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: insert artifact: %w", err)
		}
	}

	meta.UpdatedAt = now.UTC()
	if rec.Ordinal > meta.LatestOrdinal {
		meta.LatestOrdinal = rec.Ordinal
	}
	if meta.ChapterCount < rec.Ordinal+1 {
		meta.ChapterCount = rec.Ordinal + 1
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE jobdb_chapter_stories
SET updated_at_ns = ?, chapter_count = ?, latest_ordinal = ?
WHERE anthology_id = ? AND story_id = ? AND deleted = 0`,
		timeToNS(meta.UpdatedAt), meta.ChapterCount, meta.LatestOrdinal, rec.Key.AnthologyID, rec.Key.StoryID,
	); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: update story after append: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: commit append: %w", err)
	}
	return meta, nil
}

// ReadChapter loads a chapter record by ordinal.
func (s *Store) ReadChapter(ctx context.Context, key storage.StoryKey, ordinal int64) (storage.ChapterRecord, error) {
	return s.readChapter(ctx, key, ordinal, false)
}

// ReadChapterIncludingDeleted loads a local chapter from active or tombstoned stories.
func (s *Store) ReadChapterIncludingDeleted(ctx context.Context, key storage.StoryKey, ordinal int64) (storage.ChapterRecord, error) {
	return s.readChapter(ctx, key, ordinal, true)
}

func (s *Store) readChapter(ctx context.Context, key storage.StoryKey, ordinal int64, includeDeleted bool) (storage.ChapterRecord, error) {
	if err := s.Health(ctx); err != nil {
		return storage.ChapterRecord{}, err
	}
	if includeDeleted {
		if _, err := s.GetStoryIncludingDeleted(ctx, key); err != nil {
			return storage.ChapterRecord{}, err
		}
	} else {
		if _, err := s.GetStory(ctx, key); err != nil {
			return storage.ChapterRecord{}, err
		}
	}
	var body []byte
	var createdNS int64
	err := s.db.QueryRowContext(ctx, `
SELECT body, created_at_ns
FROM jobdb_chapter_chapters
WHERE anthology_id = ? AND story_id = ? AND ordinal = ?`,
		key.AnthologyID, key.StoryID, ordinal,
	).Scan(&body, &createdNS)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ChapterRecord{}, storage.ErrChapterNotFound
		}
		return storage.ChapterRecord{}, fmt.Errorf("sqlite chapterstore: read chapter: %w", err)
	}

	artifacts, err := s.readArtifacts(ctx, key, ordinal)
	if err != nil {
		return storage.ChapterRecord{}, err
	}
	return storage.ChapterRecord{
		Key:       key,
		Ordinal:   ordinal,
		Body:      append([]byte(nil), body...),
		Artifacts: artifacts,
		CreatedAt: nsToTime(createdNS),
	}, nil
}

// ListChapterOrdinals returns the next page of ordinals starting at startOrdinal.
func (s *Store) ListChapterOrdinals(ctx context.Context, key storage.StoryKey, startOrdinal int64, limit int) ([]int64, bool, error) {
	if err := s.Health(ctx); err != nil {
		return nil, false, err
	}
	if _, err := s.GetStory(ctx, key); err != nil {
		return nil, false, err
	}
	query := `
SELECT ordinal
FROM jobdb_chapter_chapters
WHERE anthology_id = ? AND story_id = ? AND ordinal >= ?
ORDER BY ordinal`
	args := []any{key.AnthologyID, key.StoryID, startOrdinal}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit+1)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("sqlite chapterstore: list chapter ordinals: %w", err)
	}
	defer rows.Close()

	var ordinals []int64
	for rows.Next() {
		var ordinal int64
		if err := rows.Scan(&ordinal); err != nil {
			return nil, false, fmt.Errorf("sqlite chapterstore: scan chapter ordinal: %w", err)
		}
		ordinals = append(ordinals, ordinal)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("sqlite chapterstore: list chapter ordinals: %w", err)
	}
	hasMore := limit > 0 && len(ordinals) > limit
	if hasMore {
		ordinals = ordinals[:limit]
	}
	return ordinals, hasMore, nil
}

// ListStories returns metadata rows for an anthology after the provided story ID.
func (s *Store) ListStories(ctx context.Context, anthologyID, startAfter string, limit int) ([]storage.StoryRecord, bool, error) {
	if err := s.Health(ctx); err != nil {
		return nil, false, err
	}
	query := `
SELECT ` + storyColumns + `
FROM jobdb_chapter_stories
WHERE anthology_id = ? AND deleted = 0`
	args := []any{anthologyID}
	if startAfter != "" {
		query += " AND story_id > ?"
		args = append(args, startAfter)
	}
	query += " ORDER BY story_id"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit+1)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("sqlite chapterstore: list stories: %w", err)
	}
	defer rows.Close()

	var records []storage.StoryRecord
	for rows.Next() {
		rec, err := scanStory(rows)
		if err != nil {
			return nil, false, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("sqlite chapterstore: list stories: %w", err)
	}
	hasMore := limit > 0 && len(records) > limit
	if hasMore {
		records = records[:limit]
	}
	return records, hasMore, nil
}

type storyScanner interface {
	Scan(dest ...any) error
}

func scanStory(scanner storyScanner) (storage.StoryRecord, error) {
	var rec storage.StoryRecord
	var createdNS, updatedNS int64
	var finalized, deleted int
	var deletedNS sql.NullInt64
	var baseAnthology, baseStory sql.NullString
	var baseFork sql.NullInt64
	err := scanner.Scan(
		&rec.Key.AnthologyID,
		&rec.Key.StoryID,
		&createdNS,
		&updatedNS,
		&finalized,
		&deleted,
		&deletedNS,
		&baseAnthology,
		&baseStory,
		&baseFork,
		&rec.ChapterCount,
		&rec.LatestOrdinal,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storage.StoryRecord{}, storage.ErrStoryNotFound
		}
		return storage.StoryRecord{}, fmt.Errorf("sqlite chapterstore: scan story: %w", err)
	}
	rec.CreatedAt = nsToTime(createdNS)
	rec.UpdatedAt = nsToTime(updatedNS)
	rec.Finalized = finalized != 0
	rec.Deleted = deleted != 0
	if deletedNS.Valid {
		rec.DeletedAt = nsToTime(deletedNS.Int64)
	}
	if baseAnthology.Valid || baseStory.Valid || baseFork.Valid {
		if !baseAnthology.Valid || !baseStory.Valid || !baseFork.Valid {
			return storage.StoryRecord{}, fmt.Errorf("%w: partial story link", storage.ErrInvalidRecord)
		}
		rec.Link = &storage.StoryLink{
			Key: storage.StoryKey{
				AnthologyID: baseAnthology.String,
				StoryID:     baseStory.String,
			},
			ForkOrdinal: baseFork.Int64,
		}
	}
	return rec, nil
}

func (s *Store) readArtifacts(ctx context.Context, key storage.StoryKey, ordinal int64) ([]storage.ArtifactRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, content_type, size_bytes, sha256, inline_data, blob_path
FROM jobdb_chapter_artifacts
WHERE anthology_id = ? AND story_id = ? AND ordinal = ?
ORDER BY position`,
		key.AnthologyID, key.StoryID, ordinal,
	)
	if err != nil {
		return nil, fmt.Errorf("sqlite chapterstore: read artifacts: %w", err)
	}
	defer rows.Close()
	return scanArtifacts(rows)
}

func scanArtifacts(rows *sql.Rows) ([]storage.ArtifactRecord, error) {
	var artifacts []storage.ArtifactRecord
	for rows.Next() {
		var art storage.ArtifactRecord
		var inlineData []byte
		var blobPath sql.NullString
		if err := rows.Scan(&art.ID, &art.Name, &art.ContentType, &art.SizeBytes, &art.Sha256, &inlineData, &blobPath); err != nil {
			return nil, fmt.Errorf("sqlite chapterstore: scan artifact: %w", err)
		}
		if inlineData != nil {
			art.InlineData = append([]byte(nil), inlineData...)
		}
		if blobPath.Valid {
			art.BlobPath = blobPath.String
		}
		artifacts = append(artifacts, art)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite chapterstore: scan artifacts: %w", err)
	}
	return artifacts, nil
}

func pathToDSN(path string) string {
	clean := filepath.Clean(path)
	if clean == ":memory:" {
		return "file::memory:?cache=shared"
	}
	if strings.HasPrefix(clean, "file:") {
		return clean
	}
	return "file:" + clean
}

func timeToNS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

func nsToTime(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func rollback(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

func isConstraint(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "constraint") || strings.Contains(msg, "unique")
}
