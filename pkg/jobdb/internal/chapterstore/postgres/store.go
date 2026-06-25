package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"gorm.io/gorm"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
)

const driverName = "pgx"

const storyColumns = "anthology_id, story_id, created_at_ns, updated_at_ns, finalized, deleted, deleted_at_ns, base_anthology_id, base_story_id, base_fork_ordinal, chapter_count, latest_ordinal"

// Store persists JobDB chapter rows using PostgreSQL.
type Store struct {
	db      *sql.DB
	closeFn func() error
}

// OpenDSN initializes a PostgreSQL rowstore from a DSN and owns the database handle.
func OpenDSN(ctx context.Context, dsn string) (*Store, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("postgres chapterstore: dsn is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres chapterstore: open: %w", err)
	}

	store := &Store{db: db, closeFn: db.Close}
	if err := store.Migrate(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// New wraps an existing GORM database handle. The caller retains ownership.
func New(db *gorm.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres chapterstore: db is required")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("postgres chapterstore: sql db: %w", err)
	}
	return NewSQLDB(sqlDB)
}

// NewSQLDB wraps an existing database handle. The caller retains ownership.
func NewSQLDB(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres chapterstore: db is required")
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
		return fmt.Errorf("postgres chapterstore: store not initialized")
	}
	return s.db.PingContext(ctx)
}

// Migrate installs the rowstore schema.
func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres chapterstore: store not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres chapterstore: begin migrate: %w", err)
	}
	defer rollback(tx)

	if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("postgres chapterstore: migrate: %w", err)
	}
	for _, col := range []struct {
		name       string
		definition string
	}{
		{name: "deleted", definition: "deleted boolean NOT NULL DEFAULT false"},
		{name: "deleted_at_ns", definition: "deleted_at_ns bigint"},
		{name: "base_anthology_id", definition: "base_anthology_id text"},
		{name: "base_story_id", definition: "base_story_id text"},
		{name: "base_fork_ordinal", definition: "base_fork_ordinal bigint"},
	} {
		if err := ensureStoryColumn(ctx, tx, col.name, col.definition); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("postgres chapterstore: commit migrate: %w", err)
	}
	return nil
}

func ensureStoryColumn(ctx context.Context, tx *sql.Tx, name, definition string) error {
	exists, err := storyColumnExists(ctx, tx, name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if _, err := tx.ExecContext(ctx, "ALTER TABLE jobdb_chapter_stories ADD COLUMN "+definition); err != nil {
		return fmt.Errorf("postgres chapterstore: add %s column: %w", name, err)
	}
	return nil
}

func storyColumnExists(ctx context.Context, tx *sql.Tx, name string) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM information_schema.columns
	WHERE table_schema = current_schema()
		AND table_name = 'jobdb_chapter_stories'
		AND column_name = $1
)`, name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("postgres chapterstore: inspect story columns: %w", err)
	}
	return exists, nil
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
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		key.AnthologyID, key.StoryID, timeToNS(rec.CreatedAt), timeToNS(rec.UpdatedAt), rec.Finalized, rec.Deleted, nil,
		nil, nil, nil, rec.ChapterCount, rec.LatestOrdinal,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return storage.StoryRecord{}, storage.ErrStoryExists
		}
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: create story: %w", err)
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
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: begin create linked story: %w", err)
	}
	defer rollback(tx)

	base, err := scanStory(tx.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = $1 AND story_id = $2
FOR SHARE`,
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
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		key.AnthologyID, key.StoryID, timeToNS(rec.CreatedAt), timeToNS(rec.UpdatedAt), rec.Finalized, rec.Deleted, nil,
		link.Key.AnthologyID, link.Key.StoryID, link.ForkOrdinal, rec.ChapterCount, rec.LatestOrdinal,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return storage.StoryRecord{}, storage.ErrStoryExists
		}
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: create linked story: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: commit create linked story: %w", err)
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
WHERE anthology_id = $1 AND story_id = $2 AND deleted = false`,
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
WHERE anthology_id = $1 AND story_id = $2`,
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
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: begin publish: %w", err)
	}
	defer rollback(tx)

	rec, err := scanStory(tx.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = $1 AND story_id = $2 AND deleted = false
FOR UPDATE`,
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
SET updated_at_ns = $1, finalized = true
WHERE anthology_id = $2 AND story_id = $3 AND deleted = false`,
		timeToNS(rec.UpdatedAt), key.AnthologyID, key.StoryID,
	); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: publish: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: commit publish: %w", err)
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
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: begin tombstone: %w", err)
	}
	defer rollback(tx)

	rec, err := scanStory(tx.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = $1 AND story_id = $2 AND deleted = false
FOR UPDATE`,
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

	if _, err := tx.ExecContext(ctx, `
UPDATE jobdb_chapter_stories
SET updated_at_ns = $1, deleted = true, deleted_at_ns = $2
WHERE anthology_id = $3 AND story_id = $4 AND deleted = false`,
		timeToNS(rec.UpdatedAt), timeToNS(rec.DeletedAt), key.AnthologyID, key.StoryID,
	); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: tombstone story: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: commit tombstone: %w", err)
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
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: begin append: %w", err)
	}
	defer rollback(tx)

	meta, err := scanStory(tx.QueryRowContext(ctx, `
SELECT `+storyColumns+`
FROM jobdb_chapter_stories
WHERE anthology_id = $1 AND story_id = $2 AND deleted = false
FOR UPDATE`,
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
WHERE anthology_id = $1 AND story_id = $2 AND ordinal = $3`,
		rec.Key.AnthologyID, rec.Key.StoryID, rec.Ordinal,
	).Scan(&exists)
	if err == nil {
		return storage.StoryRecord{}, storage.ErrChapterExists
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: check chapter: %w", err)
	}
	if err := storage.ValidateChapterAppend(meta, rec); err != nil {
		return storage.StoryRecord{}, err
	}

	rec.CreatedAt = now.UTC()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO jobdb_chapter_chapters (anthology_id, story_id, ordinal, body, created_at_ns)
VALUES ($1, $2, $3, $4, $5)`,
		rec.Key.AnthologyID, rec.Key.StoryID, rec.Ordinal, []byte(rec.Body), timeToNS(rec.CreatedAt),
	); err != nil {
		if isUniqueViolation(err) {
			return storage.StoryRecord{}, storage.ErrChapterExists
		}
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: insert chapter: %w", err)
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
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
			rec.Key.AnthologyID, rec.Key.StoryID, rec.Ordinal, i, art.ID, art.Name, art.ContentType, art.SizeBytes, art.Sha256, inlineData, blobPath,
		); err != nil {
			return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: insert artifact: %w", err)
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
SET updated_at_ns = $1, chapter_count = $2, latest_ordinal = $3
WHERE anthology_id = $4 AND story_id = $5 AND deleted = false`,
		timeToNS(meta.UpdatedAt), meta.ChapterCount, meta.LatestOrdinal, rec.Key.AnthologyID, rec.Key.StoryID,
	); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: update story after append: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: commit append: %w", err)
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
WHERE anthology_id = $1 AND story_id = $2 AND ordinal = $3`,
		key.AnthologyID, key.StoryID, ordinal,
	).Scan(&body, &createdNS)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ChapterRecord{}, storage.ErrChapterNotFound
		}
		return storage.ChapterRecord{}, fmt.Errorf("postgres chapterstore: read chapter: %w", err)
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
WHERE anthology_id = $1 AND story_id = $2 AND ordinal >= $3
ORDER BY ordinal`
	args := []any{key.AnthologyID, key.StoryID, startOrdinal}
	if limit > 0 {
		query += " LIMIT $4"
		args = append(args, limit+1)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("postgres chapterstore: list chapter ordinals: %w", err)
	}
	defer rows.Close()

	var ordinals []int64
	for rows.Next() {
		var ordinal int64
		if err := rows.Scan(&ordinal); err != nil {
			return nil, false, fmt.Errorf("postgres chapterstore: scan chapter ordinal: %w", err)
		}
		ordinals = append(ordinals, ordinal)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("postgres chapterstore: list chapter ordinals: %w", err)
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
WHERE anthology_id = $1 AND deleted = false`
	args := []any{anthologyID}
	if startAfter != "" {
		query += " AND story_id > $2"
		args = append(args, startAfter)
	}
	query += " ORDER BY story_id"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", len(args)+1)
		args = append(args, limit+1)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("postgres chapterstore: list stories: %w", err)
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
		return nil, false, fmt.Errorf("postgres chapterstore: list stories: %w", err)
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
	var deletedNS sql.NullInt64
	var baseAnthology, baseStory sql.NullString
	var baseFork sql.NullInt64
	err := scanner.Scan(
		&rec.Key.AnthologyID,
		&rec.Key.StoryID,
		&createdNS,
		&updatedNS,
		&rec.Finalized,
		&rec.Deleted,
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
		return storage.StoryRecord{}, fmt.Errorf("postgres chapterstore: scan story: %w", err)
	}
	rec.CreatedAt = nsToTime(createdNS)
	rec.UpdatedAt = nsToTime(updatedNS)
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
WHERE anthology_id = $1 AND story_id = $2 AND ordinal = $3
ORDER BY position`,
		key.AnthologyID, key.StoryID, ordinal,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres chapterstore: read artifacts: %w", err)
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
			return nil, fmt.Errorf("postgres chapterstore: scan artifact: %w", err)
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
		return nil, fmt.Errorf("postgres chapterstore: scan artifacts: %w", err)
	}
	return artifacts, nil
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

func rollback(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
