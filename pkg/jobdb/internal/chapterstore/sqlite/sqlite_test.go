package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storagetest"
)

func TestSQLiteContract(t *testing.T) {
	storagetest.RunRowStoreSuite(t, func(t testing.TB) storagetest.RowStoreFixture {
		t.Helper()
		store, err := sqlite.Open(filepath.Join(t.TempDir(), "rows.db"))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		return storagetest.RowStoreFixture{
			Store:   store,
			Cleanup: func() { _ = store.Close() },
		}
	})
}

func TestMigrateAddsSoftDeleteColumns(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "rows.db"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, `
CREATE TABLE jobdb_chapter_stories (
	anthology_id TEXT NOT NULL,
	story_id TEXT NOT NULL,
	created_at_ns INTEGER NOT NULL,
	updated_at_ns INTEGER NOT NULL,
	finalized INTEGER NOT NULL DEFAULT 0 CHECK (finalized IN (0, 1)),
	chapter_count INTEGER NOT NULL DEFAULT 0 CHECK (chapter_count >= 0),
	latest_ordinal INTEGER NOT NULL DEFAULT -1 CHECK (latest_ordinal >= -1),
	PRIMARY KEY (anthology_id, story_id)
)`)
	if err != nil {
		t.Fatalf("create legacy stories table: %v", err)
	}

	created := time.Unix(1_700_000_000, 0).UTC().UnixNano()
	key := storage.StoryKey{AnthologyID: "legacy", StoryID: "story"}
	_, err = db.ExecContext(ctx, `
INSERT INTO jobdb_chapter_stories (
	anthology_id, story_id, created_at_ns, updated_at_ns, finalized, chapter_count, latest_ordinal
) VALUES (?, ?, ?, ?, 0, 0, -1)`,
		key.AnthologyID, key.StoryID, created, created,
	)
	if err != nil {
		t.Fatalf("insert legacy story: %v", err)
	}

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	if _, err := store.GetStory(ctx, key); err != nil {
		t.Fatalf("GetStory after migration: %v", err)
	}
	if err := storage.MarkStoryDeleted(ctx, store, key, time.Unix(1_700_000_001, 0)); err != nil {
		t.Fatalf("MarkStoryDeleted after migration: %v", err)
	}
	if _, err := store.GetStory(ctx, key); !errors.Is(err, storage.ErrStoryNotFound) {
		t.Fatalf("GetStory after delete error=%v, want ErrStoryNotFound", err)
	}
}
