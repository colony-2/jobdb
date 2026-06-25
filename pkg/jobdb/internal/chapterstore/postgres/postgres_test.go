package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	postgresrowstore "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/postgres"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storagetest"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var testPostgresDSN string

func TestMain(m *testing.M) {
	dsn := strings.TrimSpace(os.Getenv("JOBDB_CHAPTER_POSTGRES_DSN"))
	if dsn != "" {
		testPostgresDSN = dsn
		os.Exit(m.Run())
	}

	tempDir, err := os.MkdirTemp("", "jobdb-chapter-embedded-postgres-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create embedded postgres temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tempDir)

	port, err := freeTCPPort()
	if err != nil {
		fmt.Fprintf(os.Stderr, "choose embedded postgres port: %v\n", err)
		os.Exit(1)
	}

	cfg := embeddedpostgres.DefaultConfig().
		Port(uint32(port)).
		Database("postgres").
		Username("postgres").
		Password("postgres").
		DataPath(filepath.Join(tempDir, "data")).
		RuntimePath(filepath.Join(tempDir, "runtime")).
		Logger(io.Discard)
	embedded := embeddedpostgres.NewDatabase(cfg)
	if err := embedded.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start embedded postgres: %v\n", err)
		os.Exit(1)
	}
	testPostgresDSN = cfg.GetConnectionURL()

	code := m.Run()
	if err := embedded.Stop(); err != nil && code == 0 {
		fmt.Fprintf(os.Stderr, "stop embedded postgres: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

func TestPostgresContract(t *testing.T) {
	storagetest.RunRowStoreSuite(t, func(t testing.TB) storagetest.RowStoreFixture {
		t.Helper()
		store, cleanup := newPostgresStore(t, testPostgresDSN)
		return storagetest.RowStoreFixture{
			Store: store,
			Cleanup: func() {
				_ = store.Close()
				cleanup()
			},
		}
	})
}

func TestMigrateAddsSoftDeleteAndLinkColumns(t *testing.T) {
	schemaDSN, cleanup := newTestSchema(t, testPostgresDSN)
	defer cleanup()

	ctx := context.Background()
	db, err := sql.Open("pgx", schemaDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	_, err = db.ExecContext(ctx, `
CREATE TABLE jobdb_chapter_stories (
	anthology_id text NOT NULL,
	story_id text NOT NULL,
	created_at_ns bigint NOT NULL,
	updated_at_ns bigint NOT NULL,
	finalized boolean NOT NULL DEFAULT false,
	chapter_count bigint NOT NULL DEFAULT 0 CHECK (chapter_count >= 0),
	latest_ordinal bigint NOT NULL DEFAULT -1 CHECK (latest_ordinal >= -1),
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
) VALUES ($1, $2, $3, $4, false, 0, -1)`,
		key.AnthologyID, key.StoryID, created, created,
	)
	if err != nil {
		t.Fatalf("insert legacy story: %v", err)
	}

	store, err := postgresrowstore.NewSQLDB(db)
	if err != nil {
		t.Fatalf("postgres.NewSQLDB: %v", err)
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

func TestNewGORMBorrowsConnection(t *testing.T) {
	schemaDSN, cleanup := newTestSchema(t, testPostgresDSN)
	defer cleanup()

	gormDB, err := gorm.Open(gormpostgres.Open(schemaDSN), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	sqlDB, err := gormDB.DB()
	if err != nil {
		t.Fatalf("gorm DB: %v", err)
	}
	defer sqlDB.Close()

	store, err := postgresrowstore.New(gormDB)
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("borrowed store Close: %v", err)
	}
	if err := sqlDB.PingContext(context.Background()); err != nil {
		t.Fatalf("caller-owned gorm connection was closed: %v", err)
	}
}

func newPostgresStore(t testing.TB, baseDSN string) (*postgresrowstore.Store, func()) {
	t.Helper()
	schemaDSN, cleanup := newTestSchema(t, baseDSN)
	store, err := postgresrowstore.OpenDSN(context.Background(), schemaDSN)
	if err != nil {
		cleanup()
		t.Fatalf("OpenDSN: %v", err)
	}
	return store, cleanup
}

func freeTCPPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func newTestSchema(t testing.TB, baseDSN string) (string, func()) {
	t.Helper()
	db, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatalf("ping postgres: %v", err)
	}

	schema := testSchemaName(t)
	if _, err := db.ExecContext(context.Background(), "CREATE SCHEMA "+quoteIdent(schema)); err != nil {
		_ = db.Close()
		t.Fatalf("create test schema: %v", err)
	}

	cleanup := func() {
		_, _ = db.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+quoteIdent(schema)+" CASCADE")
		_ = db.Close()
	}
	return withSearchPath(baseDSN, schema), cleanup
}

func testSchemaName(t testing.TB) string {
	t.Helper()
	return fmt.Sprintf("jobdb_chapter_test_%d", time.Now().UnixNano())
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func withSearchPath(baseDSN, schema string) string {
	u, err := url.Parse(baseDSN)
	if err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		return u.String()
	}
	if strings.TrimSpace(baseDSN) == "" {
		return baseDSN
	}
	return baseDSN + " search_path=" + schema
}
