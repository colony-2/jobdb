package postgres_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

func startPostgres(test *testing.T) (dsn, dir string) {
	test.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatalf("choose Postgres port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	dir = test.TempDir()
	cfg := embeddedpostgres.DefaultConfig().
		Port(uint32(port)).
		Database("postgres").
		Username("postgres").
		Password("postgres").
		DataPath(filepath.Join(dir, "data")).
		RuntimePath(filepath.Join(dir, "runtime")).
		Logger(io.Discard)
	postgres := embeddedpostgres.NewDatabase(cfg)
	if err := postgres.Start(); err != nil {
		test.Fatalf("start Postgres: %v", err)
	}
	test.Cleanup(func() {
		if err := postgres.Stop(); err != nil {
			test.Errorf("stop Postgres: %v", err)
		}
	})
	return cfg.GetConnectionURL(), dir
}

func TestChapterLogRoundTrip(t *testing.T) {
	ctx := context.Background()
	dsn, dir := startPostgres(t)
	store, err := chapterpostgres.OpenDSN(ctx, dsn, chapterpostgres.Config{Projects: celltest.UnrestrictedProjects{},
		BlobStoreURI: "blobfs://" + filepath.ToSlash(filepath.Join(dir, "blobs")),
	})
	if err != nil {
		t.Fatalf("open chapter log: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(ctx); err != nil {
			t.Errorf("close chapter log: %v", err)
		}
	})

	key := runtimecore.ChapterLogKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}}
	body := bytes.Repeat([]byte("stored artifact"), 30)
	initial := runtimecore.EncodedChapter{
		Ordinal: 0,
		Payload: []byte(`{"kind":"start"}`),
		ArtifactUploads: []jobdb.ArtifactUpload{{
			Name: "input.txt", Size: int64(len(body)),
			Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil },
		}},
	}
	if err := store.Create(ctx, key, initial); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.Create(ctx, key, initial); !errors.Is(err, jobdb.ErrConflict) {
		t.Fatalf("duplicate create = %v, want conflict", err)
	}
	got, err := store.Get(ctx, key, 0)
	if err != nil {
		t.Fatalf("get initial: %v", err)
	}
	if !bytes.Equal(got.Payload, initial.Payload) || len(got.Artifacts) != 1 {
		t.Fatalf("initial chapter = %+v", got)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(body))
	if got.Artifacts[0].Digest != digest {
		t.Fatalf("artifact digest = %s, want %s", got.Artifacts[0].Digest, digest)
	}
	reader, err := store.OpenArtifact(ctx, runtimecore.ArtifactLookup{
		JobKey: key.JobKey, Ordinal: 0, Name: "input.txt", Digest: digest,
	})
	if err != nil {
		t.Fatalf("open artifact: %v", err)
	}
	stream, err := reader.Open()
	if err != nil {
		t.Fatalf("artifact reader: %v", err)
	}
	readBody, err := io.ReadAll(stream)
	_ = stream.Close()
	if err != nil || !bytes.Equal(readBody, body) {
		t.Fatalf("artifact body = %q, err = %v", readBody, err)
	}

	second := runtimecore.EncodedChapter{Ordinal: 1, Payload: []byte(`{"kind":"outcome"}`)}
	if err := store.Append(ctx, key, second); err != nil {
		t.Fatalf("append: %v", err)
	}
	count, err := store.Count(ctx, key)
	if err != nil || count != 2 {
		t.Fatalf("count = %d, err = %v", count, err)
	}
	end := int64(1)
	listed, err := store.List(ctx, key, runtimecore.ChapterRange{StartOrdinal: 1, EndOrdinal: &end})
	if err != nil || len(listed) != 1 || !bytes.Equal(listed[0].Payload, second.Payload) {
		t.Fatalf("list = %+v, err = %v", listed, err)
	}

	cloneKey := runtimecore.ChapterLogKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "clone"}}
	if err := store.ClonePrefix(ctx, runtimecore.ClonePrefixRequest{
		SourceKey: key, DestinationKey: cloneKey, LastOrdinal: 0, Append: &second,
	}); err != nil {
		t.Fatalf("clone prefix: %v", err)
	}
	cloned, err := store.Get(ctx, cloneKey, 0)
	if err != nil || !bytes.Equal(cloned.Payload, initial.Payload) || len(cloned.Artifacts) != 1 || cloned.Artifacts[0].Digest != digest {
		t.Fatalf("cloned initial = %+v, err = %v", cloned, err)
	}
	clonedReader, err := store.OpenArtifact(ctx, runtimecore.ArtifactLookup{
		JobKey: cloneKey.JobKey, Ordinal: 0, Name: "input.txt", Digest: digest,
	})
	if err != nil {
		t.Fatalf("open cloned artifact: %v", err)
	}
	clonedStream, err := clonedReader.Open()
	if err != nil {
		t.Fatalf("cloned artifact reader: %v", err)
	}
	clonedBody, err := io.ReadAll(clonedStream)
	_ = clonedStream.Close()
	if err != nil || !bytes.Equal(clonedBody, body) {
		t.Fatalf("cloned artifact body = %q, err = %v", clonedBody, err)
	}
	// The optional diagnostic source resolves the linked prefix without touching
	// artifact objects and bounds oversized bodies before materialization.
	ceiling, err := store.ExecutionProvenanceCeiling(ctx, cloneKey)
	if err != nil || ceiling == nil || *ceiling != 1 {
		t.Fatalf("provenance ceiling=%v, err=%v", ceiling, err)
	}
	page, err := store.ReadProvenanceChapterPage(ctx, runtimecore.ProvenanceChapterPageRequest{Key: cloneKey, ThroughOrdinal: *ceiling, StartOrdinal: 0, Records: 1, Bytes: 1 << 20})
	if err != nil || len(page.Entries) != 1 || !bytes.Equal(page.Entries[0].Body, initial.Payload) || page.NextOrdinal != 1 {
		t.Fatalf("inherited provenance page=%+v, err=%v", page, err)
	}
	oversized := runtimecore.EncodedChapter{Ordinal: 2, Payload: bytes.Repeat([]byte("x"), (1<<20)+1)}
	if err := store.Append(ctx, cloneKey, oversized); err != nil {
		t.Fatal(err)
	}
	page, err = store.ReadProvenanceChapterPage(ctx, runtimecore.ProvenanceChapterPageRequest{Key: cloneKey, ThroughOrdinal: 2, StartOrdinal: 1, Records: 256, Bytes: 1 << 20})
	if err != nil || len(page.Entries) != 2 || page.Entries[1].Issue != jobdb.ExecutionProvenanceOversizedRecord || len(page.Entries[1].Body) != 0 {
		t.Fatalf("bounded provenance page=%+v, err=%v", page, err)
	}

	if _, err := store.Get(ctx, key, 99); !errors.Is(err, jobdb.ErrChapterNotFound) {
		t.Fatalf("missing chapter = %v, want chapter not found", err)
	}
}

func TestChapterLogInitialCreateWaitsForCommittedHistory(test *testing.T) {
	dsn, dir := startPostgres(test)
	leader, leaderDB := openTestChapterLog(test, dsn, dir)
	peer, peerDB := openTestChapterLog(test, dsn, dir)
	controlDB := openTestDB(test, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	control, err := controlDB.Conn(ctx)
	if err != nil {
		test.Fatal(err)
	}
	defer control.Close()
	leaderPID, peerPID, controlPID := postgresPID(test, ctx, leaderDB), postgresPID(test, ctx, peerDB), postgresPID(test, ctx, control)
	if leaderPID == peerPID || leaderPID == controlPID || peerPID == controlPID {
		test.Fatal("expected three separate PostgreSQL sessions")
	}

	key, initial, expectedArtifact := initialCreateFixture()
	const barrier int64 = 7831061
	execSQL(test, ctx, control, fmt.Sprintf(`
CREATE FUNCTION hold_initial_chapter() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(%d::bigint);
    RETURN NEW;
END;
$$;
CREATE TRIGGER hold_initial_chapter BEFORE INSERT ON jobdb_chapter_chapters
FOR EACH ROW WHEN (NEW.anthology_id = '%s' AND NEW.story_id = '%s' AND NEW.ordinal = 0)
EXECUTE FUNCTION hold_initial_chapter();`, barrier, key.JobKey.TenantId, key.JobKey.JobId))
	execSQL(test, ctx, control, `SELECT pg_advisory_lock($1::bigint)`, barrier)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = control.ExecContext(cleanup, `SELECT pg_advisory_unlock_all()`)
	}()

	leaderDone := make(chan error, 1)
	go func() { leaderDone <- leader.Create(ctx, key, initial) }()
	awaitPostgresBlock(test, ctx, control, leaderPID, controlPID)

	type outcome struct {
		createErr error
		chapter   runtimecore.EncodedChapter
		readErr   error
	}
	peerDone := make(chan outcome, 1)
	go func() {
		result := outcome{createErr: peer.Create(ctx, key, initial)}
		if errors.Is(result.createErr, jobdb.ErrConflict) {
			result.chapter, result.readErr = peer.Get(ctx, key, 0)
		}
		peerDone <- result
	}()
	awaitPostgresBlock(test, ctx, control, peerPID, leaderPID)
	var unlocked bool
	if err := control.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1::bigint)`, barrier).Scan(&unlocked); err != nil || !unlocked {
		test.Fatalf("release chapter insert barrier: unlocked=%t err=%v", unlocked, err)
	}
	select {
	case result := <-peerDone:
		if !errors.Is(result.createErr, jobdb.ErrConflict) {
			test.Fatalf("peer Create = %v, want ErrConflict", result.createErr)
		}
		checkInitialChapter(test, result.chapter, result.readErr, initial, expectedArtifact)
		checkInitialContentType(test, ctx, peerDB, key)
	case <-ctx.Done():
		test.Fatalf("peer did not finish: %v", ctx.Err())
	}
	select {
	case err := <-leaderDone:
		if err != nil {
			test.Fatalf("leader Create: %v", err)
		}
	case <-ctx.Done():
		test.Fatalf("leader did not finish: %v", ctx.Err())
	}
}

func TestChapterLogInitialCreateRollsBackHistory(test *testing.T) {
	dsn, dir := startPostgres(test)
	store, _ := openTestChapterLog(test, dsn, dir)
	observer := openTestDB(test, dsn)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key, initial, expectedArtifact := initialCreateFixture()
	execSQL(test, ctx, observer, fmt.Sprintf(`
CREATE FUNCTION fail_initial_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.chapter_count <> 1 OR NEW.latest_ordinal <> 0 OR NOT EXISTS (
        SELECT 1 FROM jobdb_chapter_chapters
        WHERE anthology_id = NEW.anthology_id AND story_id = NEW.story_id AND ordinal = 0
    ) OR NOT EXISTS (
        SELECT 1 FROM jobdb_chapter_artifacts
        WHERE anthology_id = NEW.anthology_id AND story_id = NEW.story_id AND ordinal = 0
            AND name = 'input.txt' AND sha256 = '%s' AND size_bytes = %d
            AND content_type = 'application/octet-stream'
    ) THEN
        RAISE EXCEPTION 'incomplete initial history at rollback injection';
    END IF;
    RAISE EXCEPTION USING ERRCODE = 'JDB01', MESSAGE = 'test failure after initial history writes';
END;
$$;
CREATE TRIGGER fail_initial_history AFTER UPDATE ON jobdb_chapter_stories
FOR EACH ROW WHEN (NEW.anthology_id = '%s' AND NEW.story_id = '%s')
EXECUTE FUNCTION fail_initial_history();`, expectedArtifact.Digest, expectedArtifact.Size, key.JobKey.TenantId, key.JobKey.JobId))
	err := store.Create(ctx, key, initial)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "JDB01" {
		test.Fatalf("Create = %v, want injected failure after all writes", err)
	}
	for _, table := range []string{"jobdb_chapter_stories", "jobdb_chapter_chapters", "jobdb_chapter_artifacts"} {
		var count int
		if err := observer.QueryRowContext(ctx, `SELECT count(*) FROM `+table+`
WHERE anthology_id = $1 AND story_id = $2`, key.JobKey.TenantId, key.JobKey.JobId).Scan(&count); err != nil || count != 0 {
			test.Fatalf("after rollback, %s has %d rows for key: %v", table, count, err)
		}
	}
	execSQL(test, ctx, observer, `DROP TRIGGER fail_initial_history ON jobdb_chapter_stories`)
	if err := store.Create(ctx, key, initial); err != nil {
		test.Fatalf("Create with rolled-back key: %v", err)
	}
	got, err := store.Get(ctx, key, 0)
	checkInitialChapter(test, got, err, initial, expectedArtifact)
	checkInitialContentType(test, ctx, observer, key)
}

func TestChapterLogInitialCreateLeavesLegacyEmptyStory(test *testing.T) {
	dsn, dir := startPostgres(test)
	store, db := openTestChapterLog(test, dsn, dir)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key, initial, _ := initialCreateFixture()
	execSQL(test, ctx, db, `INSERT INTO jobdb_chapter_stories (anthology_id, story_id, created_at_ns, updated_at_ns)
VALUES ($1, $2, $3, $3)`, key.JobKey.TenantId, key.JobKey.JobId, time.Now().UnixNano())
	if err := store.Create(ctx, key, initial); !errors.Is(err, jobdb.ErrConflict) {
		test.Fatalf("Create on a legacy empty story = %v, want ErrConflict", err)
	}
	if _, err := store.Get(ctx, key, 0); !errors.Is(err, jobdb.ErrChapterNotFound) {
		test.Fatalf("Get on a legacy empty story = %v, want ErrChapterNotFound", err)
	}
}

func initialCreateFixture() (runtimecore.ChapterLogKey, runtimecore.EncodedChapter, jobdb.StoredArtifact) {
	key := runtimecore.ChapterLogKey{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: uuid.NewString()}}
	body := bytes.Repeat([]byte("initial artifact"), 32)
	meta := jobdb.StoredArtifact{Name: "input.txt", Size: int64(len(body)), Digest: fmt.Sprintf("%x", sha256.Sum256(body))}
	initial := runtimecore.EncodedChapter{
		Ordinal: 0,
		Payload: []byte(`{"kind":"start"}`),
		ArtifactUploads: []jobdb.ArtifactUpload{{
			Name: meta.Name, Size: meta.Size,
			Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil },
		}},
	}
	return key, initial, meta
}

func checkInitialChapter(test *testing.T, got runtimecore.EncodedChapter, err error, want runtimecore.EncodedChapter, artifact jobdb.StoredArtifact) {
	test.Helper()
	if err != nil {
		test.Fatalf("read initial chapter: %v", err)
	}
	if got.Ordinal != 0 || !bytes.Equal(got.Payload, want.Payload) || len(got.Artifacts) != 1 || got.Artifacts[0] != artifact {
		test.Fatalf("initial chapter = %+v, want ordinal zero, payload %s and artifact %+v", got, want.Payload, artifact)
	}
}

func checkInitialContentType(test *testing.T, ctx context.Context, db *sql.DB, key runtimecore.ChapterLogKey) {
	test.Helper()
	var contentType string
	err := db.QueryRowContext(ctx, `SELECT content_type FROM jobdb_chapter_artifacts
WHERE anthology_id = $1 AND story_id = $2 AND ordinal = 0 AND position = 0`, key.JobKey.TenantId, key.JobKey.JobId).Scan(&contentType)
	if err != nil || contentType != "application/octet-stream" {
		test.Fatalf("initial artifact content type = %q: %v", contentType, err)
	}
}

func openTestDB(test *testing.T, dsn string) *sql.DB {
	test.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		test.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	test.Cleanup(func() { _ = db.Close() })
	return db
}

func openTestChapterLog(test *testing.T, dsn, dir string) (*chapterpostgres.Store, *sql.DB) {
	test.Helper()
	db := openTestDB(test, dsn)
	store, err := chapterpostgres.NewSQLDB(context.Background(), db, chapterpostgres.Config{
		Projects:     celltest.UnrestrictedProjects{},
		BlobStoreURI: "blobfs://" + filepath.ToSlash(filepath.Join(dir, "blobs")),
	})
	if err != nil {
		test.Fatalf("open chapter log: %v", err)
	}
	test.Cleanup(func() { _ = store.Close(context.Background()) })
	return store, db
}

func postgresPID(test *testing.T, ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) int {
	test.Helper()
	var pid int
	if err := db.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		test.Fatal(err)
	}
	return pid
}

func execSQL(test *testing.T, ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, query string, args ...any) {
	test.Helper()
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		test.Fatal(err)
	}
}

func awaitPostgresBlock(test *testing.T, ctx context.Context, observer *sql.Conn, waiter, blocker int) {
	test.Helper()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		var blocked bool
		if err := observer.QueryRowContext(ctx, `SELECT $2::integer = ANY(pg_blocking_pids($1))`, waiter, blocker).Scan(&blocked); err != nil {
			test.Fatalf("observe PostgreSQL session %d blocked by %d: %v", waiter, blocker, err)
		}
		if blocked {
			return
		}
		select {
		case <-poll.C:
		case <-ctx.Done():
			test.Fatalf("PostgreSQL session %d never blocked on %d: %v", waiter, blocker, ctx.Err())
		}
	}
}
