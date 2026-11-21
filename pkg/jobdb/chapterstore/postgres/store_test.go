package postgres_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

func TestChapterLogRoundTrip(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("choose Postgres port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	dir := t.TempDir()
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
		t.Fatalf("start Postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := postgres.Stop(); err != nil {
			t.Errorf("stop Postgres: %v", err)
		}
	})

	store, err := chapterpostgres.OpenDSN(ctx, cfg.GetConnectionURL(), chapterpostgres.Config{Projects: celltest.UnrestrictedProjects{},
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
