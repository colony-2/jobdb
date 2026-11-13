package sqlite

import (
	"context"
	"io"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func assertStartArtifact(t *testing.T, ctx context.Context, runtime *Runtime, jobKey jobdb.JobKey, name string, want []byte) {
	t.Helper()
	chapter, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: jobKey, Ordinal: 0})
	if err != nil {
		t.Fatalf("get start chapter for %s: %v", jobKey, err)
	}
	if len(chapter.Artifacts) != 1 {
		t.Fatalf("start artifacts for %s = %+v, want one", jobKey, chapter.Artifacts)
	}
	stored := chapter.Artifacts[0]
	if stored.Name != name {
		t.Fatalf("artifact name = %q, want %q", stored.Name, name)
	}
	if stored.Size != int64(len(want)) {
		t.Fatalf("artifact size = %d, want %d", stored.Size, len(want))
	}
	reader, err := runtime.OpenArtifact(ctx, jobdb.ArtifactRef{
		JobKey:  jobKey,
		Ordinal: 0,
		Name:    stored.Name,
		Digest:  stored.Digest,
	})
	if err != nil {
		t.Fatalf("open start artifact for %s: %v", jobKey, err)
	}
	rc, err := reader.Open()
	if err != nil {
		t.Fatalf("open artifact reader for %s: %v", jobKey, err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read start artifact for %s: %v", jobKey, err)
	}
	if string(got) != string(want) {
		t.Fatalf("artifact data = %q, want %q", string(got), string(want))
	}
}
