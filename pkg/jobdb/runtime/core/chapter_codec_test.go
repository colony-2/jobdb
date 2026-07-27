package runtimecore_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

func TestEncodeDecodeChapterRoundTrip(t *testing.T) {
	createdAt := time.Date(2026, 7, 27, 13, 0, 0, 0, time.UTC)
	encoded, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{
		Chapter: jobdb.Chapter{
			Ordinal:   2,
			TaskType:  "task-a",
			InputHash: "input-hash",
			CreatedAt: createdAt,
			Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
				Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
			}},
			Artifacts: []jobdb.StoredArtifact{{
				Name:   "result.txt",
				Digest: "sha256:result",
				Size:   11,
			}},
		},
		ArtifactUploads: []jobdb.ArtifactUpload{{
			Name: "result.txt",
			Size: 11,
			Open: func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte("hello world"))), nil
			},
		}},
	})
	if err != nil {
		t.Fatalf("encode chapter: %v", err)
	}
	if encoded.Ordinal != 2 || len(encoded.Payload) == 0 {
		t.Fatalf("unexpected encoded chapter %+v", encoded)
	}
	if len(encoded.ArtifactUploads) != 1 || len(encoded.Artifacts) != 1 {
		t.Fatalf("unexpected encoded artifacts %+v uploads=%+v", encoded.Artifacts, encoded.ArtifactUploads)
	}

	decoded, err := runtimecore.DecodeChapter(encoded)
	if err != nil {
		t.Fatalf("decode chapter: %v", err)
	}
	if decoded.Ordinal != 2 || decoded.TaskType != "task-a" || decoded.InputHash != "input-hash" || !decoded.CreatedAt.Equal(createdAt) {
		t.Fatalf("unexpected decoded chapter %+v", decoded)
	}
	body, ok := decoded.Body.(jobdb.TaskAttemptOutcomeChapter)
	if !ok {
		t.Fatalf("decoded body type = %T, want TaskAttemptOutcomeChapter", decoded.Body)
	}
	outcome, ok := body.Outcome.(jobdb.ApplicationOutputOutcome)
	if !ok {
		t.Fatalf("decoded outcome type = %T, want ApplicationOutputOutcome", body.Outcome)
	}
	if string(outcome.Output.Data) != `{"ok":true}` {
		t.Fatalf("decoded payload = %s", outcome.Output.Data)
	}
	if len(decoded.Artifacts) != 1 || decoded.Artifacts[0].Name != "result.txt" {
		t.Fatalf("decoded artifacts = %+v", decoded.Artifacts)
	}
}

func TestDecodeChapterRejectsInvalidPayload(t *testing.T) {
	if _, err := runtimecore.DecodeChapter(runtimecore.EncodedChapter{Payload: []byte("not a chapter")}); err == nil {
		t.Fatal("expected invalid payload error")
	}
}
