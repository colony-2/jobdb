package runtimecore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

type readTestScheduler struct{ runtimecore.Scheduler }
type readTestSchemas struct{ runtimecore.SchemaStore }

type readTestChapters struct {
	runtimecore.ChapterLog
	encoded runtimecore.EncodedChapter
}

func (s readTestChapters) Get(_ context.Context, _ runtimecore.ChapterLogKey, ordinal int64) (runtimecore.EncodedChapter, error) {
	if ordinal != s.encoded.Ordinal {
		return runtimecore.EncodedChapter{}, jobdb.ErrChapterNotFound
	}
	return s.encoded, nil
}

func (s readTestChapters) List(_ context.Context, _ runtimecore.ChapterLogKey, rng runtimecore.ChapterRange) ([]runtimecore.EncodedChapter, error) {
	if s.encoded.Ordinal < rng.StartOrdinal || (rng.EndOrdinal != nil && s.encoded.Ordinal > *rng.EndOrdinal) {
		return nil, nil
	}
	return []runtimecore.EncodedChapter{s.encoded}, nil
}

func TestRuntimeReadsChapterThroughPublicPort(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	want := jobdb.Chapter{
		Ordinal: 0, TaskType: "collect", CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
		Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`{"item":1}`)}},
	}
	encoded, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: want})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := runtimecore.NewRuntime(runtimecore.Config{
		Scheduler: readTestScheduler{}, Chapters: readTestChapters{encoded: encoded},
		Schemas: readTestSchemas{},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := runtime.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: key, Ordinal: 0})
	if err != nil || got.TaskType != want.TaskType || got.Ordinal != want.Ordinal {
		t.Fatalf("get chapter = %+v, %v", got, err)
	}
	start, ok := got.Body.(jobdb.JobStartChapter)
	if !ok || string(start.Input.Data) != `{"item":1}` {
		t.Fatalf("decoded chapter body = %#v", got.Body)
	}
	chapters, err := runtime.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
	if err != nil || len(chapters) != 1 || chapters[0].TaskType != "collect" {
		t.Fatalf("list chapters = %+v, %v", chapters, err)
	}
	if _, err := runtime.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: key, Ordinal: 1}); !errors.Is(err, jobdb.ErrChapterNotFound) {
		t.Fatalf("missing chapter = %v", err)
	}
}
