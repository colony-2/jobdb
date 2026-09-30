package runtimecore_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

type taskCompletionScheduler struct {
	runtimecore.Scheduler
	events   []string
	failAt   string
	failure  error
	waiting  runtimecore.WaitingTaskSnapshot
	identity runtimecore.LeaseIdentity
}

func (b *taskCompletionScheduler) GetWaitingTask(context.Context, jobdb.JobKey) (runtimecore.WaitingTaskSnapshot, error) {
	return b.waiting, nil
}
func (b *taskCompletionScheduler) GetJob(context.Context, jobdb.JobKey) (runtimecore.StoredJob, error) {
	return runtimecore.StoredJob{Store: jobdb.JobStoreActive, WorkKind: runtimecore.WorkKindTask, TaskWork: &b.waiting.Task}, nil
}
func (b *taskCompletionScheduler) ClaimTask(_ context.Context, req runtimecore.ClaimTaskRequest) (runtimecore.LeaseIdentity, error) {
	b.events = append(b.events, "claim")
	if req.Task != b.waiting || req.LeaseDuration <= 0 {
		return runtimecore.LeaseIdentity{}, errors.New("invalid claim")
	}
	if b.failAt == "claim" {
		return runtimecore.LeaseIdentity{}, b.failure
	}
	b.identity = runtimecore.LeaseIdentity{JobKey: req.JobKey, LeaseID: "claim", WorkerID: req.WorkerID, ExpiresAt: req.Now.Add(req.LeaseDuration)}
	return b.identity, nil
}
func (b *taskCompletionScheduler) ValidateLease(_ context.Context, identity runtimecore.LeaseIdentity) (runtimecore.LeaseSnapshot, error) {
	b.events = append(b.events, "validate")
	if identity != b.identity {
		return runtimecore.LeaseSnapshot{}, errors.New("lost claim identity")
	}
	if b.failAt == "validate" {
		return runtimecore.LeaseSnapshot{}, b.failure
	}
	return runtimecore.LeaseSnapshot{Identity: identity}, nil
}
func (b *taskCompletionScheduler) RescheduleLease(_ context.Context, req runtimecore.RescheduleMutation) (runtimecore.StoredJob, error) {
	b.events = append(b.events, "reschedule")
	if req.Identity != b.identity || req.RouteJobType != b.waiting.Task.ResumeJobType || req.WorkKind != runtimecore.WorkKindJob || req.TaskWork != nil || req.ClientPayloadUpdate != nil {
		return runtimecore.StoredJob{}, errors.New("invalid completion reschedule")
	}
	if b.failAt == "reschedule" {
		return runtimecore.StoredJob{}, b.failure
	}
	return runtimecore.StoredJob{}, nil
}

// This chapter adapter has only the ordinary ChapterLog operations. It neither
// receives a scheduler transaction nor implements a completion-specific port.
type taskCompletionChapters struct {
	runtimecore.ChapterLog
	scheduler *taskCompletionScheduler // test event recorder only
	output    *runtimecore.EncodedChapter
}

func (c *taskCompletionChapters) Count(context.Context, runtimecore.ChapterLogKey) (int64, error) {
	return 1, nil
}
func (c *taskCompletionChapters) Append(_ context.Context, key runtimecore.ChapterLogKey, chapter runtimecore.EncodedChapter) error {
	c.scheduler.events = append(c.scheduler.events, "append")
	if c.scheduler.failAt == "append" {
		return c.scheduler.failure
	}
	c.output = &chapter
	return nil
}

func TestTaskCompletionUsesIndependentSchedulerAndChapterOperations(t *testing.T) {
	for _, failAt := range []string{"", "claim", "validate", "append", "reschedule"} {
		t.Run("fail_"+failAt, func(t *testing.T) {
			scheduler := &taskCompletionScheduler{failAt: failAt, failure: errors.New("injected failure"), waiting: runtimecore.WaitingTaskSnapshot{JobType: "task-route", Task: runtimecore.TaskWork{TaskType: "external", ResumeJobType: "job-route", OutputOrdinal: 1, InputHash: "input"}}}
			chapters := &taskCompletionChapters{scheduler: scheduler}
			r, err := runtimecore.NewRuntime(runtimecore.Config{Scheduler: scheduler, Chapters: chapters, Schemas: newMemorySchemaStore(), Now: func() time.Time { return time.Unix(1700000000, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			err = r.CompleteTaskIfWaiting(context.Background(), jobdb.CompleteTaskIfWaitingRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, Route: jobdb.Route{JobType: "task-route", TaskType: "external"}, OutputOrdinal: 1, InputHash: "input", Data: jobdb.NewTaskDataOrPanic(2)})
			if failAt == "" && err != nil || failAt != "" && !errors.Is(err, scheduler.failure) {
				t.Fatalf("complete: %v", err)
			}
			want := []string{"claim", "validate", "append", "reschedule"}
			for i, event := range want {
				if event == failAt {
					want = want[:i+1]
					break
				}
			}
			if !reflect.DeepEqual(scheduler.events, want) {
				t.Fatalf("events: %v", scheduler.events)
			}
			if failAt == "" || failAt == "reschedule" {
				if chapters.output == nil {
					t.Fatal("lost independent chapter commit")
				}
				chapter, err := runtimecore.DecodeChapter(*chapters.output)
				if err != nil || chapter.Ordinal != 1 || chapter.InputHash != "input" {
					t.Fatalf("output: %+v %v", chapter, err)
				}
			} else if chapters.output != nil {
				t.Fatal("unexpected output")
			}
		})
	}
}
