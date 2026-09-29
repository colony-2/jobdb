package runtimecore_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

type taskCompletionBackend struct {
	runtimecore.Scheduler
	runtimecore.ChapterLog
	events   []string
	failAt   string
	failure  error
	waiting  runtimecore.WaitingTaskSnapshot
	identity runtimecore.LeaseIdentity
	output   *runtimecore.TaskOutputRequest
}

func (b *taskCompletionBackend) GetWaitingTask(context.Context, jobdb.JobKey) (runtimecore.WaitingTaskSnapshot, error) {
	return b.waiting, nil
}
func (b *taskCompletionBackend) GetJob(context.Context, jobdb.JobKey) (runtimecore.StoredJob, error) {
	return runtimecore.StoredJob{Store: jobdb.JobStoreActive, WorkKind: runtimecore.WorkKindTask, TaskWork: &b.waiting.Task}, nil
}
func (b *taskCompletionBackend) Count(context.Context, runtimecore.ChapterLogKey) (int64, error) {
	return 1, nil
}
func (b *taskCompletionBackend) ClaimTask(_ context.Context, req runtimecore.ClaimTaskRequest) (runtimecore.LeaseIdentity, error) {
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
func (b *taskCompletionBackend) PublishTaskOutput(_ context.Context, req runtimecore.TaskOutputRequest) error {
	b.events = append(b.events, "publish")
	if req.Identity != b.identity {
		return errors.New("publication lost claim identity")
	}
	if b.failAt == "publish" {
		return b.failure
	}
	b.output = &req
	return nil
}
func (b *taskCompletionBackend) RescheduleLease(_ context.Context, req runtimecore.RescheduleMutation) (runtimecore.StoredJob, error) {
	b.events = append(b.events, "reschedule")
	if req.Identity != b.identity || req.RouteJobType != b.waiting.Task.ResumeJobType || req.WorkKind != runtimecore.WorkKindJob || req.TaskWork != nil || req.ClientPayloadUpdate != nil {
		return runtimecore.StoredJob{}, errors.New("invalid completion reschedule")
	}
	if b.failAt == "reschedule" {
		return runtimecore.StoredJob{}, b.failure
	}
	return runtimecore.StoredJob{}, nil
}

func TestTaskCompletionClaimsBeforePublishingAndRescheduling(t *testing.T) {
	for _, failAt := range []string{"", "claim", "publish", "reschedule"} {
		t.Run("fail_"+failAt, func(t *testing.T) {
			backend := &taskCompletionBackend{failAt: failAt, failure: errors.New("injected failure"), waiting: runtimecore.WaitingTaskSnapshot{JobType: "task-route", Task: runtimecore.TaskWork{TaskType: "external", ResumeJobType: "job-route", OutputOrdinal: 1, InputHash: "input"}}}
			r, err := runtimecore.NewRuntime(runtimecore.Config{Scheduler: backend, Chapters: backend, TaskCompletions: backend, Schemas: newMemorySchemaStore(), Now: func() time.Time { return time.Unix(1700000000, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			revision := int64(0)
			update := &jobdb.ClientPayloadUpdate{Mode: "reset", ExpectedRevision: &revision, Value: json.RawMessage(`{"done":true}`)}
			err = r.CompleteTaskIfWaiting(context.Background(), jobdb.CompleteTaskIfWaitingRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, Route: jobdb.Route{JobType: "task-route", TaskType: "external"}, OutputOrdinal: 1, InputHash: "input", Data: jobdb.NewTaskDataOrPanic(2), ClientPayloadUpdate: update})
			if failAt == "" && err != nil || failAt != "" && !errors.Is(err, backend.failure) {
				t.Fatalf("complete: %v", err)
			}
			want := []string{"claim", "publish", "reschedule"}
			if failAt == "claim" {
				want = want[:1]
			} else if failAt == "publish" {
				want = want[:2]
			}
			if !reflect.DeepEqual(backend.events, want) {
				t.Fatalf("events: %v", backend.events)
			}
			if failAt == "" || failAt == "reschedule" {
				if backend.output == nil || backend.output.ClientPayloadUpdate != update {
					t.Fatal("lost output/payload publication")
				}
				chapter, err := runtimecore.DecodeChapter(backend.output.Chapter)
				if err != nil || chapter.Ordinal != 1 || chapter.InputHash != "input" {
					t.Fatalf("output: %+v %v", chapter, err)
				}
			} else if backend.output != nil {
				t.Fatal("unexpected output")
			}
		})
	}
}

func TestTaskCompletionRequiresRecoveryBackend(t *testing.T) {
	r, err := runtimecore.NewRuntime(runtimecore.Config{Scheduler: readTestScheduler{}, Chapters: readTestChapters{}, Schemas: newMemorySchemaStore()})
	if err != nil {
		t.Fatal(err)
	}
	err = r.CompleteTaskIfWaiting(context.Background(), jobdb.CompleteTaskIfWaitingRequest{Route: jobdb.Route{JobType: "job", TaskType: "external"}})
	if err == nil {
		t.Fatal("accepted completion without recovery backend")
	}
}
