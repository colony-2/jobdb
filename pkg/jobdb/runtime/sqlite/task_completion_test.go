package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
)

func waitingTaskFixture(t *testing.T) (*Runtime, Config, jobdb.CompleteTaskIfWaitingRequest) {
	t.Helper()
	ctx := context.Background()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobdb.db")}
	r, err := NewFromConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close(ctx) })
	h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "tenant", JobID: "job", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1),
	}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
	if err != nil || lease == nil {
		t.Fatalf("lease: %v %v", lease, err)
	}
	err = lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{
		NextRoute: jobdb.Route{JobType: "job", TaskType: "external"},
		TaskWait:  &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, ResumeJobType: "job", InputHash: "input"},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := int64(0)
	return r, cfg, jobdb.CompleteTaskIfWaitingRequest{
		JobKey: h.JobKey, Route: jobdb.Route{JobType: "job", TaskType: "external"},
		OutputOrdinal: 1, InputHash: "input", Data: jobdb.NewTaskDataOrPanic(2),
		ClientPayloadUpdate: &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"done":true}`), ExpectedRevision: &revision},
	}
}

// Inject failures at the two durable boundaries and reopen the database, as a
// new daemon would. Advancing the lease deadline avoids a wall-clock sleep.
func TestTaskCompletionRecoversAfterRestart(t *testing.T) {
	for _, afterChapter := range []bool{false, true} {
		name := "before_chapter"
		if afterChapter {
			name = "after_chapter"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			r, cfg, req := waitingTaskFixture(t)
			trigger := `CREATE TRIGGER fail_completion BEFORE INSERT ON jobdb_chapter_chapters BEGIN SELECT RAISE(ABORT, 'injected failure'); END`
			if afterChapter {
				trigger = `CREATE TRIGGER fail_completion BEFORE UPDATE ON jobdb_jobs WHEN OLD.lease_id IS NOT NULL AND NEW.lease_id IS NULL BEGIN SELECT RAISE(ABORT, 'injected failure'); END`
			}
			if _, err := r.db.ExecContext(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			if err := r.CompleteTaskIfWaiting(ctx, req); err == nil || (afterChapter && !strings.Contains(err.Error(), "injected failure")) {
				t.Fatalf("completion: %v", err)
			}
			if err := r.Close(ctx); err != nil {
				t.Fatal(err)
			}
			recovered, err := NewFromConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close(ctx)
			row, err := recovered.loadJobRow(ctx, req.JobKey)
			if err != nil {
				t.Fatal(err)
			}
			if row.nextRoute != (jobdb.Route{JobType: "job"}) || !row.leaseID.Valid {
				t.Fatalf("missing recovery claim: %+v", row)
			}
			if wait, err := extractTaskWaitFromRaw(row.payload); err != nil || wait != nil {
				t.Fatalf("stale task wait: %v %v", wait, err)
			}
			if _, err := recovered.db.ExecContext(ctx, `DROP TRIGGER fail_completion`); err != nil {
				t.Fatal(err)
			}
			if _, err := recovered.db.ExecContext(ctx, `UPDATE jobdb_jobs SET lease_expires_at_ns=0`); err != nil {
				t.Fatal(err)
			}
			taskLease, err := recovered.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: req.JobKey, WorkerID: "task-worker", Routes: []jobdb.Route{req.Route}})
			if err != nil || taskLease != nil {
				t.Fatalf("task worker acquired recovery: %v %v", taskLease, err)
			}
			lease, err := recovered.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: req.JobKey, WorkerID: "recovery", Routes: []jobdb.Route{{JobType: "job"}}})
			if err != nil || lease == nil {
				t.Fatalf("job recovery lease: %v %v", lease, err)
			}
			chapter, err := recovered.GetChapter(ctx, jobdb.ChapterRef{JobKey: req.JobKey, Ordinal: 1})
			if afterChapter {
				if err != nil {
					t.Fatal(err)
				}
				outcome := chapter.Body.(jobdb.TaskAttemptOutcomeChapter).Outcome.(jobdb.ApplicationOutputOutcome)
				if string(outcome.Output.Data) != "2" || lease.ClientPayloadRevision() != 1 || string(lease.ClientPayload()) != `{"done":true}` {
					t.Fatalf("lost output/payload: %+v %s", chapter, lease.ClientPayload())
				}
			} else {
				if !errors.Is(err, jobdb.ErrChapterNotFound) {
					t.Fatalf("unexpected chapter: %v", err)
				}
				if lease.ClientPayloadRevision() != 0 || lease.ClientPayload() != nil {
					t.Fatalf("payload committed without chapter: %s", lease.ClientPayload())
				}
				if err := lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: req.Route, TaskWait: &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, ResumeJobType: "job", InputHash: "input"}}); err != nil {
					t.Fatal(err)
				}
				if err := recovered.CompleteTaskIfWaiting(ctx, req); err != nil {
					t.Fatalf("retry after reconstructed wait: %v", err)
				}
			}
		})
	}
}

func TestTaskCompletionClaimAndPublicationAreFenced(t *testing.T) {
	for _, cause := range []string{"expired", "replaced", "cancelled", "payload_conflict"} {
		t.Run(cause, func(t *testing.T) {
			ctx := context.Background()
			r, _, req := waitingTaskFixture(t)
			row, err := r.loadJobRow(ctx, req.JobKey)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := r.claimWaitingTask(ctx, row, "job", jobdb.RunPolicy{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := r.claimWaitingTask(ctx, row, "job", jobdb.RunPolicy{}); !errors.Is(err, jobdb.ErrConflict) {
				t.Fatalf("duplicate claim: %v", err)
			}
			statement := map[string]string{
				"expired":          `UPDATE jobdb_jobs SET lease_expires_at_ns=0`,
				"replaced":         `UPDATE jobdb_jobs SET lease_id='replacement'`,
				"cancelled":        `UPDATE jobdb_jobs SET cancel_requested=1`,
				"payload_conflict": `UPDATE jobdb_jobs SET client_payload_revision=1`,
			}[cause]
			if _, err := r.db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
			chapter := story.NewChapter().WithOrdinal(1).WithBytes([]byte(`{}`))
			err = r.publishTaskOutput(ctx, lease, chapter, req.ClientPayloadUpdate)
			if err == nil {
				t.Fatal("accepted invalid publication")
			}
			if cause != "payload_conflict" && !errors.Is(err, jobdb.ErrExecutionLeaseLost) {
				t.Fatalf("publication: %v", err)
			}
			if _, err := r.GetChapter(ctx, jobdb.ChapterRef{JobKey: req.JobKey, Ordinal: 1}); !errors.Is(err, jobdb.ErrChapterNotFound) {
				t.Fatalf("invalid claim wrote chapter: %v", err)
			}
		})
	}
}

func TestTaskCompletionClaimRejectsChangedWaitAndUnavailableTask(t *testing.T) {
	ctx := context.Background()
	r, _, req := waitingTaskFixture(t)
	row, err := r.loadJobRow(ctx, req.JobKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET route_task_type='other'`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.claimWaitingTask(ctx, row, "job", jobdb.RunPolicy{}); !errors.Is(err, jobdb.ErrConflict) {
		t.Fatalf("changed wait: %v", err)
	}
	if _, err := r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET route_task_type='external',available_at_ns=?`, timeToNS(time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := r.CompleteTaskIfWaiting(ctx, req); !errors.Is(err, jobdb.ErrConflict) {
		t.Fatalf("future task: %v", err)
	}
}
