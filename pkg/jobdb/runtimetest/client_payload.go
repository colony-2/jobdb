package runtimetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
)

// RunClientPayloadConformance checks scheduler state through public APIs.
func RunClientPayloadConformance(t *testing.T, harnesses ...Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			f := buildFixture(t, h)
			defer f.Shutdown(t)
			r := f.Runtime
			ctx := context.Background()
			initial := json.RawMessage(`{"n":9007199254740993,"nested":{"keep":1,"remove":2},"run_policy":"client-owned"}`)
			request := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: f.WorkerTenantID, JobID: "payload-job", JobType: "payloadjob", Data: NumberTaskData(1), RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 3}}, ClientPayloadUpdate: &jobdb.ClientPayloadUpdate{Mode: "reset", Value: initial}}}
			handle, err := r.SubmitJob(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			check := func(want string, revision int64) {
				t.Helper()
				info, err := r.GetJob(ctx, handle.JobKey)
				if err != nil {
					t.Fatal(err)
				}
				a, err := clientpayload.Digest(info.ClientPayload)
				if err != nil {
					t.Fatal(err)
				}
				var raw json.RawMessage
				if want != "" {
					raw = json.RawMessage(want)
				}
				b, _ := clientpayload.Digest(raw)
				if a != b || info.ClientPayloadRevision != revision || info.ExecutionState.RunPolicy.Retry.MaximumAttempts != 3 {
					t.Fatalf("job state: %s revision=%d policy=%+v want=%s/%d", info.ClientPayload, info.ClientPayloadRevision, info.ExecutionState, want, revision)
				}
			}
			leaseFor := func(capability string) jobdb.ExecutionLease {
				t.Helper()
				l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "payload-worker", Capabilities: []string{capability}})
				if err != nil || l == nil {
					t.Fatalf("lease %s: %v", capability, err)
				}
				return l
			}
			update := func(mode, value string, rev int64) *jobdb.ClientPayloadUpdate {
				var raw json.RawMessage
				if value != "" {
					raw = json.RawMessage(value)
				}
				return &jobdb.ClientPayloadUpdate{Mode: mode, Value: raw, ExpectedRevision: &rev}
			}
			check(string(initial), 1)
			l := leaseFor("payloadjob")
			if string(l.ClientPayload()) != string(initial) || l.ClientPayloadRevision() != 1 {
				t.Fatalf("lease snapshot %s/%d", l.ClientPayload(), l.ClientPayloadRevision())
			}
			// An ordinary task result must leave the active lease and payload untouched.
			chapter := appTaskAttemptChapterForTest(t, 1, "ordinary", "hash", []byte(`{}`), nil)
			if err := r.PutChapter(ctx, jobdb.PutChapterRequest{Ref: jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 1}, LeaseID: l.LeaseID(), LeaseToken: leaseTokenForTest(l), Chapter: chapter}); err != nil {
				t.Fatal(err)
			}
			check(string(initial), 1)
			if err := l.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextNeed: "payloadjob", ClientPayloadUpdate: update("patch", `{"nested":{"remove":null},"cursor":"next"}`, 0)}); !errors.Is(err, jobdb.ErrConflict) {
				t.Fatalf("stale revision: %v", err)
			}
			check(string(initial), 1)
			if err := l.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextNeed: "payloadjob", ClientPayloadUpdate: update("patch", `{"nested":{"remove":null},"cursor":"next"}`, 1)}); err != nil {
				t.Fatal(err)
			}
			patched := `{"n":9007199254740993,"nested":{"keep":1},"cursor":"next","run_policy":"client-owned"}`
			check(patched, 2)
			if _, err := r.SubmitJob(ctx, request); err != nil {
				t.Fatalf("identical retry: %v", err)
			}
			check(patched, 2)
			request.Job.ClientPayloadUpdate = &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`null`)}
			if _, err := r.SubmitJob(ctx, request); !errors.Is(err, jobdb.ErrExistingJobMismatch) {
				t.Fatalf("changed initial payload: %v", err)
			}
			l = leaseFor("payloadjob")
			task := &jobdb.TaskWait{InputOrdinal: 1, OutputOrdinal: 2, InputHash: "task-hash", ResumeNeed: "payloadjob"}
			if err := l.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextNeed: "payloadjob:external", TaskWait: task}); err != nil {
				t.Fatal(err)
			}
			check(patched, 2)
			taskLease := leaseFor("payloadjob:external")
			complete := jobdb.CompleteTaskIfWaitingRequest{JobKey: handle.JobKey, Capability: "payloadjob:external", InputOrdinal: 1, OutputOrdinal: 2, InputHash: "task-hash", ResumeNeed: "payloadjob", Data: NumberTaskData(2), ClientPayloadUpdate: update("reset", `null`, 2)}
			if err := r.CompleteTaskIfWaiting(ctx, complete); !errors.Is(err, jobdb.ErrConflict) {
				t.Fatalf("accepted task completion with live owner: %v", err)
			}
			if err := taskLease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextNeed: "payloadjob:external", TaskWait: task}); err != nil {
				t.Fatal(err)
			}
			if err := r.CompleteTaskIfWaiting(ctx, complete); err != nil {
				t.Fatal(err)
			}
			check(`null`, 3)
			l = leaseFor("payloadjob")
			final := jobdb.Chapter{Ordinal: 3, TaskType: "payloadjob", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`{}`)}}}}
			if err := l.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final, ClientPayloadUpdate: update("reset", "", 3)}); err != nil {
				t.Fatal(err)
			}
			check("", 4)
			request.Job.ClientPayloadUpdate = &jobdb.ClientPayloadUpdate{Mode: "reset", Value: initial}
			if _, err := r.SubmitJob(ctx, request); err != nil {
				t.Fatalf("archived submission retry: %v", err)
			}
			restart := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: handle.JobKey, JobID: "restart-payload", LastStepToKeep: 1, ClientPayloadUpdate: &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"restart":true}`)}}}
			restarted, err := r.SubmitRestartJob(ctx, restart)
			if err != nil {
				t.Fatal(err)
			}
			info, err := r.GetJob(ctx, restarted.JobKey)
			if err != nil || string(info.ClientPayload) != `{"restart":true}` || info.ClientPayloadRevision != 1 {
				t.Fatalf("restart state: %+v %v", info, err)
			}
			if _, err := r.SubmitRestartJob(ctx, restart); err != nil {
				t.Fatal(err)
			}
			restart.Job.ClientPayloadUpdate = nil
			if _, err := r.SubmitRestartJob(ctx, restart); !errors.Is(err, jobdb.ErrExistingJobMismatch) {
				t.Fatalf("restart initial mismatch: %v", err)
			}
			listed, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{f.WorkerTenantID}, JobKeys: []jobdb.JobKey{handle.JobKey}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived}})
			if err != nil {
				t.Fatal(err)
			}
			if len(listed.Jobs) != 1 || listed.Jobs[0].ClientPayload != nil || listed.Jobs[0].ClientPayloadRevision != 4 {
				t.Fatalf("archived list: %+v", listed)
			}
		})
	}
}
