package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
	"github.com/stretchr/testify/require"
)

// RunCompactListConformance checks the default list projection and its detail
// replacements. Adapters should run it over both direct and HTTP runtimes.
func RunCompactListConformance(t *testing.T, harnesses ...Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			f := buildFixture(t, h)
			defer f.Shutdown(t)
			ctx := context.Background()
			r := f.Runtime
			t.Run("stored_fields", func(t *testing.T) { runStoredListFields(t, r, f.WorkerTenantID) })
			metadata := json.RawMessage(`{"label":"visible","nested":{"array":[1,true,"value"]},"large":9007199254740993}`)
			policy := jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 7, BackoffCoefficient: 1}}
			submit := func(id string) jobdb.JobHandle {
				handle, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
					TenantId: f.WorkerTenantID, JobID: id, JobType: "compact", Data: NumberTaskData(1),
					Metadata: metadata, RunPolicy: policy,
					ClientPayloadUpdate: &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"detail":"payload"}`)},
				}})
				require.NoError(t, err)
				return handle
			}
			producer := submit("producer")
			consumer := submit("consumer")
			_, err := r.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: jobdb.GoalKey{TenantId: f.WorkerTenantID, GoalID: producer.GoalID}, JobID: consumer.JobKey.JobId})
			require.NoError(t, err)
			lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: consumer.JobKey, WorkerID: "setup", Routes: []jobdb.Route{{JobType: "compact"}}})
			require.NoError(t, err)
			require.NotNil(t, lease)
			deadline := time.Now().UTC().Add(time.Hour)
			require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "compact"}, DependencyWait: &jobdb.DependencyWait{
				WaitID: "pending", Deadline: &deadline,
				Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: producer.JobKey.JobId}, Condition: jobdb.JobPrereqSuccess}},
			}}))
			filter, err := jobdb.Metadata().EqualFilter("label", "visible")
			require.NoError(t, err)
			req := jobdb.ListJobsRequest{TenantIds: []string{f.WorkerTenantID}, GoalIDs: []string{producer.GoalID}, MetadataFilter: filter, PageSize: 1}
			seen := map[jobdb.JobKey]bool{}
			for {
				page, err := r.ListJobs(ctx, req)
				require.NoError(t, err)
				require.Len(t, page.Jobs, 1)
				row := page.Jobs[0]
				require.False(t, seen[row.JobKey], "cursor repeated a job")
				seen[row.JobKey] = true
				require.JSONEq(t, string(metadata), string(row.Metadata))
				require.EqualValues(t, 1, row.ClientPayloadRevision)
				require.Equal(t, &jobdb.Route{JobType: "compact"}, row.NextRoute)
				require.Nil(t, row.BlockingWait)
				require.Empty(t, row.OtherGoalIDs)
				require.Empty(t, row.WaitFor)
				require.Equal(t, json.RawMessage(`{"detail":"payload"}`), row.ClientPayload)
				require.Empty(t, row.CompletionDetail)
				require.Equal(t, jobdb.ExecutionState{RunPolicy: policy}, row.ExecutionState)
				if row.JobKey == consumer.JobKey {
					require.Equal(t, consumer.GoalID, row.GoalID, "membership filtering must preserve the primary goal")
					require.Equal(t, jobdb.JobStatusPendingJobs, row.Status)
					require.Equal(t, &jobdb.DependencyWaitSummary{WaitID: "pending", Deadline: &deadline}, row.BlockingWaitSummary)
				} else {
					require.Nil(t, row.BlockingWaitSummary)
				}
				if page.NextPageToken == "" {
					break
				}
				req.PageToken = page.NextPageToken
			}
			require.Len(t, seen, 2, "secondary goal membership must still match")
			detail, err := r.GetJob(ctx, consumer.JobKey)
			require.NoError(t, err)
			require.Equal(t, []string{producer.GoalID}, detail.OtherGoalIDs)
			require.JSONEq(t, `{"detail":"payload"}`, string(detail.ClientPayload))
			require.Equal(t, policy, detail.ExecutionState.RunPolicy)
			require.Equal(t, &jobdb.Route{JobType: "compact"}, detail.NextRoute)
			require.NotNil(t, detail.BlockingWait)
			require.Len(t, detail.BlockingWait.Dependencies, 1)
			require.Equal(t, producer.JobKey.JobId, detail.BlockingWait.Dependencies[0].Target.JobID)
			require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: consumer.JobKey}))
			cancelled, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{f.WorkerTenantID}, JobKeys: []jobdb.JobKey{consumer.JobKey}})
			require.NoError(t, err)
			require.Len(t, cancelled.Jobs, 1)
			require.Nil(t, cancelled.Jobs[0].BlockingWaitSummary)
		})
	}
}

// Exercise the same stored projection through embedded and HTTP backends. Exact
// byte comparisons (rather than JSONEq's floating-point maps) detect lost numbers.
func runStoredListFields(t *testing.T, r jobdb.WorkflowRuntime, tenant string) {
	ctx := context.Background()
	policy := jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 7, BackoffCoefficient: 1}}
	parent, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: "stored-parent", JobType: "stored-parent", Data: NumberTaskData(1)}})
	require.NoError(t, err)
	parentLease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: parent.JobKey, WorkerID: "setup", Routes: []jobdb.Route{{JobType: "stored-parent"}}})
	require.NoError(t, err)
	require.NotNil(t, parentLease)
	prereq, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: "stored-prereq", JobType: "stored-prereq", Data: NumberTaskData(1)}})
	require.NoError(t, err)
	task := &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: "sha256:stored-input", ResumeJobType: "resume"}
	payloads := []json.RawMessage{
		json.RawMessage(`{"app":{"n":9007199254740993,"array":[null,true,"雪"]},"other":[18446744073709551615],"c2j":{"execution":{"mode":"local"}}}`),
		json.RawMessage(`[9007199254740993,null,{"outside":"c2j"}]`),
		json.RawMessage(`null`),
		json.RawMessage(`{"max":"` + strings.Repeat("x", clientpayload.MaxBytes-len(`{"max":""}`)) + `"}`),
	}
	for i, payload := range payloads {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			h, err := parentLease.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
				TenantId: tenant, JobID: fmt.Sprintf("stored-%d", i), JobType: "stored-fields", Data: NumberTaskData(1),
				RunPolicy:           policy,
				ClientPayloadUpdate: &jobdb.ClientPayloadUpdate{Mode: "reset", Value: payload},
			}})
			require.NoError(t, err)
			lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "setup", Routes: []jobdb.Route{{JobType: "stored-fields"}}})
			require.NoError(t, err)
			require.NotNil(t, lease)
			// Prerequisites are copied in stored order, without loading explanations.
			prerequisites := []string{prereq.JobKey.JobId, parent.JobKey.JobId}
			past := time.Now().UTC().Add(-time.Minute)
			require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{
				NextRoute: jobdb.Route{JobType: "stored-fields", TaskType: "input"}, TaskWait: task, WaitForJobIDs: prerequisites, WaitUntil: &past,
			}))
			for _, req := range []jobdb.ListJobsRequest{
				{TenantIds: []string{tenant}, JobKeys: []jobdb.JobKey{h.JobKey}},
				{TenantIds: []string{tenant}, ParentJobIDs: []string{"stored-parent"}, JobKeys: []jobdb.JobKey{h.JobKey}, Stores: []jobdb.JobStore{jobdb.JobStoreActive}},
			} {
				page, err := r.ListJobs(ctx, req)
				require.NoError(t, err)
				require.Len(t, page.Jobs, 1)
				got := page.Jobs[0]
				require.Equal(t, payload, got.ClientPayload)
				require.EqualValues(t, 1, got.ClientPayloadRevision)
				require.Equal(t, jobdb.ExecutionState{RunPolicy: policy, TaskWait: task}, got.ExecutionState)
				require.Equal(t, prerequisites, got.WaitFor)
				require.Empty(t, got.OtherGoalIDs)
				require.Nil(t, got.BlockingWait)
			}
			taskLease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "finish", Routes: []jobdb.Route{{JobType: "stored-fields", TaskType: "input"}}})
			require.NoError(t, err)
			require.NotNil(t, taskLease)
			final := jobdb.Chapter{Ordinal: 1, TaskType: "stored-fields", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "failure"}}}}
			detail := " \tfailure 雪\n "
			require.NoError(t, taskLease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Detail: detail, Chapter: &final}))
			page, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenant}, ParentJobIDs: []string{"stored-parent"}, JobKeys: []jobdb.JobKey{h.JobKey}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived}})
			require.NoError(t, err)
			require.Len(t, page.Jobs, 1)
			got := page.Jobs[0]
			require.Equal(t, payload, got.ClientPayload)
			require.EqualValues(t, 1, got.ClientPayloadRevision)
			require.Equal(t, jobdb.ExecutionState{RunPolicy: policy, TaskWait: task}, got.ExecutionState)
			// Some backends clear prerequisites on completion. The list must
			// expose their stored final state, matching selected-job inspection.
			info, err := r.GetJob(ctx, h.JobKey)
			require.NoError(t, err)
			require.Equal(t, append([]string{}, info.WaitFor...), append([]string{}, got.WaitFor...))
			require.Equal(t, "failed_app", got.CompletionStatus)
			require.Equal(t, detail, got.CompletionDetail)
		})
	}
}
