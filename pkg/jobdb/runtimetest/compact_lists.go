package runtimetest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
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
				require.Empty(t, row.ClientPayload)
				require.Empty(t, row.CompletionDetail)
				require.Equal(t, jobdb.ExecutionState{}, row.ExecutionState)
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
