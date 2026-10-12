package runtimetest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

// RunConfigurationConformance checks the required admin and lease read contract
// on an isolated runtime. Backend adapters can also call it through remote.New.
func RunConfigurationConformance(t *testing.T, rt jobdb.WorkflowRuntime) {
	t.Helper()
	ctx := context.Background()
	admin, ok := rt.(jobdb.ConfigurationAdmin)
	require.True(t, ok)
	empty, err := admin.GetTenantConfiguration(ctx, "configuration-tenant")
	require.NoError(t, err)
	require.Empty(t, empty.Values)
	require.NotNil(t, empty.Values)
	revision, err := admin.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "configuration-tenant", Name: "a/b.c", Value: json.RawMessage(`{"large":9007199254740993123456789,"nested":[null,false,0,""]}`)})
	require.NoError(t, err)
	job, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "configuration-tenant", JobType: "config-job", Data: jobdb.NewTaskDataOrPanic("input")}})
	require.NoError(t, err)
	lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "config-worker", Routes: []jobdb.Route{{JobType: "config-job"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	first, err := lease.GetConfiguration(ctx)
	require.NoError(t, err)
	require.Equal(t, revision, first.Revision)
	require.Contains(t, string(first.Values["a/b.c"]), "9007199254740993123456789")
	secondRevision, err := admin.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "configuration-tenant", Name: "a/b.c", Value: json.RawMessage(`null`), ExpectedRevision: &revision})
	require.NoError(t, err)
	second, err := lease.GetConfiguration(ctx)
	require.NoError(t, err)
	require.Equal(t, secondRevision, second.Revision)
	require.Equal(t, "null", string(second.Values["a/b.c"]))
	require.NotEqual(t, first.Revision, second.Revision)
	second.Values["a/b.c"][0] = 'x'
	third, err := lease.GetConfiguration(ctx)
	require.NoError(t, err)
	require.Equal(t, "null", string(third.Values["a/b.c"]))
	_, err = admin.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "configuration-tenant", Name: "a/b.c", ExpectedRevision: &revision})
	require.ErrorIs(t, err, jobdb.ErrConfigurationRevisionMismatch)
	_, err = admin.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "configuration-tenant", Name: "a/b.c", ExpectedRevision: &secondRevision})
	require.NoError(t, err)
	removed, err := lease.GetConfiguration(ctx)
	require.NoError(t, err)
	require.Empty(t, removed.Values)
	_, err = admin.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "another-tenant", Name: "other", Value: json.RawMessage(`1`)})
	require.NoError(t, err)
	isolated, err := lease.GetConfiguration(ctx)
	require.NoError(t, err)
	require.Empty(t, isolated.Values)
	// Raw identifiers are insufficient for embedded or remote transport APIs.
	_, err = rt.GetConfiguration(ctx, jobdb.GetConfigurationRequest{JobKey: job.JobKey, LeaseID: lease.LeaseID()})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	// Task routes carry the same tenant read authority under their own lease.
	chapter, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: job.JobKey, Ordinal: 0})
	require.NoError(t, err)
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{
		NextRoute: jobdb.Route{JobType: "config-job", TaskType: "config-task"},
		TaskWait:  &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: chapter.InputHash, ResumeJobType: "config-job"},
	}))
	_, err = lease.GetConfiguration(ctx)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	lease, err = rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "task-worker", Routes: []jobdb.Route{{JobType: "config-job", TaskType: "config-task"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	taskSnapshot, err := lease.GetConfiguration(ctx)
	require.NoError(t, err)
	require.Empty(t, taskSnapshot.Values)
	require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: job.JobKey, WorkerID: "admin"}))
	lost, err := lease.GetConfiguration(ctx)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Empty(t, lost.Values)
}
