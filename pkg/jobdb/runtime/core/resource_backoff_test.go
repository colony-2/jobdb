package runtimecore

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestResourceBackoffOptionalPorts(t *testing.T) {
	t.Run("B10a_B15b_hidden", func(t *testing.T) {
		var hidden jobdb.ExecutionLease = struct{ jobdb.ExecutionLease }{nil}
		_, ok := hidden.(jobdb.ResourceBackoffLease)
		require.False(t, ok)
		var scheduler Scheduler
		_, ok = any(struct{ Scheduler }{scheduler}).(ResourceBackoffScheduler)
		require.False(t, ok)
		require.ErrorIs(t, (*jobdb.ResourceBackoffCapability)(nil).Validate(false), jobdb.ErrResourceBackoffUnsupported)
	})
	t.Run("S01_known_zero", func(t *testing.T) {
		zero := int64(0)
		known := resourceExecutionState(jobdb.RunPolicy{}, nil, &zero, &jobdb.ResourceBackoffCapability{ProtocolVersion: 1}, nil)
		require.NotNil(t, known.CrashBudgetRemaining)
		require.Zero(t, *known.CrashBudgetRemaining)
		unknown := resourceExecutionState(jobdb.RunPolicy{}, nil, nil, nil, nil)
		require.Nil(t, unknown.CrashBudgetRemaining)
	})
	t.Run("B10a_no_fallback", func(t *testing.T) {
		r := &Runtime{scheduler: struct{ Scheduler }{}, chapters: struct{ ChapterLog }{}, schemas: &SchemaRegistry{}, projects: celltest.UnrestrictedProjects{}}
		req := jobdb.ResourceBackoffRequest{Version: 1, RequestID: "r", Invocation: "task", Attempt: 1, Reason: jobdb.ResourceBackoffReason, Policy: jobdb.DefaultResourceBackoffPolicy(), Resume: jobdb.ResourceResume{Version: 1, CheckpointHash: strings.Repeat("a", 64), Invocation: "task", Attempt: 1, Route: jobdb.Route{JobType: "work"}}}
		_, err := r.RescheduleResourceBackoffByID(context.Background(), jobdb.JobKey{TenantId: "tenant", JobId: "job"}, "l", "w", req)
		require.ErrorIs(t, err, jobdb.ErrResourceBackoffUnsupported)
		zero := int64(0)
		lease, err := r.wrapLease(LeaseSnapshot{CrashBudgetRemaining: &zero, ResourceBackoff: &jobdb.ResourceBackoffCapability{ProtocolVersion: 1, DurabilityVersion: 1}})
		require.NoError(t, err)
		require.Nil(t, lease.ExecutionState().ResourceBackoff)
		require.NotNil(t, lease.ExecutionState().CrashBudgetRemaining)
		require.Zero(t, *lease.ExecutionState().CrashBudgetRemaining)
	})
	t.Run("B15b_S01_hidden_detail_and_list", func(t *testing.T) {
		zero := int64(0)
		row := StoredJob{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, Store: jobdb.JobStoreActive, RouteJobType: "work", Status: jobdb.JobStatusReady, CrashBudgetRemaining: &zero, ResourceBackoff: &jobdb.ResourceBackoffCapability{ProtocolVersion: 1, DurabilityVersion: 1}}
		r := &Runtime{now: time.Now, scheduler: resourceSnapshotScheduler{row: row}, chapters: struct{ ChapterLog }{}, schemas: &SchemaRegistry{}, projects: celltest.UnrestrictedProjects{}}
		info, err := r.GetJob(context.Background(), row.JobKey)
		require.NoError(t, err)
		require.Nil(t, info.ExecutionState.ResourceBackoff)
		require.NotNil(t, info.ExecutionState.CrashBudgetRemaining)
		require.Zero(t, *info.ExecutionState.CrashBudgetRemaining)
		list, err := r.ListJobs(context.Background(), jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
		require.NoError(t, err)
		require.Len(t, list.Jobs, 1)
		require.Equal(t, info.ExecutionState, list.Jobs[0].ExecutionState)
	})
}

type resourceSnapshotScheduler struct {
	Scheduler
	row StoredJob
}

func (s resourceSnapshotScheduler) GetJob(context.Context, jobdb.JobKey) (StoredJob, error) {
	return s.row, nil
}
func (s resourceSnapshotScheduler) ListJobs(context.Context, ListJobsRequest) (ListJobsResponse, error) {
	return ListJobsResponse{Jobs: []StoredJob{s.row}}, nil
}
