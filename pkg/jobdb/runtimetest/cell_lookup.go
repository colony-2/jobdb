package runtimetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func runLeaseCellLookup(t *testing.T, rt jobdb.WorkflowRuntime) {
	tenant := "lookup-" + ksuid.New().String()
	admin, ctx := cellAdmin(tenant), context.Background()
	create := func(tenant, name, repo string) jobdb.Cell {
		c, err := rt.CreateCell(cellAdmin(tenant), jobdb.CreateCellRequest{TenantID: tenant, Name: name, Repo: repo})
		require.NoError(t, err)
		return c
	}
	a := create(tenant, "A", "example.test/a")
	b := create(tenant, "B", "git@github.com:org/b.git")
	retired := create(tenant, "retired", "example.test/retired")
	_, err := rt.RetireCell(admin, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: retired.ID}, IfMatch: fmt.Sprintf("%q", retired.Revision)})
	require.NoError(t, err)
	create(tenant+"-other", "B", "example.test/foreign-b")
	create(tenant+"-other", "foreign-only", "example.test/foreign-only")
	job, err := rt.SubmitJob(admin, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, Cell: a.Name, JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	lease, err := rt.GetJobLease(admin, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "lookup", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, err)
	require.NotNil(t, lease)
	cellsBefore, err := rt.ListCells(admin, jobdb.ListCellsRequest{TenantID: tenant, State: "ALL"})
	require.NoError(t, err)
	jobsBefore, err := rt.ListJobs(admin, jobdb.ListJobsRequest{TenantIds: []string{tenant}})
	require.NoError(t, err)
	original, err := lease.LookupCell(ctx, "B")
	require.NoError(t, err)
	require.Equal(t, b.Resolved(), original)
	require.Equal(t, "github.com/org/b", original.CellRepo)
	for _, tc := range []struct {
		name string
		err  error
	}{{"", jobdb.ErrInvalidCell}, {".", jobdb.ErrInvalidCell}, {"a/b", jobdb.ErrInvalidCell}, {"missing", jobdb.ErrCellNotFound}, {"foreign-only", jobdb.ErrCellNotFound}, {"retired", jobdb.ErrCellRetired}} {
		_, err := lease.LookupCell(ctx, tc.name)
		require.ErrorIs(t, err, tc.err, tc.name)
	}
	cellsAfter, err := rt.ListCells(admin, jobdb.ListCellsRequest{TenantID: tenant, State: "ALL"})
	require.NoError(t, err)
	require.Equal(t, cellsBefore, cellsAfter)
	jobsAfter, err := rt.ListJobs(admin, jobdb.ListJobsRequest{TenantIds: []string{tenant}})
	require.NoError(t, err)
	require.Equal(t, jobsBefore, jobsAfter)
	name, repo := "renamed", "https://github.com/org/new-b.git"
	b, err = rt.UpdateCell(admin, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: tenant, CellID: b.ID}, IfMatch: fmt.Sprintf("%q", b.Revision), Name: &name, Repo: &repo})
	require.NoError(t, err)
	for _, name := range []string{"B", b.Name} {
		got, err := lease.LookupCell(ctx, name)
		require.NoError(t, err)
		require.Equal(t, b.Resolved(), got)
	}
	require.Equal(t, "B", original.Cell)
	require.Equal(t, "github.com/org/b", original.CellRepo)

	// Use a real completed target so restart denial cannot be explained by a
	// missing job or invalid history. Neither its ID nor its repo grants authority.
	prior, err := rt.SubmitJob(admin, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, Cell: b.Name, JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	priorLease, err := rt.GetJobLease(admin, jobdb.GetJobLeaseRequest{JobKey: prior.JobKey, WorkerID: "prior", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, err)
	require.NotNil(t, priorLease)
	require.NoError(t, priorLease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "job", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "fixture"}}}}}))
	checkRestricted := func(t *testing.T, l jobdb.ExecutionLease, restricted context.Context, denied error, canSubmitA bool) {
		got, err := l.LookupCell(restricted, "B")
		require.NoError(t, err)
		require.Equal(t, b.Resolved(), got)
		_, err = l.ResolveCell(restricted, got.Cell)
		require.Error(t, err, "existing resolver still requires submission authority")
		_, err = l.SubmitJob(restricted, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, Cell: got.Cell, CellRepo: got.CellRepo, JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
		require.ErrorIs(t, err, denied)
		_, err = l.SubmitRestartJob(restricted, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: prior.JobKey, Cell: got.Cell, CellRepo: got.CellRepo, LastStepToKeep: 0}})
		require.ErrorIs(t, err, denied)
		if canSubmitA {
			_, err = l.SubmitJob(restricted, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, Cell: a.Name, JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, err)
		}
	}
	var delegated jobdb.ExecutionLease
	if delegator, ok := rt.(jobdb.LeaseDelegator); ok {
		for _, tc := range []struct {
			name        string
			permissions jobdb.DelegationPermissions
			canSubmitA  bool
		}{
			{name: "empty grant"},
			{name: "read only", permissions: jobdb.DelegationPermissions{Read: &jobdb.DelegationChapters{Any: true}}},
			{name: "submit A", permissions: jobdb.DelegationPermissions{Submit: &jobdb.DelegationSubmitPermission{JobTypes: jobdb.DelegationNames{Any: true}, JobIDs: jobdb.DelegationNames{Any: true}, CellNames: &jobdb.DelegationNames{Names: []string{a.Name}}}}, canSubmitA: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				l, err := delegator.CreateDelegation(ctx, lease, jobdb.CreateDelegationRequest{Permissions: tc.permissions})
				require.NoError(t, err)
				checkRestricted(t, l, ctx, jobdb.ErrLeasePermissionDenied, tc.canSubmitA)
				_, err = l.LookupCell(ctx, "a/b")
				require.ErrorIs(t, err, jobdb.ErrInvalidCell)
				_, err = l.LookupCell(ctx, "retired")
				require.ErrorIs(t, err, jobdb.ErrCellRetired)
				_, err = l.CreateCell(ctx, jobdb.CreateCellRequest{Name: "ungranted", Repo: "example.test/ungranted"})
				require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
				delegated = l
			})
		}
	} else {
		if lookup, ok := rt.(interface {
			LookupCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error)
		}); ok {
			for _, tc := range []struct {
				key           jobdb.JobKey
				lease, worker string
			}{
				{jobdb.JobKey{TenantId: tenant + "-other", JobId: job.JobKey.JobId}, lease.LeaseID(), "lookup"},
				{jobdb.JobKey{TenantId: tenant, JobId: "other"}, lease.LeaseID(), "lookup"},
				{job.JobKey, "forged", "lookup"},
				{job.JobKey, lease.LeaseID(), "forged"},
			} {
				_, err := lookup.LookupCellWithLeaseByID(ctx, tc.key, tc.lease, tc.worker, "B")
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			}
		}
		for _, tc := range []struct {
			name        string
			permissions map[string]bool
			denied      error
		}{{"no submit", nil, jobdb.ErrCellForbidden}, {"submit A", map[string]bool{"jobs.submit": true}, jobdb.ErrCellNotFound}} {
			t.Run(tc.name, func(t *testing.T) {
				restricted := jobdb.WithCellAuthority(ctx, jobdb.CellAuthority{Principal: "restricted", TenantID: tenant, Permissions: tc.permissions, Cells: map[string]bool{a.ID: true}})
				checkRestricted(t, lease, restricted, tc.denied, tc.permissions != nil)
			})
		}
	}
	require.NoError(t, rt.CancelJob(admin, jobdb.CancelJobRequest{JobKey: job.JobKey}))
	_, err = lease.LookupCell(ctx, "B")
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	if delegated != nil {
		_, err = delegated.LookupCell(ctx, "B")
		require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	}
}
