package jobdb_test

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestPrimaryGoalSelectionAuthorityIsIndependent(t *testing.T) {
	target := jobdb.ResolvedCell{Cell: "work", CellID: "stable", CellRepo: "example.test/work"}
	goal := jobdb.GoalKey{TenantId: "tenant", GoalID: "goal"}
	ctx := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{Principal: "caller", TenantID: "tenant", Permissions: map[string]bool{"jobs.submit": true}})
	require.ErrorIs(t, jobdb.AuthorizePrimaryGoalSelection(ctx, goal, target), jobdb.ErrPrimaryGoalSelectionForbidden)
	calls := 0
	allow := func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error { calls++; return nil }
	require.ErrorIs(t, jobdb.AuthorizePrimaryGoalSelection(jobdb.WithPrimaryGoalSelectionAuthorizer(context.Background(), allow), goal, target), jobdb.ErrPrimaryGoalSelectionForbidden)
	ctx = jobdb.WithPrimaryGoalSelectionAuthorizer(ctx, allow)
	require.NoError(t, jobdb.AuthorizePrimaryGoalSelection(ctx, goal, target))
	require.Equal(t, 1, calls)
	require.ErrorIs(t, jobdb.AuthorizePrimaryGoalSelection(ctx, jobdb.GoalKey{TenantId: "other", GoalID: "goal"}, target), jobdb.ErrPrimaryGoalSelectionForbidden)
	deny := jobdb.WithPrimaryGoalSelectionAuthorizer(ctx, func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error {
		return jobdb.ErrPrimaryGoalSelectionForbidden
	})
	require.ErrorIs(t, jobdb.AuthorizePrimaryGoalSelection(deny, goal, target), jobdb.ErrPrimaryGoalSelectionForbidden)
}
