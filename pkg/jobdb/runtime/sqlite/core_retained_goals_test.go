package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

// Adding the optional port forwards to the actual SQLite transaction. The
// retained composition below uses the very same scheduler without that port.
type legacyGoalScheduler struct {
	*coreEventScheduler
	jobdb.GoalCandidateMutator
}

func TestRetainedCoreGoalScheduler(t *testing.T) {
	ctx := celltest.Context(context.Background())
	storage, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "goals.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close(ctx)) })
	scheduler := &coreEventScheduler{r: storage, CellRuntime: storage, GoalScheduler: storage}
	for _, typ := range []reflect.Type{reflect.TypeFor[runtimecore.GoalScheduler](), reflect.TypeFor[runtimecore.Scheduler](), reflect.TypeOf(scheduler)} {
		_, found := typ.MethodByName("AddGoalCandidate")
		require.False(t, found, typ.String())
	}
	cfg := runtimecore.Config{Scheduler: scheduler, Chapters: coreEventChapters{r: storage}, Schemas: sqliteSchemaStore{runtime: storage}, Configurations: storage.Service.Store}
	core, err := runtimecore.NewRuntime(cfg)
	require.NoError(t, err)
	view := struct{ jobdb.ConsumerRuntime }{core}
	require.NotImplements(t, (*jobdb.GoalCandidateMutator)(nil), view)
	submit := func(id string) jobdb.JobHandle {
		h, err := celltest.Submit(view, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(id)}})
		require.NoError(t, err)
		return h
	}
	a, b := submit("a"), submit("b")
	goal := jobdb.GoalKey{TenantId: "tenant", GoalID: a.GoalID}
	before, err := view.GetGoal(ctx, goal)
	require.NoError(t, err)
	req := jobdb.AddGoalCandidateRequest{GoalKey: goal, JobID: b.JobKey.JobId}
	result, err := core.AddGoalCandidate(ctx, req)
	require.ErrorIs(t, err, errors.ErrUnsupported)
	require.Empty(t, result)
	after, err := view.GetGoal(ctx, goal)
	require.NoError(t, err)
	require.Equal(t, before, after)
	// Retained scheduler operations still reach real storage and core leases.
	lease, err := view.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: b.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	_, err = lease.(jobdb.RenewableExecutionLease).Renew(ctx)
	require.NoError(t, err)
	events, err := view.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"b"}})
	require.NoError(t, err)
	require.Len(t, events.Events, 2)
	// The compatibility path must genuinely attach, preserving the historical
	// event's primary identity while secondary filters match the same events.
	cfg.Scheduler = legacyGoalScheduler{scheduler, storage}
	legacy, err := runtimecore.NewRuntime(cfg)
	require.NoError(t, err)
	result, err = legacy.AddGoalCandidate(ctx, req)
	require.NoError(t, err)
	require.Equal(t, "b", result.JobID)
	attached, err := storage.GetJob(ctx, b.JobKey)
	require.NoError(t, err)
	require.Equal(t, b.GoalID, attached.GoalID)
	require.Equal(t, []string{a.GoalID}, attached.OtherGoalIDs)
	matched, err := view.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", GoalIDs: []string{a.GoalID, a.GoalID}, JobIDs: []string{"b"}})
	require.NoError(t, err)
	require.Equal(t, events, matched)
	_, err = legacy.AddGoalCandidate(ctx, jobdb.AddGoalCandidateRequest{GoalKey: goal, JobID: "missing"})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	closed, err := view.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goal, Message: " \tstop 雪\n"})
	require.NoError(t, err)
	require.Equal(t, jobdb.GoalFailed, closed.State)
}
