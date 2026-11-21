package sqlite

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

func (s *coreEventScheduler) JobGoalCapabilities() jobdb.JobGoalCapabilities {
	return s.r.JobGoalCapabilities()
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
	_, mutation := reflect.TypeOf(core).MethodByName("AddGoalCandidate")
	require.False(t, mutation)
	submit := func(id string) jobdb.JobHandle {
		h, err := celltest.Submit(view, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(id)}})
		require.NoError(t, err)
		return h
	}
	a, b := submit("a"), submit("b")
	goal := jobdb.GoalKey{TenantId: "tenant", GoalID: a.GoalID}
	before, err := view.GetGoal(ctx, goal)
	require.NoError(t, err)
	// Retained scheduler operations still reach real storage and core leases.
	lease, err := view.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: b.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	_, err = lease.(jobdb.RenewableExecutionLease).Renew(ctx)
	require.NoError(t, err)
	events, err := view.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"b"}})
	require.NoError(t, err)
	require.Len(t, events.Events, 2)
	matched, err := view.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", GoalIDs: []string{a.GoalID}, JobIDs: []string{"b"}})
	require.NoError(t, err)
	require.Empty(t, matched.Events, "another goal cannot acquire this job's events")
	after, err := view.GetGoal(ctx, goal)
	require.NoError(t, err)
	require.Equal(t, before, after)
	closed, err := view.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: goal, Message: " \tstop 雪\n"})
	require.NoError(t, err)
	require.Equal(t, jobdb.GoalFailed, closed.State)
}
