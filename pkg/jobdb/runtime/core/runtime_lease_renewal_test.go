package runtimecore_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type renewalTestScheduler struct {
	runtimecore.Scheduler
	snapshot runtimecore.LeaseSnapshot
	mutation runtimecore.LeaseMutation
	err      error
}

func (s *renewalTestScheduler) KeepAliveLease(_ context.Context, m runtimecore.LeaseMutation) (runtimecore.LeaseSnapshot, error) {
	s.mutation = m
	return s.snapshot, s.err
}
func TestCoreRenewReturnsAuthoritativeSnapshotWithoutAcquisition(t *testing.T) {
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	scheduler := &renewalTestScheduler{snapshot: runtimecore.LeaseSnapshot{Identity: runtimecore.LeaseIdentity{JobKey: key, LeaseID: "lease", WorkerID: "owner", ExpiresAt: time.Now().Add(time.Minute)}, RouteJobType: "job", WorkKind: runtimecore.WorkKindTask, TaskWork: &runtimecore.TaskWork{TaskType: "task", ResumeJobType: "job", InputOrdinal: 2, OutputOrdinal: 3, InputHash: "hash"}, ClientPayload: json.RawMessage(`{"cursor":3}`), ClientPayloadRevision: 4, SchemaHash: "schema", Duration: time.Minute}}
	runtime, err := runtimecore.NewRuntime(runtimecore.Config{Scheduler: scheduler, Chapters: readTestChapters{}, Schemas: readTestSchemas{}})
	require.NoError(t, err)
	lease, err := runtime.RenewExecutionLeaseByID(context.Background(), key, "lease", "owner", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "owner", scheduler.mutation.Identity.WorkerID)
	require.Equal(t, "lease", lease.LeaseID())
	require.Equal(t, jobdb.Route{JobType: "job", TaskType: "task"}, lease.Route())
	require.Equal(t, int64(3), lease.ExecutionState().TaskWait.OutputOrdinal)
	scheduler.snapshot.ClientPayloadRevision = 5
	scheduler.snapshot.ClientPayload = json.RawMessage(`{"cursor":4}`)
	next, err := lease.Renew(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(5), next.ClientPayloadRevision())
	require.Equal(t, int64(4), lease.ClientPayloadRevision())
	scheduler.err = jobdb.ErrExecutionLeaseLost
	_, err = next.Renew(context.Background())
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
}
