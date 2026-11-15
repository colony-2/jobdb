package runtimetest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

// RunParentIdentityConformance checks durable parent facts through public runtime
// and HTTP APIs. These cases exercise recorded ancestry and lease-backed retries;
// they do not certify trusted native storage entry points or direct SQL writers.
func RunParentIdentityConformance(t *testing.T, harnesses ...Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			f := h.New(t)
			require.NotNil(t, f.Runtime)
			if f.Cleanup != nil {
				t.Cleanup(func() { require.NoError(t, f.Cleanup(context.Background())) })
			}
			r := f.Runtime
			ctx := celltest.Context(context.Background())
			tenant := "parent-identity"
			route := jobdb.Route{JobType: "work"}
			request := func(id string) jobdb.SubmitJobRequest {
				return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: id, Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1), Metadata: json.RawMessage(`{"internal":{"parentJobId":"forged"},"data":true}`)}}
			}
			submit := func(id string) jobdb.JobHandle {
				got, err := celltest.Submit(r, ctx, request(id))
				require.NoError(t, err)
				return got
			}
			acquire := func(key jobdb.JobKey, worker string) jobdb.ExecutionLease {
				l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: worker, Routes: []jobdb.Route{route}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				require.NotNil(t, l)
				return l
			}
			summary := func(key jobdb.JobKey, parent string) {
				list, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenant}, JobKeys: []jobdb.JobKey{key}, Stores: []jobdb.JobStore{jobdb.JobStoreActive, jobdb.JobStoreArchived}})
				require.NoError(t, err)
				require.Len(t, list.Jobs, 1)
				require.Equal(t, parent, list.Jobs[0].ParentJobID)
				require.JSONEq(t, string(request("").Job.Metadata), string(list.Jobs[0].Metadata))
			}
			a := submit("A")
			b := submit("B")
			summary(a.JobKey, "")
			la := acquire(a.JobKey, "worker-1")
			lb := acquire(b.JobKey, "worker-B")
			childReq := request("child")
			child, err := la.SubmitJob(ctx, childReq)
			require.NoError(t, err)
			summary(child.JobKey, "A")
			// Root, other-parent and changed-input retries cannot steal accepted facts.
			_, err = r.SubmitJob(ctx, childReq)
			require.Error(t, err)
			_, err = lb.SubmitJob(ctx, childReq)
			require.Error(t, err)
			changed := childReq
			changed.Job.Data = jobdb.NewTaskDataOrPanic(2)
			_, err = la.SubmitJob(ctx, changed)
			require.Error(t, err)
			summary(child.JobKey, "A")
			// A different live lease/worker of A can retry the immutable operation.
			require.NoError(t, la.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: route}))
			later := acquire(a.JobKey, "worker-2")
			replay, err := later.SubmitJob(ctx, childReq)
			require.NoError(t, err)
			require.Equal(t, child, replay)
			_, err = la.SubmitJob(ctx, request("former-lease"))
			require.Error(t, err)
			_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: tenant, JobId: "former-lease"})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
			// Archive reads and retries continue to use dedicated parent facts.
			completeLeaseForTest(t, ctx, acquire(child.JobKey, "child-worker"), 1)
			summary(child.JobKey, "A")
			replay, err = later.SubmitJob(ctx, childReq)
			require.NoError(t, err)
			require.Equal(t, child, replay)
			_, err = r.SubmitJob(ctx, childReq)
			require.Error(t, err)
			_, err = lb.SubmitJob(ctx, childReq)
			require.Error(t, err)
			_, err = later.SubmitJob(ctx, changed)
			require.Error(t, err)
			filtered, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenant}, ParentJobIDs: []string{"A"}, Stores: []jobdb.JobStore{jobdb.JobStoreActive, jobdb.JobStoreArchived}})
			require.NoError(t, err)
			require.Len(t, filtered.Jobs, 1)
			require.Equal(t, child.JobKey, filtered.Jobs[0].JobKey)
			forged, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenant}, ParentJobIDs: []string{"forged"}})
			require.NoError(t, err)
			require.Empty(t, forged.Jobs)
			roots, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenant}, RootOnly: true})
			require.NoError(t, err)
			require.Len(t, roots.Jobs, 2)
			// Restarting A's child from B must record B, independently of source ancestry.
			restart := func(id string) jobdb.SubmitRestartJobRequest {
				return jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{JobID: id, PriorJobKey: child.JobKey, Cell: "test", CellRepo: "example.test/test", LastStepToKeep: 0}}
			}
			rootRestart, err := r.SubmitRestartJob(ctx, restart("root-restart"))
			require.NoError(t, err)
			summary(rootRestart.JobKey, "")
			childRestart, err := lb.SubmitRestartJob(ctx, restart("child-restart"))
			require.NoError(t, err)
			summary(childRestart.JobKey, "B")
			_, err = later.SubmitRestartJob(ctx, restart("child-restart"))
			require.Error(t, err)
			require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: a.JobKey}))
			_, err = later.SubmitJob(ctx, childReq)
			require.Error(t, err)
			_, err = later.SubmitJob(ctx, request("cancelled-parent"))
			require.Error(t, err)
			_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: tenant, JobId: "cancelled-parent"})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
			completeLeaseForTest(t, ctx, lb, 1)
			_, err = lb.SubmitJob(ctx, request("completed-parent"))
			require.Error(t, err)
			_, err = r.GetJob(ctx, jobdb.JobKey{TenantId: tenant, JobId: "completed-parent"})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
		})
	}
}
