package remote

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

// Deliberately accepts submissions without checking their lease. The shared
// server must reject invalid authority before reaching these provider methods.
type submissionDispatchProbe struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
	leaseOperationRuntime
	validate func(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
	calls    int
	parent   jobdb.JobKey
	worker   string
}

func (p *submissionDispatchProbe) ValidateExecutionLeaseByID(ctx context.Context, key jobdb.JobKey, lease, worker string) (jobdb.LeaseValidity, error) {
	return p.validate(ctx, key, lease, worker)
}
func (p *submissionDispatchProbe) SubmitJobWithLeaseByID(_ context.Context, parent jobdb.JobKey, _, worker string, _ jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	p.calls++
	p.parent, p.worker = parent, worker
	return jobdb.JobHandle{JobKey: jobdb.JobKey{TenantId: parent.TenantId, JobId: "child"}}, nil
}
func (p *submissionDispatchProbe) SubmitRestartJobWithLeaseByID(ctx context.Context, parent jobdb.JobKey, lease, worker string, _ jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	return p.SubmitJobWithLeaseByID(ctx, parent, lease, worker, jobdb.SubmitJobRequest{})
}

func TestParentIdentityServerGuardsProviderDispatch(t *testing.T) {
	for _, invalidation := range []string{"cancel", "complete", "expire", "replace", "missing-worker", "missing-validator"} {
		t.Run(invalidation, func(t *testing.T) {
			f := newDelegationFixture(t, "toy", nil)
			ctx := celltest.Context(context.Background())
			owner := f.owner.(*remoteExecutionLease)
			probe := &submissionDispatchProbe{
				WorkflowRuntime: f.backend, ConfigurationAdmin: f.backend.(jobdb.ConfigurationAdmin),
				leaseOperationRuntime: runtimeLeaseOps(f.backend),
				validate: f.backend.(interface {
					ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
				}).ValidateExecutionLeaseByID,
			}
			var runtime jobdb.WorkflowRuntime = probe
			if invalidation == "missing-validator" {
				// Hide the optional validator, keeping submission and all required ports.
				runtime = struct {
					jobdb.WorkflowRuntime
					jobdb.ConfigurationAdmin
					leaseOperationRuntime
				}{f.backend, probe.ConfigurationAdmin, probe}
			}
			handler, err := newServer(runtime, f.signer)
			require.NoError(t, err)
			server := httptest.NewServer(celltest.Handler(handler))
			t.Cleanup(server.Close)
			f.server = server
			body, err := submitJobRequestToAPI(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, err)
			restart, err := submitRestartJobRequestToAPI(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: owner.jobKey, Cell: "test", CellRepo: "example.test/test"}})
			require.NoError(t, err)
			operations := []struct {
				method, suffix string
				body           any
			}{
				{"POST", "/jobs", body}, {"PUT", "/jobs/child", body},
				{"POST", "/jobs/restarts", restart}, {"PUT", "/jobs/child/restart", restart},
			}
			if invalidation != "missing-validator" {
				for _, op := range operations {
					status, raw := delegationHTTP(t, f, op.method, delegationBase(owner)+op.suffix, owner.LeaseToken(), op.body)
					require.Equal(t, http.StatusOK, status, string(raw))
				}
				require.Equal(t, 4, probe.calls)
				require.Equal(t, owner.jobKey, probe.parent)
				require.Equal(t, owner.workerID, probe.worker)
			}
			switch invalidation {
			case "cancel":
				require.NoError(t, f.backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: owner.jobKey}))
			case "complete":
				final := jobdb.Chapter{Ordinal: 1, TaskType: owner.Route().JobType, CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`null`)}}}}
				require.NoError(t, owner.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
			case "expire", "replace":
				_, err = f.backend.(leaseRenewalRuntime).KeepAliveLeaseByIDWithExpiry(ctx, owner.jobKey, owner.leaseID, owner.workerID, time.Nanosecond)
				require.NoError(t, err)
				if invalidation == "replace" {
					later, err := f.backend.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: owner.jobKey, WorkerID: "new-worker", Routes: []jobdb.Route{owner.Route()}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					require.NotNil(t, later)
				}
			}
			calls := probe.calls
			token := owner.LeaseToken()
			if invalidation == "missing-worker" {
				h := owner.Job()
				token, err = f.signer.mintWithPrincipal(owner.jobKey, owner.leaseID, "", "", owner.LeaseExpiry(), time.Minute, nil, jobdb.ResolvedCell{Cell: h.Cell, CellID: h.CellID, CellRepo: h.CellRepo})
				require.NoError(t, err)
			}
			for _, op := range operations {
				status, raw := delegationHTTP(t, f, op.method, delegationBase(owner)+op.suffix, token, op.body)
				want := http.StatusConflict
				if invalidation == "missing-validator" {
					want = http.StatusNotImplemented
				}
				require.Equal(t, want, status, string(raw))
			}
			require.Equal(t, calls, probe.calls, "invalid authority reached provider submission")
		})
	}
}

func TestParentIdentityConformance(t *testing.T) {
	runPayloadAndRouteConformance(t, runtimetest.RunParentIdentityConformance)
}

func TestParentIdentityHTTPAuthority(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			f := newDelegationFixture(t, kind, nil)
			ctx := celltest.Context(context.Background())
			owner := f.owner.(*remoteExecutionLease)
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", JobID: "child", JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}}
			body, err := submitJobRequestToAPI(ctx, req)
			require.NoError(t, err)
			restartBody, err := submitRestartJobRequestToAPI(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: owner.Job().JobKey, Cell: "test", CellRepo: "example.test/test"}})
			require.NoError(t, err)
			base := delegationBase(owner)
			for _, operation := range []struct {
				suffix string
				body   any
			}{{"/jobs/child", body}, {"/jobs/child/restart", restartBody}} {
				for _, token := range []string{"", "invalid", owner.LeaseToken() + "tampered"} {
					status, _ := delegationHTTP(t, f, "PUT", base+operation.suffix, token, operation.body)
					require.GreaterOrEqual(t, status, 400, "credential rejection")
					require.Less(t, status, 500)
				}
				for _, badBase := range []string{strings.Replace(base, "/tenants/tenant/", "/tenants/other/", 1), strings.Replace(base, "/jobs/"+owner.jobKey.JobId+"/", "/jobs/other/", 1), strings.Replace(base, "/leases/"+owner.leaseID, "/leases/other", 1)} {
					status, _ := delegationHTTP(t, f, "PUT", badBase+operation.suffix, owner.LeaseToken(), operation.body)
					require.GreaterOrEqual(t, status, 400, "path binding")
					require.Less(t, status, 500)
				}
			}
			_, err = f.backend.GetJob(ctx, jobdb.JobKey{TenantId: "tenant", JobId: "child"})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
			denied := f.create(t, jobdb.CreateDelegationRequest{})
			_, err = denied.SubmitJob(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			allowed := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
			child, err := allowed.SubmitJob(ctx, req)
			require.NoError(t, err)
			list, err := f.backend.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{child.JobKey}})
			require.NoError(t, err)
			require.Len(t, list.Jobs, 1)
			require.Equal(t, owner.jobKey.JobId, list.Jobs[0].ParentJobID)
			// Existing delegation grants intentionally exclude every restart operation.
			_, err = allowed.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: child.JobKey}})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			// A token with a valid signature and future expiry cannot revive invalidated
			// stored authority, even when replaying an already accepted child.
			require.NoError(t, f.backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: owner.jobKey}))
			status, _ := delegationHTTP(t, f, "PUT", base+"/jobs/child", owner.LeaseToken(), body)
			require.GreaterOrEqual(t, status, 400)
			require.Less(t, status, 500)
			_, err = allowed.SubmitJob(ctx, req)
			require.Error(t, err)
			info, err := f.backend.GetJob(ctx, child.JobKey)
			require.NoError(t, err)
			require.Equal(t, child.GoalID, info.GoalID)
		})
	}
}

func TestParentIdentityHTTPSignedTokenCannotReviveLease(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			f := newDelegationFixture(t, kind, nil)
			ctx := celltest.Context(context.Background())
			owner := f.owner.(*remoteExecutionLease)
			token, err := f.signer.mintForLease(owner, time.Minute)
			require.NoError(t, err)
			renewer, ok := f.backend.(interface {
				KeepAliveLeaseByIDWithExpiry(context.Context, jobdb.JobKey, string, string, time.Duration) (time.Time, error)
			})
			require.True(t, ok)
			_, err = renewer.KeepAliveLeaseByIDWithExpiry(ctx, owner.jobKey, owner.leaseID, owner.workerID, time.Nanosecond)
			require.NoError(t, err)
			body, err := submitJobRequestToAPI(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", JobID: "stale-child", JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, err)
			status, _ := delegationHTTP(t, f, "PUT", delegationBase(owner)+"/jobs/stale-child", token, body)
			require.GreaterOrEqual(t, status, 400)
			require.Less(t, status, 500)
			later, err := f.backend.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: owner.jobKey, WorkerID: "new-worker", Routes: []jobdb.Route{owner.Route()}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, later)
			status, _ = delegationHTTP(t, f, "PUT", delegationBase(owner)+"/jobs/stale-child", token, body)
			require.GreaterOrEqual(t, status, 400)
			require.Less(t, status, 500)
			_, err = f.backend.GetJob(ctx, jobdb.JobKey{TenantId: owner.jobKey.TenantId, JobId: "stale-child"})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
		})
	}
}
