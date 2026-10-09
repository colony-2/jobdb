package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type suppliedJob struct {
	run func(workflow.JobContext, jobdb.JobData) (jobdb.JobData, error)
}

func (suppliedJob) Name() string { return "supplied" }
func (j suppliedJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return j.run(ctx, data)
}

type suppliedTask struct{}

func (suppliedTask) Name() string { return "echo" }
func (suppliedTask) Run(_ workflow.TaskContext, data jobdb.TaskData) (jobdb.TaskData, error) {
	return data, nil
}

type suppliedFixture struct {
	backend                *sqlite.Runtime
	dispatcher, receiver   *remote.Runtime
	key                    jobdb.JobKey
	lease                  jobdb.ExecutionLease
	acquisitions, renewals atomic.Int64
	failRenew              atomic.Bool
	corruptRenew           atomic.Value // string fault applied after a successful backend renewal
	corruptedRenewals      atomic.Int64
}

func newSuppliedFixture(t *testing.T) *suppliedFixture {
	t.Helper()
	ctx := context.Background()
	backend, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.Close(ctx)) })
	f := &suppliedFixture{backend: backend}
	handler := remote.NewServer(backend)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Receiver") == "yes" {
			if r.URL.Path == "/v1/jobs/poll" || strings.HasSuffix(r.URL.Path, "/lease") {
				f.acquisitions.Add(1)
				http.Error(w, "acquisition forbidden", http.StatusForbidden)
				return
			}
			if strings.HasSuffix(r.URL.Path, "/keepalive") {
				f.renewals.Add(1)
				if f.failRenew.Load() {
					http.Error(w, "injected failure", http.StatusServiceUnavailable)
					return
				}
				if fault, _ := f.corruptRenew.Load().(string); fault != "" {
					// Commit a real backend renewal before damaging its response. The
					// client must still stop, without assuming the renewal had no effect.
					recorded := httptest.NewRecorder()
					handler.ServeHTTP(recorded, r)
					if recorded.Code != http.StatusOK {
						t.Errorf("backend renewal failed before fault injection: status %d", recorded.Code)
						w.WriteHeader(recorded.Code)
						return
					}
					f.corruptedRenewals.Add(1)
					w.Header().Set("Content-Type", "application/json")
					switch fault {
					case "decode":
						fmt.Fprint(w, `{"broken":`)
					case "read":
						w.Header().Set("Content-Length", "100")
						fmt.Fprint(w, `{"broken":`)
					case "snapshot":
						var response map[string]any
						if err := json.Unmarshal(recorded.Body.Bytes(), &response); err != nil {
							t.Error(err)
							w.WriteHeader(500)
							return
						}
						response["lease"].(map[string]any)["clientPayloadRevision"] = "private-renewal-secret"
						if err := json.NewEncoder(w).Encode(response); err != nil {
							t.Error(err)
						}
					case "unsupported":
						fmt.Fprint(w, `{"leaseToken":"private-renewal-secret"}`)
					}
					return
				}
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	f.dispatcher, err = remote.New(server.URL, server.Client())
	require.NoError(t, err)
	f.receiver, err = remote.New(server.URL, &http.Client{Transport: receiverTransport{base: server.Client().Transport}})
	require.NoError(t, err)
	handle, err := f.dispatcher.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "supplied", Data: jobdb.NewTaskDataOrPanic(7, jobdb.NewArtifactFromBytes("input.txt", []byte("artifact"))), ClientPayloadUpdate: &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"cursor":1}`)}}})
	require.NoError(t, err)
	f.key = handle.JobKey
	f.lease, err = f.dispatcher.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: f.key, WorkerID: "dispatcher", Routes: []jobdb.Route{{JobType: "supplied"}}, LeaseDuration: time.Second})
	require.NoError(t, err)
	require.NotNil(t, f.lease)
	return f
}

type receiverTransport struct{ base http.RoundTripper }

func (r receiverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Receiver", "yes")
	return r.base.RoundTrip(req)
}
func (f *suppliedFixture) imported(t *testing.T) jobdb.RenewableExecutionLease {
	t.Helper()
	capability, err := remote.ExportLease(f.lease)
	require.NoError(t, err)
	encoded, err := capability.Encode()
	require.NoError(t, err)
	decoded, err := remote.DecodeLeaseCapability(encoded)
	require.NoError(t, err)
	lease, err := f.receiver.ImportLease(context.Background(), decoded)
	require.NoError(t, err)
	require.Equal(t, f.lease.LeaseID(), lease.LeaseID())
	require.Equal(t, "dispatcher", lease.LeaseWorkerID())
	return lease
}
func TestSuppliedRemoteLeaseRenewsAndPersists(t *testing.T) {
	f := newSuppliedFixture(t)
	lease := f.imported(t)
	job := suppliedJob{run: func(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
		if err := ctx.AwaitDuration(jobdb.Duration(1400 * time.Millisecond)); err != nil {
			return nil, err
		}
		if _, err := ctx.SubmitJob(context.Background(), jobdb.SubmitJob{JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}); err != nil {
			return nil, err
		}
		return ctx.DoTask(jobdb.RunPolicy{}, "echo", data)
	}}
	runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, lease, workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: job, TaskWorkers: []workflow.TaskWorker{suppliedTask{}}, BeforeRun: func(_ context.Context, l jobdb.ExecutionLease) error {
		require.JSONEq(t, `{"cursor":1}`, string(l.ClientPayload()))
		_, err := remote.ExportLease(l)
		return err
	}})
	require.NoError(t, err)
	outcome, err := runnable.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunCompleted, outcome.Status)
	require.Zero(t, f.acquisitions.Load())
	require.GreaterOrEqual(t, f.renewals.Load(), int64(4))
	artifacts, err := outcome.Output.GetArtifacts()
	require.NoError(t, err)
	require.Len(t, artifacts, 1)
	raw, err := artifacts[0].Bytes(context.Background())
	require.NoError(t, err)
	require.Equal(t, "artifact", string(raw))
	count := f.renewals.Load()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, count, f.renewals.Load())
}
func TestSuppliedLeaseAdmissionReschedulesWithoutWork(t *testing.T) {
	f := newSuppliedFixture(t)
	var called atomic.Bool
	runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, f.imported(t), workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(_ workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
		called.Store(true)
		return data, nil
	}}, BeforeRun: func(ctx context.Context, l jobdb.ExecutionLease) error {
		return l.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "elsewhere"}})
	}})
	require.NoError(t, err)
	out, err := runnable.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunSuspended, out.Status)
	require.False(t, called.Load())
	require.Zero(t, f.acquisitions.Load())
	chapters, err := f.backend.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: f.key})
	require.NoError(t, err)
	require.Len(t, chapters, 1)
}
func TestSuppliedLeaseLossCancelsWithoutFailureChapter(t *testing.T) {
	for _, transport := range []bool{false, true} {
		t.Run(map[bool]string{false: "revoked", true: "transport"}[transport], func(t *testing.T) {
			f := newSuppliedFixture(t)
			entered := make(chan struct{})
			exited := make(chan struct{})
			runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, f.imported(t), workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
				close(entered)
				defer close(exited)
				return nil, ctx.AwaitDuration(jobdb.Duration(time.Minute))
			}}})
			require.NoError(t, err)
			result := make(chan error, 1)
			go func() { _, err := runnable.Run(nil); result <- err }()
			<-entered
			if transport {
				f.failRenew.Store(true)
			} else {
				require.NoError(t, f.backend.CancelJob(context.Background(), jobdb.CancelJobRequest{JobKey: f.key}))
			}
			select {
			case err := <-result:
				var renewal *jobdb.LeaseRenewalError
				require.ErrorAs(t, err, &renewal)
				require.Equal(t, !transport, errors.Is(err, jobdb.ErrExecutionLeaseLost))
			case <-time.After(3 * time.Second):
				t.Fatal("lease loss did not stop runner")
			}
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("execution context not cancelled")
			}
			chapters, err := f.backend.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: f.key})
			require.NoError(t, err)
			require.Len(t, chapters, 1)
			require.Zero(t, f.acquisitions.Load())
		})
	}
}
func TestSuppliedLeaseInvalidBeforeExecution(t *testing.T) {
	for _, kind := range []string{"cancelled", "expired", "initial_transport", "wrong_job", "wrong_worker", "unsupported_route"} {
		t.Run(kind, func(t *testing.T) {
			f := newSuppliedFixture(t)
			lease := f.imported(t)
			var called atomic.Bool
			req := workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(_ workflow.JobContext, d jobdb.JobData) (jobdb.JobData, error) { called.Store(true); return d, nil }}}
			if kind == "wrong_job" {
				req.JobKey.JobId = "different"
			}
			if kind == "wrong_worker" {
				req.WorkerID = "receiver"
			}
			if kind == "unsupported_route" {
				require.NoError(t, lease.Reschedule(context.Background(), jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "other"}}))
				var err error
				f.lease, err = f.dispatcher.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: f.key, WorkerID: "dispatcher", Routes: []jobdb.Route{{JobType: "other"}}})
				require.NoError(t, err)
				lease = f.imported(t)
			}
			runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, lease, req)
			if kind == "wrong_job" || kind == "wrong_worker" {
				require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
				return
			}
			require.NoError(t, err)
			if kind == "cancelled" {
				require.NoError(t, f.backend.CancelJob(context.Background(), jobdb.CancelJobRequest{JobKey: f.key}))
			}
			if kind == "expired" {
				time.Sleep(time.Until(lease.LeaseExpiry()) + 10*time.Millisecond)
			}
			if kind == "initial_transport" {
				f.failRenew.Store(true)
			}
			_, err = runnable.Run(nil)
			require.Error(t, err)
			require.False(t, called.Load())
			require.Zero(t, f.acquisitions.Load())
		})
	}
}

func TestSuppliedLeaseDamagedRenewalResponseStopsExecution(t *testing.T) {
	for _, tc := range []struct {
		fault    string
		phase    remote.LeaseFailurePhase
		category remote.LeaseFailureCategory
	}{
		{"decode", remote.LeasePhaseDecode, remote.LeaseFailureDecode},
		{"read", remote.LeasePhaseRead, remote.LeaseFailureRead},
		{"snapshot", remote.LeasePhaseSnapshot, remote.LeaseFailureSnapshot},
		{"unsupported", remote.LeasePhaseSnapshot, remote.LeaseFailureUnsupported},
	} {
		for _, heartbeat := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/heartbeat=%t", tc.fault, heartbeat), func(t *testing.T) {
				f := newSuppliedFixture(t)
				entered, exited := make(chan struct{}), make(chan struct{})
				lease := f.imported(t)
				runnable, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, lease, workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(ctx workflow.JobContext, _ jobdb.JobData) (jobdb.JobData, error) {
					close(entered)
					defer close(exited)
					return nil, ctx.AwaitDuration(jobdb.Duration(time.Minute))
				}}})
				require.NoError(t, err)
				if !heartbeat {
					f.corruptRenew.Store(tc.fault)
				}
				result := make(chan error, 1)
				go func() { _, err := runnable.Run(nil); result <- err }()
				if heartbeat {
					select {
					case <-entered:
					case <-time.After(3 * time.Second):
						t.Fatal("runner did not start")
					}
					f.corruptRenew.Store(tc.fault)
				}
				select {
				case err = <-result:
				case <-time.After(3 * time.Second):
					t.Fatal("failed renewal did not stop execution")
				}
				var renewal *jobdb.LeaseRenewalError
				require.ErrorAs(t, err, &renewal)
				var diagnostic *remote.LeaseTransportError
				require.ErrorAs(t, err, &diagnostic)
				require.Equal(t, "renew", diagnostic.Operation)
				require.Equal(t, tc.phase, diagnostic.Phase)
				require.Equal(t, tc.category, diagnostic.Category)
				require.Equal(t, 200, diagnostic.StatusCode)
				require.True(t, diagnostic.ResponseReceived)
				require.False(t, errors.Is(err, jobdb.ErrExecutionLeaseLost))
				require.Equal(t, tc.fault == "unsupported", errors.Is(err, jobdb.ErrLeaseRenewalUnsupported))
				require.Contains(t, err.Error(), "HTTP status 200")
				require.Contains(t, err.Error(), string(tc.phase))
				require.NotContains(t, err.Error(), "private-renewal-secret")
				if heartbeat {
					select {
					case <-exited:
					case <-time.After(time.Second):
						t.Fatal("execution context was not cancelled")
					}
				} else {
					select {
					case <-entered:
						t.Fatal("execution started after initial renewal failed")
					default:
					}
				}
				chapters, err := f.backend.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: f.key})
				require.NoError(t, err)
				require.Len(t, chapters, 1, "renewal failure must not append an outcome")
				require.Zero(t, f.acquisitions.Load())
				require.Equal(t, int64(1), f.corruptedRenewals.Load(), "failed renewal must not retry")
			})
		}
	}
}
func TestSuppliedInMemoryLease(t *testing.T) {
	backend := toy.New()
	ctx := context.Background()
	handle, err := backend.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "supplied", Data: jobdb.NewTaskDataOrPanic(3)}})
	require.NoError(t, err)
	lease, err := backend.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "owner", Routes: []jobdb.Route{{JobType: "supplied"}}, LeaseDuration: 100 * time.Millisecond})
	require.NoError(t, err)
	runnable, err := workflow.GetJobForRunWithLease(ctx, backend, lease, workflow.GetJobForRunRequest{JobKey: handle.JobKey, JobWorker: suppliedJob{run: func(ctx workflow.JobContext, d jobdb.JobData) (jobdb.JobData, error) {
		return d, ctx.AwaitDuration(jobdb.Duration(250 * time.Millisecond))
	}}})
	require.NoError(t, err)
	out, err := runnable.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunCompleted, out.Status)
}

func TestSuppliedLeaseTaskHandoffAndResume(t *testing.T) {
	f := newSuppliedFixture(t)
	job := suppliedJob{run: func(ctx workflow.JobContext, d jobdb.JobData) (jobdb.JobData, error) {
		return ctx.DoTask(jobdb.RunPolicy{}, "echo", d)
	}}
	first, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, f.imported(t), workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: job})
	require.NoError(t, err)
	out, err := first.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunSuspended, out.Status)
	require.Zero(t, f.acquisitions.Load())
	f.lease, err = f.dispatcher.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{JobKey: f.key, WorkerID: "task-owner", Routes: []jobdb.Route{{JobType: "supplied", TaskType: "echo"}}})
	require.NoError(t, err)
	require.NotNil(t, f.lease)
	cap, err := remote.ExportLease(f.lease)
	require.NoError(t, err)
	imported, err := f.receiver.ImportLease(context.Background(), cap)
	require.NoError(t, err)
	require.Equal(t, f.lease.ExecutionState(), imported.ExecutionState())
	require.Equal(t, f.lease.Route(), imported.Route())
	require.NotNil(t, imported.ExecutionState().TaskWait)
	require.Equal(t, f.lease.ClientPayloadRevision(), imported.ClientPayloadRevision())
	require.JSONEq(t, string(f.lease.ClientPayload()), string(imported.ClientPayload()))
	second, err := workflow.GetJobForRunWithLease(context.Background(), f.receiver, imported, workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: job, TaskWorkers: []workflow.TaskWorker{suppliedTask{}}})
	require.NoError(t, err)
	out, err = second.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunCompleted, out.Status)
	require.Zero(t, f.acquisitions.Load())
}

func TestSuppliedLeaseCallerCancellationStopsHeartbeat(t *testing.T) {
	f := newSuppliedFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	exited := make(chan struct{})
	runnable, err := workflow.GetJobForRunWithLease(ctx, f.receiver, f.imported(t), workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: suppliedJob{run: func(ctx workflow.JobContext, d jobdb.JobData) (jobdb.JobData, error) {
		close(entered)
		defer close(exited)
		return nil, ctx.AwaitDuration(jobdb.Duration(time.Minute))
	}}})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() { _, err := runnable.Run(nil); result <- err }()
	<-entered
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("runner ignored cancellation")
	}
	<-exited
	count := f.renewals.Load()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, count, f.renewals.Load())
	require.Zero(t, f.acquisitions.Load())
	chapters, err := f.backend.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: f.key})
	require.NoError(t, err)
	require.Len(t, chapters, 1)
}
