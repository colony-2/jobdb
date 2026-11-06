package cli

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type clearCrashRecorder struct {
	*toy.Runtime
	request jobdb.ClearCrashConcernRequest
}

func (r *clearCrashRecorder) ClearCrashConcern(ctx context.Context, req jobdb.ClearCrashConcernRequest) error {
	r.request = req
	return r.Runtime.ClearCrashConcern(ctx, req)
}

func TestClearCrashConcernCLI(t *testing.T) {
	ctx := celltest.Context(context.Background())
	backend := toy.New()
	h, err := celltest.Submit(backend, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", JobID: "flapping", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	acquire := func() jobdb.ExecutionLease {
		lease, err := backend.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
		require.NoError(t, err)
		return lease
	}
	for i := 0; i <= 5; i++ {
		lease := acquire()
		require.NotNil(t, lease)
		require.NoError(t, backend.KeepAliveLeaseByID(ctx, h.JobKey, lease.LeaseID(), "worker", time.Nanosecond))
	}
	require.Nil(t, acquire())
	recorder := &clearCrashRecorder{Runtime: backend}
	handler := remote.NewServer(recorder)
	var requests atomic.Int32
	t.Setenv("JOBDB_BEARER_TOKEN", "operator-token")
	server := httptest.NewServer(celltest.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, "Bearer operator-token", r.Header.Get("Authorization"))
		require.Equal(t, http.MethodPost, r.Method)
		handler.ServeHTTP(w, r)
	})))
	defer server.Close()
	run := func(command string, args ...string) (string, error) {
		cmd := NewRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs(append([]string{"jobs", command, "-u", server.URL}, args...))
		err := cmd.ExecuteContext(ctx)
		return out.String(), err
	}
	for _, args := range [][]string{{}, {"flapping"}, {"-t", "c2"}, {"-t", "c2", "one", "two"}, {"-t", "c2", ""}, {"-t", "c2", "flapping", "-u", "file:///tmp/jobdb"}} {
		_, err := run("clear-crash-concern", args...)
		require.Error(t, err)
	}
	require.Zero(t, requests.Load())
	out, err := run("clear-crash-concern", "-t", "c2", "flapping", "--reason", "worker fixed", "--worker-id", "operator")
	require.NoError(t, err)
	require.Contains(t, out, "Cleared crash concern for job c2/flapping")
	require.Equal(t, jobdb.ClearCrashConcernRequest{JobKey: h.JobKey, WorkerID: "operator", Reason: "worker fixed"}, recorder.request)
	out, err = run("reset-crash-concern", "-t", "c2", "flapping", "-j")
	require.NoError(t, err)
	require.JSONEq(t, `{"tenantId":"c2","jobId":"flapping","cleared":true}`, out)
	_, err = run("clear-crash-concern", "-t", "wrong", "flapping")
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	require.NotNil(t, acquire(), "the command must restore acquisition")
	_, err = run("clear-crash-concern", "-t", "c2", "flapping")
	require.ErrorIs(t, err, jobdb.ErrConflict)
}
