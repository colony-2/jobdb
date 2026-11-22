package resourceconsumer

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffPublicC01(t *testing.T) {
	ctx := celltest.Context(context.Background())
	rt := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
	h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: "resource", Data: jobdb.NewTaskDataOrPanic("input")}})
	require.NoError(t, err)
	lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "resource"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	req, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, rt, lease, "task/attempt:1", 1, jobdb.DefaultResourceBackoffPolicy())
	require.NoError(t, err)
	port, ok := lease.(jobdb.ResourceBackoffLease)
	require.True(t, ok)
	first, err := port.RescheduleResourceBackoff(ctx, req)
	require.NoError(t, err)
	again, err := port.RescheduleResourceBackoff(ctx, req)
	require.NoError(t, err)
	require.Equal(t, first, again)
	require.EqualValues(t, 5, first.ResolvedCrashAllowance)
	require.ErrorIs(t, wf.EAGAINWait(wf.TaskContext{}), jobdb.ErrResourceBackoffUnsupported)
	require.False(t, implementsResource(oldLease{}))
	require.ErrorIs(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "resource"}}), jobdb.ErrExecutionLeaseLost)
}
func implementsResource(v any) bool { _, ok := v.(jobdb.ResourceBackoffLease); return ok }

func TestResourceBackoffOldReaderB02c(t *testing.T) {
	policy := jobdb.DefaultResourceBackoffPolicy()
	metadata, _ := json.Marshal(jobdb.ResourceCheckpoint{Version: 1, Invocation: "task/attempt:1", Attempt: 1, Policy: policy})
	rec := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "resource_checkpoint", ID: "task/attempt:1", At: time.Now().UTC(), Payload: metadata}
	raw, err := json.Marshal(rec)
	require.NoError(t, err)
	_, err = beforeResourceDecodeExecutionRecord(raw)
	require.ErrorContains(t, err, "unknown execution kind")
	_, err = jobdb.DecodeExecutionRecord(raw)
	require.NoError(t, err)
	raw = []byte(strings.Replace(string(raw), `"payload":{"version":1`, `"payload":{"version":2`, 1))
	_, err = jobdb.DecodeExecutionRecord(raw)
	require.ErrorContains(t, err, "resource checkpoint")
}

func TestResourceBackoffPublicSQLite(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := sqlite.Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "public.db")}
	backend, err := sqlite.NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close(ctx)) }()
	options := remote.ServerOptions{LeaseTokenSigningKey: bytes.Repeat([]byte{11}, 32), LeaseTokenAudience: "public-resource"}
	handler, err := remote.NewServerWithOptions(backend, options)
	require.NoError(t, err)
	var mu sync.RWMutex
	server := httptest.NewServer(celltest.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { mu.RLock(); defer mu.RUnlock(); handler.ServeHTTP(w, r) })))
	defer server.Close()
	client, err := remote.NewProjectRuntime(server.URL, server.Client())
	require.NoError(t, err)
	h, err := celltest.Submit(client, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "public", Cell: "test", CellRepo: "example.test/test", JobType: "resource", Data: jobdb.NewTaskDataOrPanic("input")}})
	require.NoError(t, err)
	lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "public", Routes: []jobdb.Route{{JobType: "resource"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NoError(t, lease.ExecutionState().ResourceBackoff.Validate(true))
	p := jobdb.DefaultResourceBackoffPolicy()
	allowance := int64(1<<53) + 19
	p.CrashAllowance = &allowance
	req, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, backend, lease, "public/attempt:1", 1, p)
	require.NoError(t, err)
	receipt, err := lease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
	require.NoError(t, err)
	require.Equal(t, allowance, receipt.ResolvedCrashAllowance)
	require.NoError(t, backend.Close(ctx))
	backend, err = sqlite.NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	replacement, err := remote.NewServerWithOptions(backend, options)
	require.NoError(t, err)
	mu.Lock()
	handler = replacement
	mu.Unlock()
	again, err := lease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
	require.NoError(t, err)
	require.Equal(t, receipt, again)
	info, err := client.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, allowance, *info.ExecutionState.CrashBudgetRemaining)
	require.Equal(t, receipt.Resume, *info.ExecutionState.ResourceResume)
}
