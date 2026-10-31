package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type catchCountingRuntime struct {
	jobdb.WorkflowRuntime
	reads atomic.Int32
}

func (r *catchCountingRuntime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	r.reads.Add(1)
	return r.WorkflowRuntime.ListChapters(ctx, req)
}

func TestCatchHistoryRuntimeErrors(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			count := &catchCountingRuntime{WorkflowRuntime: inspection}
			_, err := wf.ReadCatchHistory(context.Background(), count, jobdb.GetJobRunRequest{})
			require.Error(t, err)
			require.Zero(t, count.reads.Load())
			_, err = wf.ReadCatchHistory(context.Background(), nil, jobdb.GetJobRunRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "valid"}})
			require.ErrorContains(t, err, "nil runtime")
			key := submitExecutionTest(t, recording, wf.ExecutionWorker{JobType: "read-errors"})
			_, err = wf.ReadCatchHistory(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: jobdb.JobKey{TenantId: "other-tenant", JobId: key.JobId}})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
			_, err = wf.ReadCatchHistory(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: jobdb.JobKey{TenantId: key.TenantId, JobId: "missing"}})
			require.ErrorIs(t, err, jobdb.ErrJobNotFound)
			sentinel := errors.New("read failed")
			_, err = wf.ReadCatchHistory(context.Background(), catchFrozenRuntime{WorkflowRuntime: inspection, err: sentinel}, jobdb.GetJobRunRequest{JobKey: key})
			require.ErrorIs(t, err, sentinel)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = wf.ReadCatchHistory(ctx, inspection, jobdb.GetJobRunRequest{JobKey: key})
			require.Error(t, err)
			h, err := wf.ReadCatchHistory(nil, count, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			require.Empty(t, h.Attempts)
			require.EqualValues(t, 1, count.reads.Load())
		})
	}
}

func TestCatchHistoryTypedFailure(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			payload := jobdb.AppErrorPayload{Message: "typed", Level: "warning", Attrs: map[string]interface{}{"code": "E123", "nested": map[string]interface{}{"value": float64(7)}}}
			w := wf.ExecutionWorker{JobType: "typed-catch", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("typed", wf.ScopeOptions{Catch: []wf.CatchRule{{ID: "recover", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
					return wf.CatchResult{Disposition: wf.CatchHandled, Output: in}, nil
				}}}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, jobdb.AppError{Payload: payload} })
			}}
			key := submitExecutionTest(t, recording, w)
			out, err := runExecutionTest(t, recording, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			a := catchAt(t, readCatches(t, inspection, key), "typed")
			var got jobdb.AppErrorPayload
			require.NoError(t, json.Unmarshal(a.Original.Data, &got))
			require.Equal(t, payload, got)
			info, err := jobdb.GetJobRun(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			for _, e := range info.Execution {
				if e.Record.Kind == "result" && e.Record.ID == a.Boundary.AttemptID {
					require.Equal(t, e.Record.Error, a.Original.Data)
					require.Equal(t, e.Record.ErrorKind, a.Original.Kind)
				}
			}
		})
	}
}
