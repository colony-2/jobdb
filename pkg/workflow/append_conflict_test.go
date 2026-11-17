package workflow_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

type firstAppendConflict struct {
	jobdb.WorkflowRuntime
	fail bool
}

func (r *firstAppendConflict) PutChapter(ctx context.Context, q jobdb.PutChapterRequest) error {
	if r.fail {
		r.fail = false
		return jobdb.ErrConflict
	}
	return r.WorkflowRuntime.PutChapter(ctx, q)
}
func TestAppendFirstWorkerWriteConflictRetries(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			ctx := celltest.Context(context.Background())
			_, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
			require.NoError(t, err)
			h, err := rt.StartOrAppendJob(ctx, jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "conflict", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("start"), RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 1}}}, Append: json.RawMessage(`1`)}})
			require.NoError(t, err)
			calls := 0
			worker := wf.ExecutionWorker{JobType: "batch", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) { calls++; return in, nil }}
			fault := &firstAppendConflict{WorkflowRuntime: rt, fail: true}
			out, err := runExecutionTest(t, fault, h.JobKey, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunSuspended, out.Status)
			require.Zero(t, calls)
			out, err = runExecutionTest(t, fault, h.JobKey, worker)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.Equal(t, 1, calls)
		})
	}
}
