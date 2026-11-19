package sqlite

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestInfrastructureChildStorageFailurePreservesParent(t *testing.T) {
	for _, overHTTP := range []bool{false, true} {
		for _, mode := range []string{"submit", "restart", "append", "invalid"} {
			t.Run(map[bool]string{false: "direct", true: "http"}[overHTTP]+"/"+mode, func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")})
				require.NoError(t, err)
				defer r.Close(ctx)
				var runtime jobdb.WorkflowRuntime = r
				if overHTTP {
					server := httptest.NewServer(celltest.Handler(remote.NewServer(r)))
					defer server.Close()
					runtime, err = remote.New(server.URL, server.Client())
					require.NoError(t, err)
				}
				template := jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: "source", JobType: "child", Data: jobdb.NewTaskDataOrPanic("input")}
				source, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: template})
				require.NoError(t, err)
				var childErr error
				var atFailure [][]byte
				var before jobRow
				worker := wf.ExecutionWorker{JobType: "parent", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
					_, err := r.db.Exec(`CREATE TRIGGER child_fault BEFORE INSERT ON jobdb_chapter_chapters WHEN NEW.story_id <> 'parent' BEGIN INSERT INTO missing_infrastructure_table VALUES (NEW.story_id); END; CREATE TRIGGER child_publish_fault BEFORE INSERT ON jobdb_jobs WHEN NEW.job_id <> 'parent' BEGIN INSERT INTO missing_infrastructure_table VALUES (NEW.job_id); END`)
					require.NoError(t, err)
					defer func() {
						_, err := r.db.Exec(`DROP TRIGGER child_fault; DROP TRIGGER child_publish_fault`)
						require.NoError(t, err)
					}()
					child := template
					child.JobID = "child"
					switch mode {
					case "submit":
						_, childErr = c.SubmitJob(ctx, child)
					case "restart":
						_, childErr = c.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", CellRepo: "example.test/test", PriorJobKey: source.JobKey, JobID: "child", LastStepToKeep: 0}})
					case "append":
						child.JobID = ""
						_, childErr = c.StartOrAppendJob(ctx, jobdb.StartOrAppendJob{AppendKey: "chain", Job: child, Append: json.RawMessage(`"more"`)})
					case "invalid":
						child.JobType = ""
						_, childErr = c.SubmitJob(ctx, child)
					}
					require.Error(t, childErr)
					if mode == "invalid" {
						require.False(t, jobdb.IsExecutionInfrastructureError(childErr))
						return in, nil
					}
					require.True(t, jobdb.IsExecutionInfrastructureError(childErr), "%v", childErr)
					atFailure = rawHistory(t, r, c.GetJobKey())
					before, err = r.loadJobRow(ctx, c.GetJobKey())
					require.NoError(t, err)
					// Ignoring the failure must not admit later dispatch or apparent success.
					_, late := c.DoTask(wf.TaskCall{Site: "late", TaskType: "late", ContractVersion: 1, Command: json.RawMessage(`{}`)}, func(context.Context) (jobdb.TaskData, error) { t.Error("late dispatch admitted"); return in, nil })
					require.True(t, jobdb.IsExecutionInfrastructureError(late))
					return in, nil
				}}
				parent := template
				parent.JobID = "parent"
				parent.JobType = worker.Name()
				h, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: parent})
				require.NoError(t, err)
				run, err := wf.GetJobForRun(ctx, runtime, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: worker, WorkerID: "worker", LeaseDuration: 10 * time.Second})
				require.NoError(t, err)
				_, runErr := run.Run(nil)
				if mode == "invalid" {
					require.NoError(t, runErr)
					return
				}
				require.True(t, jobdb.IsExecutionInfrastructureError(runErr), "%v", runErr)
				var detail *jobdb.ExecutionInfrastructureError
				require.ErrorAs(t, runErr, &detail)
				require.NotNil(t, detail.JobKey)
				require.NotEqual(t, h.JobKey, *detail.JobKey, "child origin must survive parent observation")
				after, err := r.loadJobRow(ctx, h.JobKey)
				require.NoError(t, err)
				require.Equal(t, atFailure, rawHistory(t, r, h.JobKey))
				require.Equal(t, logicalJob(before), logicalJob(after))
			})
		}
	}
}
