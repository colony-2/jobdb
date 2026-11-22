package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	storagepb "github.com/colony-2/jobdb/pkg/internal/storagepb/v1"
	"google.golang.org/protobuf/proto"
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

func rawHistory(t *testing.T, r *Runtime, key jobdb.JobKey) [][]byte {
	t.Helper()
	rows, err := r.db.Query(`SELECT body FROM jobdb_chapter_chapters WHERE anthology_id=? AND story_id=? ORDER BY ordinal`, key.TenantId, key.JobId)
	require.NoError(t, err)
	defer rows.Close()
	out := [][]byte{}
	for rows.Next() {
		var b []byte
		require.NoError(t, rows.Scan(&b))
		out = append(out, b)
	}
	require.NoError(t, rows.Err())
	return out
}
func logicalJob(row jobRow) jobRow {
	row.leaseID.Valid = false
	row.leaseID.String = ""
	row.leaseWorkerID.Valid = false
	row.leaseWorkerID.String = ""
	row.leaseExpiresAtNS.Valid = false
	row.leaseExpiresAtNS.Int64 = 0
	row.updatedAtNS = 0
	row.crashBudgetRemaining = 5
	return row
}
func TestInfrastructureSameJobCrashConcernRecovery(t *testing.T) {
	for _, httpPath := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "http"}[httpPath], func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			cfg := Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")}
			r, err := NewFromConfig(ctx, cfg)
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close(ctx)) }()
			var runtime jobdb.WorkflowRuntime = r
			var server *httptest.Server
			defer func() {
				if server != nil {
					server.Close()
				}
			}()
			if httpPath {
				server = httptest.NewServer(celltest.Handler(remote.NewServer(r)))
				runtime, err = remote.New(server.URL, server.Client())
				require.NoError(t, err)
			}
			prerequisite := wf.ExecutionWorker{JobType: "prerequisite", Execute: func(_ wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) { return in, nil }}
			dependency, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: prerequisite.Name(), Data: jobdb.NewTaskDataOrPanic("prerequisite")}})
			require.NoError(t, err)
			dependencyRun, err := wf.GetJobForRun(ctx, runtime, wf.GetJobForRunRequest{JobKey: dependency.JobKey, JobWorker: prerequisite, WorkerID: "dependency"})
			require.NoError(t, err)
			_, err = dependencyRun.Run(nil)
			require.NoError(t, err)
			calls := 0
			worker := wf.ExecutionWorker{JobType: "recover", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) { calls++; return in, nil }}
			at := time.Now().Add(-time.Hour)
			h, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobID: "same-job", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic("input"), AvailableAt: &at, Metadata: json.RawMessage(`{"identity":"preserve"}`), Prerequisites: []jobdb.JobPrerequisite{{JobID: dependency.JobKey.JobId, Condition: jobdb.JobPrereqSuccess}}}})
			require.NoError(t, err)
			lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "seed", Routes: []jobdb.Route{{JobType: worker.Name()}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			rec := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "header", At: time.Now().UTC(), Versions: &jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion, JobDBVersion: "recorded-jobdb", ConsumerVersions: map[string]string{"c2j": "recorded-consumer"}}}
			raw, err := json.Marshal(rec)
			require.NoError(t, err)
			token := ""
			if v, ok := lease.(interface{ LeaseToken() string }); ok {
				token = v.LeaseToken()
			}
			require.NoError(t, runtime.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: token, Ref: jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 1}, Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "__execution__", CreatedAt: rec.At, Body: jobdb.ExecutionChapter{Record: raw}}}))
			original := rawHistory(t, r, h.JobKey)
			_, err = r.db.Exec(`UPDATE jobdb_chapter_chapters SET body=? WHERE story_id=? AND ordinal=1`, []byte{0xff}, h.JobKey.JobId)
			require.NoError(t, err)
			// The seed is an intentional setup claim. Reset only its authority before the
			// six failing runner invocations; subsequent expiries are all real runs.
			_, err = r.db.Exec(`UPDATE jobdb_jobs SET lease_id=NULL,lease_worker_id=NULL,lease_expires_at_ns=NULL WHERE job_id=?`, h.JobKey.JobId)
			require.NoError(t, err)
			before, err := r.loadJobRow(ctx, h.JobKey)
			require.NoError(t, err)
			corrupt := rawHistory(t, r, h.JobKey)
			var last jobdb.ExecutionLease
			for n := 0; n < 6; n++ {
				runnable, err := wf.GetJobForRun(ctx, runtime, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: worker, WorkerID: "failing", LeaseDuration: time.Minute, BeforeRun: func(_ context.Context, l jobdb.ExecutionLease) error { last = l; return nil }})
				require.NoError(t, err)
				require.True(t, runnable.LeaseAcquired())
				_, err = runnable.Run(nil)
				require.True(t, jobdb.IsExecutionInfrastructureError(err), "%v", err)
				require.Equal(t, corrupt, rawHistory(t, r, h.JobKey))
				after, err := r.loadJobRow(ctx, h.JobKey)
				require.NoError(t, err)
				require.Equal(t, logicalJob(before), logicalJob(after))
				_, err = r.db.Exec(`UPDATE jobdb_jobs SET lease_expires_at_ns=0 WHERE job_id=?`, h.JobKey.JobId)
				require.NoError(t, err)
			}
			require.Zero(t, calls)
			for n := 0; n < 3; n++ {
				info, err := runtime.GetJob(ctx, h.JobKey)
				require.NoError(t, err)
				require.Equal(t, jobdb.JobStatusCrashConcern, info.Status)
			}
			blocked, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "blocked", Routes: []jobdb.Route{{JobType: worker.Name()}}})
			require.NoError(t, err)
			require.Nil(t, blocked)
			// Close and reopen the durable backend before operator repair. The
			// same failed job, goals, dependency and byte history must survive.
			if server != nil {
				server.Close()
				server = nil
			}
			require.NoError(t, r.Close(ctx))
			r, err = NewFromConfig(ctx, cfg)
			require.NoError(t, err)
			runtime = r
			if httpPath {
				server = httptest.NewServer(celltest.Handler(remote.NewServer(r)))
				runtime, err = remote.New(server.URL, server.Client())
				require.NoError(t, err)
			}
			require.Equal(t, corrupt, rawHistory(t, r, h.JobKey))
			reloaded, err := r.loadJobRow(ctx, h.JobKey)
			require.NoError(t, err)
			require.Equal(t, logicalJob(before), logicalJob(reloaded))
			_, err = r.db.Exec(`UPDATE jobdb_chapter_chapters SET body=? WHERE story_id=? AND ordinal=1`, original[1], h.JobKey.JobId)
			require.NoError(t, err)
			require.NoError(t, runtime.ClearCrashConcern(ctx, jobdb.ClearCrashConcernRequest{JobKey: h.JobKey}))
			_, err = r.ValidateExecutionLeaseByID(ctx, h.JobKey, last.LeaseID(), "failing")
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			runnable, err := wf.GetJobForRun(ctx, runtime, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: worker, WorkerID: "repaired"})
			require.NoError(t, err)
			out, err := runnable.Run(nil)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			require.Equal(t, 1, calls)
			after := rawHistory(t, r, h.JobKey)
			require.Equal(t, original, after[:len(original)])
			info, err := runtime.GetJob(ctx, h.JobKey)
			require.NoError(t, err)
			require.Equal(t, jobdb.JobStatusCompleted, info.Status)
			require.Equal(t, h.GoalID, info.GoalID)
		})
	}
}

func TestInfrastructureCorruptOwnedRecordsPreserveState(t *testing.T) {
	for _, httpPath := range []bool{false, true} {
		for _, kind := range []string{"execution_protocol", "reconstruction", "task_error_envelope", "terminal_result"} {
			t.Run(fmt.Sprintf("http=%v/%s", httpPath, kind), func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				r, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")})
				require.NoError(t, err)
				defer r.Close(ctx)
				var runtime jobdb.WorkflowRuntime = r
				if httpPath {
					s := httptest.NewServer(celltest.Handler(remote.NewServer(r)))
					defer s.Close()
					runtime, err = remote.New(s.URL, s.Client())
					require.NoError(t, err)
				}
				worker := wf.ExecutionWorker{JobType: "corrupt", Execute: func(_ wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) { return in, nil }}
				h, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", CellRepo: "example.test/test", JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic("input")}})
				require.NoError(t, err)
				if kind == "terminal_result" {
					run, err := wf.GetJobForRun(ctx, runtime, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: worker, WorkerID: "seed"})
					require.NoError(t, err)
					_, err = run.Run(nil)
					require.NoError(t, err)
				} else {
					lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "seed", Routes: []jobdb.Route{{JobType: worker.Name()}}})
					require.NoError(t, err)
					rec := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "header", At: time.Now(), Versions: &jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion, JobDBVersion: "stored"}}
					raw, err := json.Marshal(rec)
					require.NoError(t, err)
					token := ""
					if v, ok := lease.(interface{ LeaseToken() string }); ok {
						token = v.LeaseToken()
					}
					require.NoError(t, runtime.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: token, Ref: jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 1}, Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "__execution__", CreatedAt: rec.At, Body: jobdb.ExecutionChapter{Record: raw}}}))
					_, err = r.db.Exec(`UPDATE jobdb_jobs SET lease_id=NULL,lease_worker_id=NULL,lease_expires_at_ns=NULL WHERE job_id=?`, h.JobKey.JobId)
					require.NoError(t, err)
				}
				history := rawHistory(t, r, h.JobKey)
				ordinal := int64(len(history) - 1)
				var stored storagepb.ChapterRecord
				require.NoError(t, proto.Unmarshal(history[ordinal], &stored))
				switch kind {
				case "execution_protocol":
					stored.GetExecution().GetRecord().SetVersion(0)
				case "reconstruction":
					bad := &storagepb.ExecutionRecord{}
					bad.SetKind("unknown-retained-kind")
					stored.SetReconstruction([]*storagepb.ExecutionRecord{bad})
				case "task_error_envelope":
					stored.SetTaskAttemptOutcome(&storagepb.TaskAttemptOutcomeChapter{})
				case "terminal_result":
					stored.SetJobAttemptOutcome(&storagepb.JobAttemptOutcomeChapter{})
				}
				corrupted, err := proto.Marshal(&stored)
				require.NoError(t, err)
				_, err = r.db.Exec(`UPDATE jobdb_chapter_chapters SET body=? WHERE story_id=? AND ordinal=?`, corrupted, h.JobKey.JobId, ordinal)
				require.NoError(t, err)
				before, err := r.loadJobRow(ctx, h.JobKey)
				require.NoError(t, err)
				saved := rawHistory(t, r, h.JobKey)
				run, err := wf.GetJobForRun(ctx, runtime, wf.GetJobForRunRequest{JobKey: h.JobKey, JobWorker: worker, WorkerID: "repair-check"})
				if err == nil {
					_, err = run.Run(nil)
				}
				require.True(t, jobdb.IsExecutionInfrastructureError(err), "%v", err)
				var failure *jobdb.ExecutionInfrastructureError
				require.ErrorAs(t, err, &failure)
				require.Equal(t, &h.JobKey, failure.JobKey)
				require.Equal(t, &ordinal, failure.Ordinal)
				require.Equal(t, saved, rawHistory(t, r, h.JobKey))
				after, err := r.loadJobRow(ctx, h.JobKey)
				require.NoError(t, err)
				require.Equal(t, logicalJob(before), logicalJob(after))
			})
		}
	}
}
