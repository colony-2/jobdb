package sqlite

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
)

func TestRestartStagesDatabaseBackedOutputBeforeWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), 5*time.Second)
	defer cancel()
	rt, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "reader.db")})
	require.NoError(t, err)
	defer rt.Close(context.Background())
	want := []byte("source artifact opened through SQLite")
	source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobType: "recipe", Data: jobdb.NewTaskDataOrPanic("original", jobdb.NewArtifactFromBytes("source.bin", want))}})
	require.NoError(t, err)
	output := jobdb.NewArtifact("output.bin", func() (io.ReadCloser, int64, error) {
		art, err := rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: source.JobKey, Ordinal: 0, Name: "source.bin"})
		if err != nil {
			return nil, 0, err
		}
		r, err := art.Open()
		return r, int64(len(want)), err
	}, nil)
	handle, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, LastStepToKeep: 0, ExtraTaskOutput: jobdb.NewTaskDataOrPanic("extra", output)}})
	require.NoError(t, err, "output readers must not deadlock behind the borrowed writer connection")
	art, err := rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: handle.JobKey, Ordinal: 1, Name: "output.bin"})
	require.NoError(t, err)
	r, err := art.Open()
	require.NoError(t, err)
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, want, b)
}

func TestRestartPublicationFailureRollsBackHistory(t *testing.T) {
	for _, transport := range []string{"native", "http"} {
		for _, failure := range []string{"scheduler", "goal", "append", "commit"} {
			for _, extra := range []bool{false, true} {
				if failure == "append" && !extra {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/extra=%t", transport, failure, extra), func(t *testing.T) {
					ctx := celltest.Context(context.Background())
					dir := t.TempDir()
					blobs := filepath.Join(dir, "blobs")
					local, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(dir, "restart.db"), BlobDir: blobs, MaxInlineArtifactBytes: 16})
					require.NoError(t, err)
					defer local.Close(ctx)
					var rt jobdb.WorkflowRuntime = local
					if transport == "http" {
						server := httptest.NewServer(celltest.Handler(remote.NewServer(local)))
						defer server.Close()
						rt, err = remote.New(server.URL, server.Client())
						require.NoError(t, err)
					}
					recipe := []byte("setup: 7\nadd: 2\n# source recipe retained after rollback\n")
					source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobID: "source", JobType: "recipe", Data: jobdb.NewTaskDataOrPanic("original", jobdb.NewArtifactFromBytes("submission.recipe.yaml", recipe))}})
					require.NoError(t, err)
					history, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source.JobKey})
					require.NoError(t, err)
					info, err := rt.GetJob(ctx, source.JobKey)
					require.NoError(t, err)
					beforeBlobs, err := os.ReadDir(blobs)
					require.NoError(t, err)
					require.Len(t, beforeBlobs, 1, "exercise external source artifact storage")
					req := jobdb.SubmitRestartJobRequest{RequestID: "restart", Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, JobID: "destination", LastStepToKeep: 0}}
					if extra {
						req.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic("extra", jobdb.NewArtifactFromBytes("extra.bin", []byte(strings.Repeat("extra", 32))))
					}
					var goalCount int
					require.NoError(t, local.db.QueryRowContext(ctx, "SELECT count(*) FROM jobdb_jobs WHERE goal_origin=1").Scan(&goalCount))
					trigger := ""
					switch failure {
					case "scheduler":
						trigger = `CREATE TRIGGER fail_restart BEFORE INSERT ON jobdb_jobs WHEN NEW.job_id='destination' BEGIN SELECT RAISE(ABORT, 'injected publication failure'); END`
					case "goal":
						trigger = `CREATE TRIGGER fail_restart BEFORE UPDATE OF goal_origin ON jobdb_jobs WHEN NEW.job_id='destination' AND NEW.goal_origin=1 BEGIN SELECT RAISE(ABORT, 'injected goal failure'); END`
					case "append":
						trigger = `CREATE TRIGGER fail_restart BEFORE INSERT ON jobdb_chapter_artifacts WHEN NEW.story_id='destination' BEGIN SELECT RAISE(ABORT, 'injected append failure'); END`
					case "commit":
						// A deferred FK fails only at the outer transaction's commit,
						// after successful history, blob, goal and scheduler staging.
						_, err = local.db.ExecContext(ctx, `CREATE TABLE restart_parent(id INTEGER PRIMARY KEY); CREATE TABLE restart_commit_probe(id INTEGER REFERENCES restart_parent(id) DEFERRABLE INITIALLY DEFERRED)`)
						require.NoError(t, err)
						trigger = `CREATE TRIGGER fail_restart AFTER INSERT ON jobdb_jobs WHEN NEW.job_id='destination' BEGIN INSERT INTO restart_commit_probe VALUES(1); END`
					}
					_, err = local.db.ExecContext(ctx, trigger)
					require.NoError(t, err)
					for attempt := 0; attempt < 2; attempt++ {
						_, err = rt.SubmitRestartJob(ctx, req)
						require.Error(t, err)
						require.True(t, jobdb.IsExecutionInfrastructureError(err))
						if transport == "http" {
							var cause *jobdb.RemoteExecutionCause
							require.ErrorAs(t, err, &cause)
							require.NotContains(t, err.Error(), "injected")
							require.NotContains(t, err.Error(), "FOREIGN KEY")
						} else if failure == "commit" {
							require.Contains(t, err.Error(), "FOREIGN KEY")
						} else {
							require.Contains(t, err.Error(), "injected")
						}
						// Public access is guarded even when private history leaks;
						// inspect all destination storage as well as public reads.
						for table, key := range map[string]string{"jobdb_jobs": "job_id", "jobdb_goal_preparations": "job_id", "jobdb_submission_receipts": "job_id", "jobdb_chapter_stories": "story_id", "jobdb_chapter_chapters": "story_id", "jobdb_chapter_artifacts": "story_id"} {
							var count int
							require.NoError(t, local.db.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE "+key+"='destination'").Scan(&count))
							require.Zero(t, count, table)
						}
						var gotGoals int
						require.NoError(t, local.db.QueryRowContext(ctx, "SELECT count(*) FROM jobdb_jobs WHERE goal_origin=1").Scan(&gotGoals))
						require.Equal(t, goalCount, gotGoals)
						gotBlobs, err := os.ReadDir(blobs)
						require.NoError(t, err)
						require.Equal(t, beforeBlobs, gotBlobs, "discard new output blobs; keep inherited blobs")
						_, err = rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "destination"}, Ordinal: 0})
						require.ErrorIs(t, err, jobdb.ErrJobNotFound)
					}
					_, err = local.db.ExecContext(ctx, "DROP TRIGGER fail_restart")
					require.NoError(t, err)
					destination, err := rt.SubmitRestartJob(ctx, req)
					require.NoError(t, err)
					again, err := rt.SubmitRestartJob(ctx, req)
					require.NoError(t, err)
					require.Equal(t, destination, again)
					got, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: destination.JobKey, Ordinal: 0})
					require.NoError(t, err)
					require.Equal(t, history[0], got)
					read := func(key jobdb.JobKey, ordinal int64, name string) []byte {
						a, err := rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: key, Ordinal: ordinal, Name: name})
						require.NoError(t, err)
						r, err := a.Open()
						require.NoError(t, err)
						b, err := io.ReadAll(r)
						require.NoError(t, err)
						require.NoError(t, r.Close())
						return b
					}
					require.Equal(t, recipe, read(destination.JobKey, 0, "submission.recipe.yaml"))
					require.Equal(t, recipe, read(source.JobKey, 0, "submission.recipe.yaml"))
					if extra {
						require.Equal(t, strings.Repeat("extra", 32), string(read(destination.JobKey, 1, "extra.bin")))
					}
					req.Job.LastStepToKeep = 1
					_, err = rt.SubmitRestartJob(ctx, req)
					require.Error(t, err, "conflicting request cannot change the published destination")
					require.Equal(t, recipe, read(destination.JobKey, 0, "submission.recipe.yaml"))
					after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source.JobKey})
					require.NoError(t, err)
					require.Equal(t, history, after)
					afterInfo, err := rt.GetJob(ctx, source.JobKey)
					require.NoError(t, err)
					require.Equal(t, info, afterInfo)
				})
			}
		}
	}
}
