package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/internal/executionmeta"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
	modernsqlite "modernc.org/sqlite"
)

func TestRestartLinksLegacyHistoryWithoutRevalidation(t *testing.T) {
	for _, transport := range []string{"local", "remote"} {
		t.Run(transport, func(t *testing.T) {
			ctx := context.Background()
			counted := &listCountingConnector{dsn: filepath.Join(t.TempDir(), "restart.db"), drv: &modernsqlite.Driver{}}
			db := sql.OpenDB(counted)
			defer db.Close()
			local, err := New(db, WithMaxChapterApplicationPayloadBytes(2*1024*1024))
			require.NoError(t, err)
			defer local.Close(ctx)
			source, err := local.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", JobType: "legacy", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			leases, err := local.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "writer", Routes: []jobdb.Route{{JobType: "legacy"}}, Limit: 1})
			require.NoError(t, err)
			require.Len(t, leases, 1)
			large, err := json.Marshal(map[string]string{"recipe_yaml": strings.Repeat("x", 1260320)})
			require.NoError(t, err)
			for ordinal := int64(1); ordinal <= 43; ordinal++ {
				data := json.RawMessage(`null`)
				if ordinal == 3 {
					data = large
				}
				record := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "result", ID: "root-source", Attempt: 1, Output: &jobdb.ExecutionSnapshot{Data: data}}
				if ordinal == 1 {
					record = jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "header", Versions: &jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion, JobDBVersion: "legacy"}}
				}
				raw, err := json.Marshal(record)
				require.NoError(t, err)
				chapter := jobdb.Chapter{Ordinal: ordinal, TaskType: "__execution__", Body: jobdb.ExecutionChapter{Record: raw}}
				if ordinal == 43 {
					prefix, err := json.Marshal([]jobdb.ExecutionRecord{{Version: jobdb.ExecutionPatternVersion, Kind: "scope", ID: "restart-target", Descriptor: json.RawMessage(`{"restart":true}`)}})
					require.NoError(t, err)
					chapter.Metadata = jobdb.ChapterMetadata{Fields: map[string]jobdb.ChapterMetadataValue{
						"metadata": {Kind: jobdb.ChapterMetadataMap, Map: map[string]jobdb.ChapterMetadataValue{
							executionmeta.Key: {Kind: jobdb.ChapterMetadataString, String: string(prefix)},
						}},
					}}
				}
				require.NoError(t, local.PutChapter(ctx, jobdb.PutChapterRequest{Ref: jobdb.ChapterRef{JobKey: source.JobKey, Ordinal: ordinal}, LeaseID: leases[0].LeaseID(), Chapter: chapter}))
			}
			// Simulate reopening legacy history with today's default write limit.
			local.maxChapterApplicationPayloadBytes = 0
			var rt jobdb.WorkflowRuntime = local
			if transport == "remote" {
				server := httptest.NewServer(remote.NewServer(local))
				defer server.Close()
				rt, err = remote.New(server.URL, server.Client())
				require.NoError(t, err)
			}
			// The new schema deliberately excludes every retained chapter.
			schema, err := local.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":{"properties":{"ordinal":{"minimum":43}}}}`)})
			require.NoError(t, err)
			var shortQueries int64
			for _, last := range []int64{4, 42} {
				req := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source.JobKey, JobID: fmt.Sprintf("restart-%d", last), LastStepToKeep: last, Schema: &jobdb.JobSchemaSelector{Hash: schema.SchemaHash}}}
				counted.queries.Store(0)
				clone, err := rt.SubmitRestartJob(ctx, req)
				queries := counted.queries.Load()
				require.NoError(t, err)
				if last == 4 {
					shortQueries = queries
				} else {
					require.Equal(t, shortQueries, queries, "clone query count must not grow with prefix length")
				}
				chapter, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: clone.JobKey, Ordinal: 3})
				require.NoError(t, err)
				record, err := jobdb.DecodeExecutionRecord(chapter.Body.(jobdb.ExecutionChapter).Record)
				require.NoError(t, err)
				require.JSONEq(t, string(large), string(record.Output.Data))
				var rows int
				require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_chapter_chapters WHERE story_id=?`, clone.JobKey.JobId).Scan(&rows))
				require.Zero(t, rows, "thin clones must not copy retained chapter rows")
				var base string
				var fork int64
				require.NoError(t, db.QueryRowContext(ctx, `SELECT base_story_id,base_fork_ordinal FROM jobdb_chapter_stories WHERE story_id=?`, clone.JobKey.JobId).Scan(&base, &fork))
				require.Equal(t, source.JobKey.JobId, base)
				require.Equal(t, last, fork)
				again, err := rt.SubmitRestartJob(ctx, req)
				require.NoError(t, err)
				require.Equal(t, clone, again)
			}
			logical := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source.JobKey, JobID: "logical", ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: "restart-target"}, Schema: &jobdb.JobSchemaSelector{Hash: schema.SchemaHash}}}
			clone, err := rt.SubmitRestartJob(ctx, logical)
			require.NoError(t, err)
			var localRows int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_chapter_chapters WHERE story_id=?`, clone.JobKey.JobId).Scan(&localRows))
			require.Equal(t, 1, localRows, "only the new restart marker is stored")
			logical.Job.JobID = "invalid-logical"
			logical.Job.Schema = &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{"chapterShape":false}`)}
			_, err = rt.SubmitRestartJob(ctx, logical)
			require.ErrorIs(t, err, jobdb.ErrJobSchemaValidation)
			logical.Job.ExecutionRestart.Payload = large
			_, err = rt.SubmitRestartJob(ctx, logical)
			require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge)

			req := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: source.JobKey, JobID: "extra", LastStepToKeep: 42, Schema: &jobdb.JobSchemaSelector{Hash: schema.SchemaHash}, ExtraTaskOutput: jobdb.NewTaskDataOrPanic(json.RawMessage(large))}}
			_, err = rt.SubmitRestartJob(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrChapterApplicationPayloadTooLarge, "new output still obeys the write limit")
			req.Job.ExtraTaskOutput = jobdb.NewTaskDataOrPanic("small")
			req.Job.Schema = &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{"chapterShape":false}`)}
			_, err = rt.SubmitRestartJob(ctx, req)
			require.ErrorIs(t, err, jobdb.ErrJobSchemaValidation, "new output still obeys the schema")
			req.Job.JobID = "valid-extra"
			req.Job.Schema = &jobdb.JobSchemaSelector{Hash: schema.SchemaHash}
			_, err = rt.SubmitRestartJob(ctx, req)
			require.NoError(t, err)
		})
	}
}
