package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	storagepb "github.com/colony-2/jobdb/pkg/internal/storagepb/v1"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func cancelledFixture(t *testing.T) (*Runtime, context.Context) {
	t.Helper()
	ctx := celltest.Context(context.Background())
	r, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close(ctx)) })
	_, err = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
	require.NoError(t, err)
	return r, ctx
}
func retainedControl(t *testing.T) jobdb.ChapterBody {
	t.Helper()
	raw, err := json.Marshal(jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "header", Versions: &jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion, JobDBVersion: "fixture"}})
	require.NoError(t, err)
	return jobdb.ExecutionChapter{Record: raw}
}

func TestCancelledRetainedChapterReads(t *testing.T) {
	for _, scenario := range []string{"grouped-start", "additional", "successor", "control", "prior-outcome", "restart-extra", "ordinary"} {
		for _, reason := range []string{"", " \t ", "first\nsecond\n"} {
			t.Run(fmt.Sprintf("%s/%q", scenario, reason), func(t *testing.T) {
				r, ctx := cancelledFixture(t)
				q := sqliteAppendRequest()
				h, err := r.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				var successor *jobdb.JobHandle
				var successorHistory [][]byte
				switch scenario {
				case "ordinary":
					h, err = r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: q.Job.Job.Data}})
					require.NoError(t, err)
				case "additional":
					_, err = r.StartOrAppendJob(ctx, q)
					require.NoError(t, err)
				case "successor", "control", "prior-outcome", "restart-extra":
					lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "batch"}}, LeaseDuration: time.Minute})
					require.NoError(t, err)
					require.NotNil(t, lease)
					if scenario == "successor" {
						next, err := r.StartOrAppendJob(ctx, q)
						require.NoError(t, err)
						require.NotEqual(t, h.JobKey, next.JobKey)
						successor = &next
						successorHistory = rawHistory(t, r, next.JobKey)
						info, err := r.GetJob(ctx, next.JobKey)
						require.NoError(t, err)
						require.Equal(t, []string{h.JobKey.JobId}, info.WaitFor)
					} else {
						ordinal := int64(1)
						if scenario == "prior-outcome" {
							require.NoError(t, r.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), Ref: jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: ordinal}, Chapter: jobdb.Chapter{Ordinal: ordinal, TaskType: "batch", Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"old result"`)}}}}}))
							ordinal++
						}
						body := retainedControl(t)
						if scenario == "restart-extra" {
							body = jobdb.RestartExtraChapter{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"extra"`)}}
						}
						require.NoError(t, r.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), Ref: jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: ordinal}, Chapter: jobdb.Chapter{Ordinal: ordinal, TaskType: "__execution__", Body: body}}))
					}
				}
				require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey, Reason: reason}))
				before := rawHistory(t, r, h.JobKey)
				row, err := r.loadJobRow(ctx, h.JobKey)
				require.NoError(t, err)
				server := httptest.NewServer(celltest.Handler(remote.NewServer(r)))
				defer server.Close()
				client, err := remote.New(server.URL, server.Client())
				require.NoError(t, err)
				for _, reader := range []jobdb.WorkflowRuntime{r, client} {
					if successor != nil {
						info, err := reader.GetJob(ctx, successor.JobKey)
						require.NoError(t, err)
						require.Equal(t, []string{h.JobKey.JobId}, info.WaitFor)
					}
					info, err := reader.GetJob(ctx, h.JobKey)
					require.NoError(t, err)
					require.Equal(t, jobdb.JobStatusCancelled, info.Status)
					require.Equal(t, "cancelled", info.CompletionStatus)
					require.Equal(t, reason, info.CompletionDetail)
					data, err := info.Data.GetData()
					require.NoError(t, err)
					require.NotEmpty(t, data)
					_, err = info.Data.GetArtifacts()
					require.NoError(t, err)
					chapters, err := reader.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
					require.NoError(t, err)
					require.Len(t, chapters, len(before))
					start, err := reader.GetChapter(ctx, jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 0})
					require.NoError(t, err)
					require.Equal(t, `"winner"`, string(start.Body.(jobdb.JobStartChapter).Input.Data))
					if scenario == "grouped-start" || scenario == "ordinary" || scenario == "successor" {
						artifacts, err := info.Data.GetArtifacts()
						require.NoError(t, err)
						require.Len(t, artifacts, 1)
						key, err := artifacts[0].ArtifactKey()
						require.NoError(t, err)
						require.Equal(t, h.JobKey.JobId, key.JobId)
						stream, err := artifacts[0].Open()
						require.NoError(t, err)
						got, err := io.ReadAll(stream)
						require.NoError(t, err)
						require.NoError(t, stream.Close())
						require.Equal(t, "saved artifact", string(got))
					}
					if scenario != "restart-extra" {
						run, err := jobdb.GetJobRun(ctx, reader, jobdb.GetJobRunRequest{JobKey: h.JobKey})
						require.NoError(t, err)
						output, err := run.GetOutput(nil, h.JobKey.TenantId)
						require.Nil(t, output)
						require.ErrorIs(t, err, jobdb.ErrJobCancelled)
						var cancelled *jobdb.JobCancelledError
						require.ErrorAs(t, err, &cancelled)
						require.Equal(t, reason, cancelled.Detail)
					}
				}
				require.Equal(t, before, rawHistory(t, r, h.JobKey))
				if successor != nil {
					require.Equal(t, successorHistory, rawHistory(t, r, successor.JobKey))
				}
				after, err := r.loadJobRow(ctx, h.JobKey)
				require.NoError(t, err)
				require.Equal(t, row, after)
			})
		}
	}
}

func TestCancelledCorruptChapterReads(t *testing.T) {
	for _, kind := range []string{"framing", "unknown", "unknown-variant", "missing-chapter", "grouped", "control", "job-outcome", "task-outcome"} {
		t.Run(kind, func(t *testing.T) {
			r, ctx := cancelledFixture(t)
			h, err := r.StartOrAppendJob(ctx, sqliteAppendRequest())
			require.NoError(t, err)
			require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey, Reason: "retained"}))
			stored := rawHistory(t, r, h.JobKey)[0]
			var record storagepb.ChapterRecord
			require.NoError(t, proto.Unmarshal(stored, &record))
			switch kind {
			case "framing":
				stored = []byte{255}
			case "unknown":
				record.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "unknown-variant":
				record.ClearJobStart()
				record.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
			case "missing-chapter":
				record.ClearJobStart()
			case "grouped":
				record.GetJobStart().GetAppend().SetData([]byte(`{"private-marker":`))
			case "control":
				record.SetExecution(&storagepb.ExecutionChapter{})
			case "job-outcome":
				record.SetJobAttemptOutcome(&storagepb.JobAttemptOutcomeChapter{})
			case "task-outcome":
				record.SetTaskAttemptOutcome(&storagepb.TaskAttemptOutcomeChapter{})
			}
			if kind != "framing" {
				stored, err = proto.Marshal(&record)
				require.NoError(t, err)
			}
			_, err = r.db.Exec(`UPDATE jobdb_chapter_chapters SET body=? WHERE story_id=? AND ordinal=0`, stored, h.JobKey.JobId)
			require.NoError(t, err)
			before := rawHistory(t, r, h.JobKey)
			info, err := r.GetJob(ctx, h.JobKey)
			require.NoError(t, err)
			_, err = info.Data.GetData()
			assertCancelledDecode(t, err, h.JobKey)
			_, err = info.Data.GetArtifacts()
			assertCancelledDecode(t, err, h.JobKey)
			server := httptest.NewServer(celltest.Handler(remote.NewServer(r)))
			defer server.Close()
			client, err := remote.New(server.URL, server.Client())
			require.NoError(t, err)
			_, err = client.GetJob(ctx, h.JobKey)
			assertCancelledDecode(t, err, h.JobKey)
			require.NotContains(t, err.Error(), "private-marker")
			require.Equal(t, before, rawHistory(t, r, h.JobKey))
		})
	}
}
func assertCancelledDecode(t *testing.T, err error, key jobdb.JobKey) {
	t.Helper()
	var failure *jobdb.ExecutionInfrastructureError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, jobdb.ExecutionInfrastructureDurableDecode, failure.Reason)
	require.Equal(t, "decode_outcome", failure.Operation)
	require.Equal(t, &key, failure.JobKey)
	require.Equal(t, int64(0), *failure.Ordinal)
}
