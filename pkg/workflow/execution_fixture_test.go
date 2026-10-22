package workflow_test

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// The compact protocol is an explicit hard cutover. Keep the old fixture to
// verify rejection before either replay or continuation invokes consumer code.
func TestExecutionPreCompactHistoryRejected(t *testing.T) {
	raw, err := os.ReadFile("testdata/execution-v1.json")
	require.NoError(t, err)
	var records []jobdb.ExecutionRecord
	require.NoError(t, json.Unmarshal(raw, &records))
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		for _, version := range []uint32{1, 2} {
			t.Run(backend+string(rune('0'+version)), func(t *testing.T) {
				rt := alternateRuntime(t, backend)
				var calls atomic.Int32
				w := wf.ExecutionWorker{JobType: "old-fixture", Execute: func(wf.ExecutionContext, jobdb.JobData) (jobdb.JobData, error) { calls.Add(1); return nil, nil }}
				key := submitExecutionTest(t, rt, w)
				ctx := context.Background()
				lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "fixture", Routes: []jobdb.Route{{JobType: w.Name()}}, LeaseDuration: time.Minute})
				require.NoError(t, err)
				token := ""
				if l, ok := lease.(interface{ LeaseToken() string }); ok {
					token = l.LeaseToken()
				}
				header := records[0]
				header.Version = version
				versions := *header.Versions
				versions.ExecutionPatternVersion = version
				header.Versions = &versions
				body, err := json.Marshal(header)
				require.NoError(t, err)
				require.NoError(t, rt.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: token, Ref: jobdb.ChapterRef{JobKey: key, Ordinal: 1}, Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "__execution__", CreatedAt: header.At, Body: jobdb.ExecutionChapter{Record: body}}}))
				require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: w.Name()}}))
				before, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				_, err = runExecutionTest(t, rt, key, w)
				require.ErrorContains(t, err, "compact-history cutover")
				_, err = replayExecutionTest(t, rt, key, w)
				require.ErrorContains(t, err, "compact-history cutover")
				require.Zero(t, calls.Load())
				after, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Equal(t, before, after)
			})
		}
	}
}
