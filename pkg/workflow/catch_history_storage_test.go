package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// Normalize only run-specific record values, retaining field presence, unknown
// fields, zero timestamps, payloads, policy, identities and record ordering.
// Use generic JSON rather than ExecutionRecord so added storage fields fail.
func catchStorageRecord(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var record map[string]any
	require.NoError(t, json.Unmarshal(raw, &record))
	if at, ok := record["at"]; ok {
		stamp, err := time.Parse(time.RFC3339Nano, at.(string))
		require.NoError(t, err)
		require.False(t, stamp.IsZero())
		record["at"] = "<recorded-time>"
	}
	if activation, ok := record["activation"]; ok {
		require.NotEmpty(t, activation)
		record["activation"] = "<activation>"
	}
	if versions, ok := record["versions"].(map[string]any); ok {
		if version, present := versions["jobdb_version"]; present {
			require.NotEmpty(t, version)
			versions["jobdb_version"] = "<build-version>"
		}
	}
	return record
}

// Freeze the physical writer contract, including compact metadata. Deliberately
// do not decode through GetJobRun here: a compatible reader could hide a writer
// format change. This test also runs on the pre-feature revision without the API.
func TestCatchHistoryStorageWriterCompatibility(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			w := wf.ExecutionWorker{JobType: "fixture", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("caught", wf.ScopeOptions{Catch: []wf.CatchRule{{ID: "recover", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
					return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("recovered")}, nil
				}}}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("original") })
			}}
			handle, err := recording.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "fixture", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic("input")}})
			require.NoError(t, err)
			run, err := wf.GetJobForRun(context.Background(), recording, wf.GetJobForRunRequest{JobKey: handle.JobKey, JobWorker: w, WorkerID: "fixture", LeaseDuration: time.Minute})
			require.NoError(t, err)
			outcome, err := run.Run(nil)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, outcome.Status)
			chapters, err := inspection.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			type physical struct {
				Record   json.RawMessage
				Metadata jobdb.ChapterMetadata
			}
			var actual []physical
			for _, chapter := range chapters {
				if body, ok := chapter.Body.(jobdb.ExecutionChapter); ok {
					actual = append(actual, physical{body.Record, chapter.Metadata})
				}
			}
			raw, err := os.ReadFile("testdata/catch-v4/storage-sqlite.json")
			require.NoError(t, err)
			var expected []physical
			require.NoError(t, json.Unmarshal(raw, &expected))
			require.Len(t, actual, len(expected), "physical execution chapter count must not change")
			for i := range expected {
				require.Equal(t, catchStorageRecord(t, expected[i].Record), catchStorageRecord(t, actual[i].Record), "physical record %d", i)
				// Compact prefix JSON is a storage string inside typed metadata. Normalize
				// only record volatility there; preserve the metadata wrapper and key.
				normalizeMetadata := func(metadata jobdb.ChapterMetadata) any {
					raw, err := json.Marshal(metadata)
					require.NoError(t, err)
					var object map[string]any
					require.NoError(t, json.Unmarshal(raw, &object))
					if fields, ok := object["Fields"].(map[string]any); ok && len(fields) > 0 {
						if created, ok := fields["created_at"].(map[string]any); ok {
							stamp, err := time.Parse(time.RFC3339Nano, created["String"].(string))
							require.NoError(t, err)
							require.False(t, stamp.IsZero())
							created["String"] = "<chapter-time>"
						}
						wrapper, exists := fields["metadata"].(map[string]any)
						if !exists {
							return object
						}
						packed := wrapper["Map"].(map[string]any)["__execution_prefix_v3"].(map[string]any)
						var records []json.RawMessage
						require.NoError(t, json.Unmarshal([]byte(packed["String"].(string)), &records))
						normalized := make([]map[string]any, len(records))
						for j, rec := range records {
							normalized[j] = catchStorageRecord(t, rec)
						}
						data, err := json.Marshal(normalized)
						require.NoError(t, err)
						packed["String"] = string(data)
					}
					return object
				}
				require.Equal(t, normalizeMetadata(expected[i].Metadata), normalizeMetadata(actual[i].Metadata), "physical metadata %d", i)
			}
		})
	}
}
