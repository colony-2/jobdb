package runtimecore_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/colony-2/jobdb/pkg/internal/executionmeta"
	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type restartPrefixReader []jobdb.Chapter

func (r restartPrefixReader) ListChapters(context.Context, jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	return r, nil
}
func (r restartPrefixReader) GetChapter(_ context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	for _, c := range r {
		if c.Ordinal == ref.Ordinal {
			return c, nil
		}
	}
	return jobdb.Chapter{}, jobdb.ErrChapterNotFound
}

// Model both ordinary compact prefixes and the legacy chain in the bug report:
// an old continuation embedded in an earlier restart descriptor, followed by
// another restart marker and a declaration carrying the stale timeout.
func TestExecutionRestartCutsBeforeSupersededContinuation(t *testing.T) {
	for _, location := range []string{"prefix", "restart-descriptor", "chapter"} {
		for _, validation := range []string{"valid", "missing", "ambiguous", "no-handler"} {
			t.Run(location+"/"+validation, func(t *testing.T) {
				marshal := func(v any) json.RawMessage { raw, err := json.Marshal(v); require.NoError(t, err); return raw }
				record := func(kind, id string) jobdb.ExecutionRecord {
					r := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: kind, ID: id}
					switch kind {
					case "header", "activation":
						r.Versions = &jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion, JobDBVersion: "test"}
					case "attempt":
						r.Attempt = 1
					case "continuation":
						r.Payload = json.RawMessage(`{}`)
					case "result", "admission":
						r.Output = &jobdb.ExecutionSnapshot{Data: json.RawMessage(`true`)}
					case "restart":
						r.Source = &jobdb.JobKey{TenantId: "tenant", JobId: "earlier"}
					}
					return r
				}
				chapter := func(ordinal int64, rec jobdb.ExecutionRecord, prefix ...jobdb.ExecutionRecord) jobdb.Chapter {
					return jobdb.Chapter{Ordinal: ordinal, Body: jobdb.ExecutionChapter{Record: marshal(rec)}, Metadata: jobdb.ChapterMetadata{Fields: map[string]jobdb.ChapterMetadataValue{
						"metadata": {Kind: jobdb.ChapterMetadataMap, Map: map[string]jobdb.ChapterMetadataValue{executionmeta.Key: {Kind: jobdb.ChapterMetadataString, String: string(marshal(prefix))}}},
					}}}
				}
				target := `/scope:"$job"#1/attempt:1/scope:"target"#1`
				ancestor := record("attempt", `/scope:"$job"#1/attempt:1`)
				// A different boundary's continuation is valid prefix history and must stay.
				other := record("continuation", "earlier-boundary")
				stale := record("continuation", target)
				admission := record("admission", target+"/restart")
				declaration := record("scope", target)
				declaration.Descriptor = json.RawMessage(`{"restart":true,"total_timeout":60000000000}`)
				if validation == "no-handler" {
					declaration.Descriptor = json.RawMessage(`{"restart":false}`)
				}
				history := restartPrefixReader{
					chapter(1, record("header", "")),
					chapter(2, record("result", "completed-earlier-task")),
					chapter(3, record("restart", target)),
				}
				switch location {
				case "prefix":
					history = append(history, chapter(4, admission, ancestor, other, stale))
				case "restart-descriptor":
					older := record("restart", target)
					older.Descriptor = marshal([]jobdb.ExecutionRecord{ancestor, other, stale, admission})
					history = append(history, chapter(4, older), chapter(5, record("activation", "")), chapter(6, record("restart", target)))
				case "chapter":
					history = append(history, chapter(4, stale, ancestor, other), chapter(5, admission))
				}
				if validation != "missing" {
					history = append(history, chapter(10, record("result", target+"/attempt:1"), declaration))
				}
				if validation == "ambiguous" {
					history = append(history, chapter(11, record("result", target+"/attempt:1"), declaration))
				}
				original := marshal(history)
				request := jobdb.SubmitRestartJob{PriorJobKey: jobdb.JobKey{TenantId: "tenant", JobId: "old-chain"}, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: target, Payload: json.RawMessage(`"new-payload"`)}}
				resolved, restart, err := runtimecore.ResolveExecutionRestart(context.Background(), history, request)
				require.Equal(t, original, marshal(history), "source history is immutable")
				if validation != "valid" {
					require.Error(t, err)
					require.Nil(t, restart)
					switch validation {
					case "missing":
						require.ErrorContains(t, err, "boundary not found")
					case "ambiguous":
						require.ErrorContains(t, err, "ambiguous")
					case "no-handler":
						require.ErrorContains(t, err, "no restart handler")
					}
					return
				}
				require.NoError(t, err)
				require.EqualValues(t, 3, resolved.LastStepToKeep, "cut at the old continuation's chapter, not the later declaration")
				require.JSONEq(t, string(marshal([]jobdb.ExecutionRecord{ancestor, other})), string(restart.Descriptor))
				require.Equal(t, request.ExecutionRestart.Payload, restart.Payload)
				require.Equal(t, request.PriorJobKey, *restart.Source)
			})
		}
	}
}
