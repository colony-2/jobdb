package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func catchFullFixture(t *testing.T) []jobdb.ExecutionRecord {
	t.Helper()
	raw, err := os.ReadFile("testdata/catch-v4/full.json")
	require.NoError(t, err)
	var records []jobdb.ExecutionRecord
	require.NoError(t, json.Unmarshal(raw, &records))
	return records
}
func catchFixtureRuntime(t *testing.T, records []jobdb.ExecutionRecord) (jobdb.WorkflowRuntime, jobdb.JobKey) {
	t.Helper()
	rt := toy.New()
	w := wf.ExecutionWorker{JobType: "fixture"}
	key := submitExecutionTest(t, rt, w)
	start, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	for _, rec := range records {
		raw, err := json.Marshal(rec)
		require.NoError(t, err)
		start = append(start, jobdb.Chapter{Ordinal: int64(len(start)), TaskType: "__execution__", CreatedAt: rec.At, Body: jobdb.ExecutionChapter{Record: raw}})
	}
	return catchFrozenRuntime{WorkflowRuntime: rt, chapters: start}, key
}

func TestCatchHistoryFixtures(t *testing.T) {
	full := catchFullFixture(t)
	rt, key := catchFixtureRuntime(t, full)
	expected := readCatches(t, rt, key)
	require.Len(t, expected.Attempts, 1)
	a := expected.Attempts[0]
	require.Equal(t, "caught", a.Site)
	require.Equal(t, "recover", a.RuleID)
	require.Equal(t, wf.CatchHandlingAccepted, a.Handling)
	require.Equal(t, wf.CatchHandled, a.Accepted.Disposition)
	require.Nil(t, a.Accepted.Failure)
	require.JSONEq(t, `"recovered"`, string(a.Accepted.Output.Data))
	require.Contains(t, string(a.Original.Data), "original")
	require.False(t, a.Retry.Retry)
	raw, err := os.ReadFile("testdata/catch-v4/compact.json")
	require.NoError(t, err)
	var compact []struct {
		Record   json.RawMessage
		Metadata jobdb.ChapterMetadata
	}
	require.NoError(t, json.Unmarshal(raw, &compact))
	// Reuse the same seeded JobKey and compare independently read physical forms.
	prefix, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	prefix = prefix[:1]
	for _, ch := range compact {
		prefix = append(prefix, jobdb.Chapter{Ordinal: int64(len(prefix)), TaskType: "__execution__", Body: jobdb.ExecutionChapter{Record: ch.Record}, Metadata: ch.Metadata})
	}
	// The embedded runtime is used only for the seed's summary and artifacts.
	seed := rt.(catchFrozenRuntime).WorkflowRuntime
	require.Equal(t, expected, readCatches(t, catchFrozenRuntime{WorkflowRuntime: seed, chapters: prefix}, key))
}

func TestCatchHistoryFixturePrefixesAndGaps(t *testing.T) {
	for _, mode := range []string{"result-only", "selection-only", "ancestor-success", "handled-only-prefix", "missing-origin", "missing-selection", "no-control", "ambiguous-replacement", "proven-fast-path", "omitted-policy", "unspecified-policy"} {
		t.Run(mode, func(t *testing.T) {
			var records []jobdb.ExecutionRecord
			full := catchFullFixture(t)
			for _, rec := range full {
				if rec.Kind == "result" && rec.ErrorKind == "" && mode != "ancestor-success" {
					continue
				}
				switch mode {
				case "result-only", "no-control", "proven-fast-path", "omitted-policy", "unspecified-policy":
					if rec.Kind == "catch" || rec.Kind == "handled" || rec.Kind == "decision" {
						continue
					}
				case "selection-only", "ancestor-success":
					if rec.Kind == "handled" || rec.Kind == "decision" {
						continue
					}
				case "handled-only-prefix":
					if rec.Kind == "decision" {
						continue
					}
				case "missing-origin":
					if rec.Kind == "result" {
						continue
					}
				case "missing-selection":
					if rec.Kind == "catch" {
						continue
					}
				case "ambiguous-replacement":
					if rec.Kind == "handled" {
						rec.ErrorKind = "AppError"
						rec.Error = json.RawMessage(`{"message":"original"}`)
						rec.Disposition = string(wf.CatchPropagate)
					}
					if rec.Kind == "decision" {
						rec.Disposition = string(wf.CatchPropagate)
					}
				}
				if mode == "proven-fast-path" && rec.Kind == "scope" && rec.ID != `/scope:"$job"#1` {
					rec.Descriptor = json.RawMessage(`{"retry":{"MaximumAttempts":1}}`)
				}
				if mode == "unspecified-policy" && rec.Kind == "scope" {
					rec.Descriptor = json.RawMessage(`{}`)
				}
				if mode == "omitted-policy" && rec.Kind == "scope" && rec.ID != `/scope:"$job"#1` {
					continue
				}
				records = append(records, rec)
			}
			rt, key := catchFixtureRuntime(t, records)
			h := readCatches(t, rt, key)
			require.Len(t, h.Attempts, 1)
			a := h.Attempts[0]
			switch mode {
			case "result-only", "no-control":
				require.Equal(t, wf.CatchSelectionUnknown, a.Selection)
				require.Equal(t, wf.CatchHandlingUnknown, a.Handling)
				require.True(t, catchGap(a, wf.CatchGapSelection, wf.CatchGapFrontier))
				require.Nil(t, a.Accepted)
			case "selection-only", "ancestor-success":
				require.Equal(t, wf.CatchSelectionSelected, a.Selection)
				require.Equal(t, wf.CatchHandlingPending, a.Handling)
				require.Nil(t, a.Accepted)
			case "handled-only-prefix":
				require.Equal(t, wf.CatchHandlingAccepted, a.Handling)
				require.Nil(t, a.Retry)
			case "missing-origin":
				require.Nil(t, a.Original)
				require.True(t, catchGap(a, wf.CatchGapOriginal, wf.CatchGapNotRecorded))
				require.Equal(t, wf.CatchHandlingAccepted, a.Handling)
				require.Equal(t, "recover", a.RuleID)
				require.NotNil(t, a.Retry)
			case "missing-selection":
				require.Equal(t, wf.CatchSelectionUnknown, a.Selection)
				require.Equal(t, wf.CatchHandlingAccepted, a.Handling)
				require.Empty(t, a.RuleID)
			case "proven-fast-path":
				require.Equal(t, wf.CatchHandlingNotApplicable, a.Handling)
				require.Nil(t, a.Accepted)
			case "omitted-policy", "unspecified-policy":
				require.Equal(t, wf.CatchHandlingUnknown, a.Handling)
				require.True(t, catchGap(a, wf.CatchGapHandling, wf.CatchGapFrontier))
			case "ambiguous-replacement":
				require.Equal(t, wf.CatchProvenanceUnknown, a.Accepted.Provenance)
				require.True(t, catchGap(a, wf.CatchGapAcceptedProvenance, wf.CatchGapNotRecorded))
				require.True(t, catchGap(a, wf.CatchGapOriginalOrigin, wf.CatchGapNotRecorded))
			}
		})
	}
}

func TestCatchHistoryCorruptHistory(t *testing.T) {
	for _, mode := range []string{"old-version", "future-version", "bad-json", "bad-identity", "attempt-number", "duplicate-catch", "duplicate-header", "duplicate-chapter", "missing-chapter", "disposition-conflict", "empty-disposition", "out-of-order", "unmatched-handled", "predicate-with-rule", "bad-retry", "undeclared-rule", "orphan-handled", "bad-policy", "retry-after-success"} {
		t.Run(mode, func(t *testing.T) {
			records := catchFullFixture(t)
			for i := range records {
				r := &records[i]
				switch mode {
				case "old-version":
					r.Version = 3
				case "future-version":
					r.Version = 5
				case "bad-identity":
					if r.Kind == "handled" {
						r.ID = "no-boundary"
					}
				case "attempt-number":
					if r.Kind == "attempt" {
						r.Attempt = 99
					}
				case "disposition-conflict":
					if r.Kind == "decision" {
						r.Disposition = string(wf.CatchPropagate)
					}
				case "empty-disposition":
					if r.Kind == "handled" {
						r.Disposition = ""
					}
				case "unmatched-handled":
					if r.Kind == "catch" {
						r.Rule = ""
					}
				case "predicate-with-rule":
					if r.Kind == "catch" {
						r.ErrorKind = "AppError"
						r.Error = json.RawMessage(`{}`)
					}
				case "undeclared-rule":
					if r.Kind == "catch" {
						r.Rule = "never-declared"
					}
				case "bad-policy":
					if r.Kind == "scope" {
						r.Descriptor = json.RawMessage(`"bad-policy"`)
					}
				case "orphan-handled":
					if r.Kind == "handled" {
						r.ID = `/scope:"$job"#1/attempt:1/scope:"unrelated"#1/attempt:1`
					}
				case "retry-after-success":
					if r.Kind == "decision" {
						r.Retry = true
						r.WakeAt = records[0].At
					}
				case "bad-retry":
					if r.Kind == "decision" {
						r.Retry = true
						r.WakeAt = r.WakeAt.AddDate(0, 0, 0)
					}
				}
			}
			if mode == "duplicate-header" {
				records = append(records, records[0])
			}
			if mode == "duplicate-catch" {
				for _, r := range records {
					if r.Kind == "catch" {
						records = append(records, r)
						break
					}
				}
			}
			if mode == "out-of-order" {
				for i, r := range records {
					if r.Kind == "result" && r.ErrorKind != "" {
						records[i], records[i+1] = records[i+1], records[i]
						break
					}
				}
			}
			rt, key := catchFixtureRuntime(t, records)
			frozen := rt.(catchFrozenRuntime)
			switch mode {
			case "bad-json":
				ch := &frozen.chapters[1]
				ch.Body = jobdb.ExecutionChapter{Record: json.RawMessage(`{`)}
			case "duplicate-chapter":
				frozen.chapters = append(frozen.chapters, frozen.chapters[1])
			case "missing-chapter":
				frozen.chapters = append(frozen.chapters[:2], frozen.chapters[3:]...)
			}
			_, err := wf.ReadCatchHistory(context.Background(), frozen, jobdb.GetJobRunRequest{JobKey: key})
			require.Error(t, err)
			require.Contains(t, err.Error(), "catch history")
		})
	}
}

func TestCatchHistoryMissingSelectionWithNestedHandler(t *testing.T) {
	for _, backend := range []string{"sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			recording, inspection := catchHistoryRuntimes(t, backend)
			inner := wf.CatchRule{ID: "inner", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("nested recovered")}, nil
			}}
			outer := wf.CatchRule{ID: "outer/with-punctuation", Match: func(error) bool { return true }, Handle: func(h wf.ExecutionContext, _ error) (wf.CatchResult, error) {
				output, err := h.DoScope("nested-handler", wf.ScopeOptions{Catch: []wf.CatchRule{inner}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("nested original") })
				return wf.CatchResult{Disposition: wf.CatchHandled, Output: output}, err
			}}
			w := wf.ExecutionWorker{JobType: "missing-selection", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				return c.DoScope("outer", wf.ScopeOptions{Catch: []wf.CatchRule{outer}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("outer original") })
			}}
			key := submitExecutionTest(t, recording, w)
			out, err := runExecutionTest(t, recording, key, w)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunCompleted, out.Status)
			info, err := jobdb.GetJobRun(context.Background(), inspection, jobdb.GetJobRunRequest{JobKey: key})
			require.NoError(t, err)
			var records []jobdb.ExecutionRecord
			// An explicit evidence-gap fixture: retain declarations and every independently
			// proven outcome, but omit only the outer selection. No writer changes.
			for _, e := range info.Execution {
				if e.Record.Kind == "catch" && e.Record.Rule == outer.ID {
					continue
				}
				records = append(records, e.Record)
			}
			fixture, fixtureKey := catchFixtureRuntime(t, records)
			h := readCatches(t, fixture, fixtureKey)
			require.Len(t, h.Attempts, 2)
			a, b := catchAt(t, h, "outer"), catchAt(t, h, "nested-handler")
			require.Equal(t, wf.CatchSelectionUnknown, a.Selection)
			require.Empty(t, a.RuleID)
			require.Equal(t, wf.CatchHandlingAccepted, a.Handling)
			require.Equal(t, wf.CatchSelectionSelected, b.Selection)
			require.Equal(t, wf.CatchHandlingAccepted, b.Handling)
			require.Equal(t, a.Boundary.BoundaryID, b.ParentID)
		})
	}
}

// Descendant paths cannot invent a whole-job attempt, even when their own
// declarations, attempts and accepted handling records are otherwise consistent.
func TestCatchHistoryMissingWholeJobAttempt(t *testing.T) {
	records := catchFullFixture(t)
	for i := range records {
		if strings.Contains(records[i].ID, `/attempt:1/scope:`) {
			records[i].ID = strings.Replace(records[i].ID, `/scope:"$job"#1/attempt:1/`, `/scope:"$job"#1/attempt:99/`, 1)
		}
	}
	rt, key := catchFixtureRuntime(t, records)
	_, err := wf.ReadCatchHistory(context.Background(), rt, jobdb.GetJobRunRequest{JobKey: key})
	require.ErrorContains(t, err, "missing recorded whole-job attempt")
	require.ErrorContains(t, err, "attempt:99")
}
