package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/colony-2/jobdb/pkg/internal/executionmeta"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestReplayTerminalBoundaryProvenance(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(*executionContext, *jobdb.ChapterApplicationPayloadError)
		match bool
	}{
		{"exact", func(*executionContext, *jobdb.ChapterApplicationPayloadError) {}, true},
		{"other identity", func(_ *executionContext, s *jobdb.ChapterApplicationPayloadError) { s.LogicalID = "other" }, false},
		{"other kind", func(_ *executionContext, s *jobdb.ChapterApplicationPayloadError) { s.RecordKind = "wait" }, false},
		{"other attempt", func(_ *executionContext, s *jobdb.ChapterApplicationPayloadError) { s.Attempt = 2 }, false},
		{"wrong ordinal", func(_ *executionContext, s *jobdb.ChapterApplicationPayloadError) { s.Ordinal = 4 }, false},
		{"no identity", func(_ *executionContext, s *jobdb.ChapterApplicationPayloadError) { s.LogicalID = "" }, false},
		{"no provenance", func(_ *executionContext, s *jobdb.ChapterApplicationPayloadError) {
			s.LogicalID = ""
			s.RecordKind = ""
		}, false},
		{"shortened identity", func(_ *executionContext, s *jobdb.ChapterApplicationPayloadError) { s.LogicalID = "task..." }, false},
		{"accepted history remains", func(c *executionContext, _ *jobdb.ChapterApplicationPayloadError) {
			c.run.events = []executionEvent{{record: jobdb.ExecutionRecord{Kind: "result", ID: "earlier"}, ordinal: 2}}
		}, false},
		{"not physical end", func(c *executionContext, _ *jobdb.ChapterApplicationPayloadError) { c.run.terminalPosition = false }, false},
		{"malformed diagnostics", func(c *executionContext, _ *jobdb.ChapterApplicationPayloadError) { c.run.final.TaskType = "malformed" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(celltest.Context(context.Background()))
			defer cancel(context.Canceled)
			e := &executionRun{r: &workerRunner{replay: true}, ordinal: 8, terminalPosition: true, final: &Chapter{Ordinal: 7}, active: map[*executionContext]bool{}}
			c := &executionContext{run: e, ctx: ctx, cancel: cancel}
			s := &jobdb.ChapterApplicationPayloadError{Ordinal: 7, RecordKind: "result", LogicalID: "task/attempt:1", Attempt: 1, ActualBytes: 999, LimitBytes: 64, DiagnosticsTruncated: true, OmittedFields: 8, OmittedBytes: 100}
			tc.edit(c, s)
			e.terminal = s.AppError()
			if e.final.TaskType == "malformed" {
				e.terminal = jobdb.AppError{Payload: jobdb.AppErrorPayload{Attrs: map[string]any{"payloadSize": map[string]any{"ordinal": "invalid"}}}}
			}
			err := c.terminalBoundary("result", "task/attempt:1", 1, nil, "")
			if tc.match {
				require.ErrorIs(t, err, e.terminal)
				require.NotNil(t, e.fatal)
			} else {
				require.NoError(t, err)
				require.Nil(t, e.fatal)
			}
			require.EqualValues(t, 8, e.ordinal)
			require.Empty(t, e.pending)
			require.False(t, e.live)
		})
	}
}

func TestReplayTerminalBoundaryPrecedenceAndControl(t *testing.T) {
	for _, cause := range []error{context.Canceled, ErrWorkflowNotDeterministic, ErrReplayShouldNeverMutate, errors.New("builder failed")} {
		t.Run(cause.Error(), func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(celltest.Context(context.Background()))
			defer cancel(context.Canceled)
			e := &executionRun{r: &workerRunner{replay: true}, terminalPosition: true, final: &Chapter{Ordinal: 7}, active: map[*executionContext]bool{}}
			e.terminal = (&jobdb.ChapterApplicationPayloadError{Ordinal: 7, RecordKind: "result", LogicalID: "task/attempt:1"}).AppError()
			c := &executionContext{run: e, ctx: ctx, cancel: cancel}
			if errors.Is(cause, context.Canceled) {
				cancel(cause)
			} else {
				e.control(cause)
			}
			require.ErrorIs(t, c.terminalBoundary("result", "task/attempt:1", 1, nil, ""), cause)
			_, err := c.finishBody("result", "task/attempt:1", nil, cause, nil)
			require.ErrorIs(t, err, cause)
		})
	}
}

func TestLiveTerminalPayloadGuard(t *testing.T) {
	rt := newRunnerTestRuntime()
	key := JobKey{TenantId: "tenant", JobId: "terminal"}
	var runs atomic.Int32
	w := countingJobWorker{name: "terminal", counter: &runs}
	ws := mustWorkSetForRunnerTest(t, w)
	seedJobStartForTest(t, rt, key, w.Name(), NewTaskDataOrPanic(1), RunPolicy{})
	failure := (&jobdb.ChapterApplicationPayloadError{Ordinal: 1, LimitBytes: 1, ActualBytes: 2}).AppError()
	ch := Chapter{Ordinal: 1, TaskType: w.Name(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: failure.Payload}}}
	rt.chapters[key][1] = ch
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: w.Name()}}
	out, err := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(celltest.Context(context.Background()))
	require.NotNil(t, out) // Preserve the live API's error envelope.
	require.True(t, jobdb.IsChapterPayloadFailure(err))
	require.Zero(t, runs.Load())
	require.Len(t, rt.chapters[key], 2)
	_, _, complete, reschedule := lease.snapshot()
	require.Len(t, complete, 1)
	require.Empty(t, reschedule)
	require.Equal(t, ch, *complete[0].Chapter)
}

func TestReplayTerminalMarkerDoesNotHideInvalidHistory(t *testing.T) {
	for _, mode := range []string{"record", "reconstruction", "version", "pinned-version", "header"} {
		t.Run(mode, func(t *testing.T) {
			rt := newRunnerTestRuntime()
			key := JobKey{TenantId: "tenant", JobId: "invalid"}
			var calls atomic.Int32
			w := countingJobWorker{name: "invalid", counter: &calls}
			ws := mustWorkSetForRunnerTest(t, w)
			seedJobStartForTest(t, rt, key, w.Name(), NewTaskDataOrPanic(1), RunPolicy{})
			header := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "header", Versions: &jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion}}
			if mode == "version" {
				header.Version++
			}
			if mode == "pinned-version" {
				header.Versions.ExecutionPatternVersion--
			}
			if mode == "header" {
				header.Kind = "activation"
			}
			raw, err := json.Marshal(header)
			require.NoError(t, err)
			if mode == "record" {
				raw = []byte(`broken`)
			}
			ch := Chapter{Ordinal: 1, TaskType: "__execution__", Body: jobdb.ExecutionChapter{Record: raw}}
			if mode == "reconstruction" {
				ch.Metadata = jobdb.ChapterMetadata{Fields: map[string]jobdb.ChapterMetadataValue{"metadata": {Kind: jobdb.ChapterMetadataMap, Map: map[string]jobdb.ChapterMetadataValue{executionmeta.Key: {Kind: jobdb.ChapterMetadataString, String: `broken`}}}}}
			}
			rt.chapters[key][1] = ch
			marker := jobdb.PayloadFailureChapter(Chapter{Ordinal: 2, TaskType: w.Name()}, &jobdb.ChapterApplicationPayloadError{Ordinal: 2, LimitBytes: 1, ActualBytes: 2})
			rt.chapters[key][2] = marker
			_, err = newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true}).DoJob(celltest.Context(context.Background()))
			require.Error(t, err)
			require.False(t, jobdb.IsChapterPayloadFailure(err))
			require.Zero(t, calls.Load())
			require.Len(t, rt.chapters[key], 3)
		})
	}
}

func TestReplayTerminalEventNeverBuildsMissingOutcome(t *testing.T) {
	for _, kind := range []string{"result", "catch", "handled", "decision", "continuation", "wait", "sleep", "join", "join_result", "admission", "dispatch"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(celltest.Context(context.Background()))
			defer cancel(context.Canceled)
			failure := (&jobdb.ChapterApplicationPayloadError{Ordinal: 7, RecordKind: kind, LogicalID: "boundary", DiagnosticsTruncated: true, OmittedFields: 8, OmittedBytes: 100}).AppError()
			e := &executionRun{r: &workerRunner{replay: true}, ordinal: 8, terminal: failure, terminalPosition: true, final: &Chapter{Ordinal: 7}, active: map[*executionContext]bool{}}
			c := &executionContext{run: e, ctx: ctx, cancel: cancel}
			calls := 0
			before, err := json.Marshal(failure)
			require.NoError(t, err)
			_, err = c.event(kind, "boundary", func() *jobdb.ExecutionRecord { calls++; return &jobdb.ExecutionRecord{} })
			require.ErrorIs(t, err, failure)
			require.Zero(t, calls)
			require.NotNil(t, e.fatal)
			require.Empty(t, e.pending)
			require.EqualValues(t, 8, e.ordinal)
			require.False(t, e.live)
			require.False(t, e.activated)
			after, err := json.Marshal(failure)
			require.NoError(t, err)
			require.Equal(t, before, after)
			// Ignoring the returned error does not let a later declaration run.
			_, err = c.event("task", "following", func() *jobdb.ExecutionRecord { calls++; return &jobdb.ExecutionRecord{} })
			require.ErrorIs(t, err, failure)
			require.Zero(t, calls)
		})
	}
}

func TestReplayTerminalLegacyOutcomeProvenance(t *testing.T) {
	for _, mode := range []string{"root", "external", "shortened external identity", "conflicting external identity", "wrong external position", "wrong external type", "unrelated task"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(celltest.Context(context.Background()))
			defer cancel(context.Canceled)
			var runs atomic.Int32
			w := countingJobWorker{name: "job", counter: &runs}
			e := &executionRun{r: &workerRunner{replay: true, worker: mustWorkSetForRunnerTest(t, w)}, ordinal: 8, terminalPosition: true, active: map[*executionContext]bool{}}
			c := &executionContext{run: e, ctx: ctx, cancel: cancel}
			id := `/scope:"$job"#1/attempt:1`
			taskType := "job"
			var positions map[int64]bool
			switch mode {
			case "external", "shortened external identity", "conflicting external identity", "wrong external position", "wrong external type":
				id = "external/attempt:1"
				taskType = "worker"
				positions = map[int64]bool{7: true}
			case "unrelated task":
				id = "local/attempt:1"
			}
			size := &jobdb.ChapterApplicationPayloadError{Ordinal: 7, TaskType: taskType}
			e.final = &Chapter{Ordinal: 7, TaskType: taskType}
			if mode == "wrong external position" {
				positions = map[int64]bool{6: true}
			}
			if mode == "wrong external type" {
				size.TaskType = "other"
			}
			if mode == "shortened external identity" {
				size.RecordKind = "result"
				size.LogicalID = "shortened..."
			}
			if mode == "conflicting external identity" {
				size.RecordKind = "result"
				size.LogicalID = "other/attempt:1"
			}
			e.terminal = size.AppError()
			err := c.terminalBoundary("result", id, 1, positions, taskType)
			if mode == "root" || mode == "external" || mode == "shortened external identity" {
				require.ErrorIs(t, err, e.terminal)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, runs.Load())
		})
	}
}

// An accepted root result is not proof that its terminal completion chapter
// was accepted. Conversely, unrelated marker provenance cannot imply success.
func TestReplayTerminalAfterAcceptedRootResult(t *testing.T) {
	for _, mode := range []string{"root outcome", "unrelated identity", "invalid record kind", "wrong job type", "unconsumed history"} {
		t.Run(mode, func(t *testing.T) {
			rt := newRunnerTestRuntime()
			key := JobKey{TenantId: "tenant", JobId: "accepted-root"}
			var calls atomic.Int32
			w := countingJobWorker{name: "accepted-root", counter: &calls}
			ws := mustWorkSetForRunnerTest(t, w)
			seedJobStartForTest(t, rt, key, w.Name(), NewTaskDataOrPanic("accepted"), RunPolicy{})
			lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: w.Name()}}
			_, err := newWorkerRunner(rt, ws, lease, workerRunnerOptions{}).DoJob(celltest.Context(context.Background()))
			require.NoError(t, err)
			before, err := rt.ListChapters(celltest.Context(context.Background()), ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			final := before[len(before)-1]
			size := &jobdb.ChapterApplicationPayloadError{Ordinal: final.Ordinal, TaskType: w.Name(), LimitBytes: 1, ActualBytes: 10, ExcessBytes: 9}
			switch mode {
			case "unrelated identity":
				size.RecordKind = "result"
				size.LogicalID = "other/attempt:1"
			case "invalid record kind":
				size.RecordKind = "job_outcome"
			case "wrong job type":
				size.TaskType = "other"
			case "unconsumed history":
				extra, encodeErr := recordResult(celltest.Context(context.Background()), NewTaskDataOrPanic("unconsumed"), nil)
				require.NoError(t, encodeErr)
				extra.Version, extra.Kind, extra.ID = jobdb.ExecutionPatternVersion, "result", "unconsumed"
				raw, encodeErr := json.Marshal(extra)
				require.NoError(t, encodeErr)
				rt.chapters[key][final.Ordinal] = Chapter{Ordinal: final.Ordinal, TaskType: "__execution__", Body: jobdb.ExecutionChapter{Record: raw}}
				final.Ordinal++
				size.Ordinal = final.Ordinal
			}
			rt.chapters[key][final.Ordinal] = jobdb.PayloadFailureChapter(final, size)
			marker, err := rt.GetChapter(celltest.Context(context.Background()), ChapterRef{JobKey: key, Ordinal: final.Ordinal})
			require.NoError(t, err)
			_, storedFailure := chapterToTaskData(rt, key, marker)
			retained, err := rt.ListChapters(celltest.Context(context.Background()), ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			writes := 0
			rt.putChapterHook = func(PutChapterRequest) error { writes++; return errors.New("unexpected replay write") }
			observed := &replayObserverRecorder{}
			out, err := newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true, Observer: observed}).DoJob(celltest.Context(context.Background()))
			require.Nil(t, out)
			require.Len(t, observed.jobStarts, 1)
			require.Empty(t, observed.jobEnds, "an accepted root result is not terminal completion")
			require.Empty(t, observed.taskStarts)
			require.Empty(t, observed.taskEnds)
			if mode == "root outcome" {
				var got, want jobdb.AppError
				require.ErrorAs(t, err, &got)
				require.ErrorAs(t, storedFailure, &want)
				require.Equal(t, want.Payload, got.Payload)
			} else if mode == "unconsumed history" {
				require.ErrorIs(t, err, ErrWorkflowNotDeterministic)
			} else {
				var miss ReplayCacheMissError
				require.ErrorAs(t, err, &miss)
				require.Equal(t, ReplayCacheMissJobResultMissing, miss.Reason)
			}
			require.EqualValues(t, 2, calls.Load())
			require.Zero(t, writes)
			after, readErr := rt.ListChapters(celltest.Context(context.Background()), ListChaptersRequest{JobKey: key})
			require.NoError(t, readErr)
			require.Equal(t, retained, after)
		})
	}
}

func TestReplayTerminalCompletionObserverHistory(t *testing.T) {
	for _, mode := range []string{"success", "ordinary failure", "retry"} {
		t.Run(mode, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			rt := newRunnerTestRuntime()
			key := JobKey{TenantId: "tenant", JobId: "completion-history"}
			var calls atomic.Int32
			w := failThenSucceedJob{name: "completion-history", counter: &calls}
			policy := RunPolicy{}
			if mode == "ordinary failure" {
				w.failAttempts = 10
			} else if mode == "retry" {
				w.failAttempts = 1
				policy.Retry.MaximumAttempts = 2
			}
			ws := mustWorkSetForRunnerTest(t, w)
			seedJobStartForTest(t, rt, key, w.Name(), NewTaskDataOrPanic("accepted"), policy)
			lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: w.Name()}}
			live := &replayObserverRecorder{}
			_, liveErr := newWorkerRunner(rt, ws, lease, workerRunnerOptions{JobPolicy: policy, Observer: live}).DoJob(ctx)
			require.Equal(t, mode == "ordinary failure", liveErr != nil)
			attempts := calls.Load()
			require.Len(t, live.jobEnds, int(attempts))
			before, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			writes := 0
			rt.putChapterHook = func(PutChapterRequest) error { writes++; return errors.New("unexpected replay write") }
			healthy := &replayObserverRecorder{}
			_, err = newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true, Observer: healthy}).DoJob(ctx)
			require.Equal(t, historyError(liveErr), historyError(err))
			require.Equal(t, live, healthy, "ordinary completion preserves every observer event")

			final := before[len(before)-1]
			size := &jobdb.ChapterApplicationPayloadError{Ordinal: final.Ordinal, TaskType: w.Name(), LimitBytes: 1, ActualBytes: 10}
			rt.chapters[key][final.Ordinal] = jobdb.PayloadFailureChapter(final, size)
			retained, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			observed := &replayObserverRecorder{}
			out, err := newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true, Observer: observed}).DoJob(ctx)
			require.Nil(t, out)
			require.True(t, jobdb.IsChapterPayloadFailure(err))
			require.Equal(t, live.jobStarts, observed.jobStarts)
			require.Len(t, observed.jobEnds, len(live.jobEnds)-1)
			for i, event := range observed.jobEnds {
				require.Equal(t, live.jobEnds[i], event, "accepted earlier attempt ends remain visible")
			}
			require.Empty(t, observed.taskStarts)
			require.Empty(t, observed.taskEnds)
			require.Equal(t, attempts*3, calls.Load())
			require.Zero(t, writes)
			after, readErr := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
			require.NoError(t, readErr)
			require.Equal(t, retained, after)
		})
	}
}

func TestReplayTerminalCompletionMissingDecision(t *testing.T) {
	ctx := celltest.Context(context.Background())
	rt := newRunnerTestRuntime()
	key := JobKey{TenantId: "tenant", JobId: "missing-decision"}
	var calls atomic.Int32
	w := failThenSucceedJob{name: "missing-decision", counter: &calls, failAttempts: 10}
	ws := mustWorkSetForRunnerTest(t, w)
	policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 2}}
	seedJobStartForTest(t, rt, key, w.Name(), NewTaskDataOrPanic("accepted"), policy)
	lease := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: w.Name()}}
	_, err := newWorkerRunner(rt, ws, lease, workerRunnerOptions{JobPolicy: policy}).DoJob(ctx)
	require.Error(t, err)
	chapters, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	var boundary int64
	for _, ch := range chapters {
		if body, ok := ch.Body.(jobdb.ExecutionChapter); ok {
			rec, decodeErr := jobdb.DecodeExecutionRecord(body.Record)
			require.NoError(t, decodeErr)
			if rec.Kind == "result" {
				boundary = ch.Ordinal + 1
				break
			}
		}
	}
	require.NotZero(t, boundary)
	for ordinal := range rt.chapters[key] {
		if ordinal >= boundary {
			delete(rt.chapters[key], ordinal)
		}
	}
	// Legacy diagnostics cannot identify the missing failure-handling decision.
	// Do not reinterpret that gap as a proven terminal completion boundary.
	rt.chapters[key][boundary] = jobdb.PayloadFailureChapter(Chapter{Ordinal: boundary, TaskType: w.Name()}, &jobdb.ChapterApplicationPayloadError{Ordinal: boundary, TaskType: w.Name(), LimitBytes: 1, ActualBytes: 10})
	before, err := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
	require.NoError(t, err)
	observed := &replayObserverRecorder{}
	out, err := newWorkerRunner(rt, ws, nil, workerRunnerOptions{JobKey: key, Replay: true, Observer: observed}).DoJob(ctx)
	require.Nil(t, out)
	var miss ReplayCacheMissError
	require.ErrorAs(t, err, &miss)
	require.Equal(t, ReplayCacheMissAwaitNotReady, miss.Reason)
	require.False(t, jobdb.IsChapterPayloadFailure(err))
	require.Len(t, observed.jobStarts, 1)
	require.Empty(t, observed.jobEnds)
	require.EqualValues(t, 3, calls.Load())
	after, readErr := rt.ListChapters(ctx, ListChaptersRequest{JobKey: key})
	require.NoError(t, readErr)
	require.Equal(t, before, after)
}
