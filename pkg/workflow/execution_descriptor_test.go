package workflow_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

// A completed task inside a subsequently timed-out job remains replayable.
// Lifecycle defaults bind to history; structural changes still fail before its
// cached result can be consumed, without exposing task data.
func TestExecutionReplayDescriptorDiagnostics(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite", "remote"} {
		t.Run(backend, func(t *testing.T) {
			rt := alternateRuntime(t, backend)
			var builds, runs atomic.Int32
			opts := wf.ScopeOptions{TotalTimeout: 5 * time.Minute, OnRestart: func(json.RawMessage) error { return nil }}
			call := invocation("command_execution:command_execution")
			call.Command = json.RawMessage(`{"credential":"recorded-secret-value"}`)
			task := executionTestTask{"echo", func(_ wf.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
				runs.Add(1)
				return in, nil
			}}
			w := wf.ExecutionWorker{JobType: "policy-replay", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
				_, err := c.DoScope("op:command_execution", opts, func(s wf.ExecutionContext) (jobdb.JobData, error) {
					return s.DoTask(call, func(context.Context) (jobdb.TaskData, error) {
						builds.Add(1)
						return in, nil
					})
				})
				if err != nil {
					return nil, err
				}
				// Record a timeout outcome without a timer or scheduling race. This
				// test concerns replay compatibility, not deadline enforcement.
				return nil, wf.NewTimeoutError("scope", 0, wf.TimeoutScopeTotal, nil, false)
			}}
			key := submitExecutionTest(t, rt, w)
			out, err := runExecutionTest(t, rt, key, w, task)
			require.NoError(t, err)
			require.Equal(t, wf.JobRunFailed, out.Status)
			var timeout jobdb.TimeoutError
			require.ErrorAs(t, out.JobError, &timeout)
			before, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
			require.NoError(t, err)
			originalOptions, originalCall := opts, call
			for _, tc := range []struct {
				name   string
				change func()
				want   string
			}{
				{"unchanged", func() {}, ""},
				{"removed-default", func() { opts.TotalTimeout = 0 }, ""},
				{"added-attempt-limit", func() { opts.AttemptTimeout = time.Minute }, ""},
				{"retry", func() { opts.Retry.MaximumAttempts = 2 }, ""},
				{"restart", func() { opts.OnRestart = nil }, "changed fields: restart"},
				{"catch", func() {
					opts.Catch = []wf.CatchRule{{ID: "new-rule", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) { return wf.CatchResult{}, nil }}}
				}, "changed fields: rules"},
				{"contract-version", func() { call.ContractVersion++ }, "changed fields: call"},
				{"task-type", func() { call.TaskType = "different" }, "changed fields: call"},
				{"command", func() { call.Command = json.RawMessage(`{"credential":"current-secret-value"}`) }, "changed fields: call"},
				{"task-policy", func() { call.Policy.TotalTimeout = jobdb.AsDuration(time.Minute) }, ""},
				{"restored", func() {}, ""},
			} {
				t.Run(tc.name, func(t *testing.T) {
					opts, call = originalOptions, originalCall
					tc.change()
					_, err := replayExecutionTest(t, rt, key, w, task)
					if tc.want == "" {
						require.ErrorAs(t, err, &timeout)
						require.NotErrorIs(t, err, jobdb.ErrWorkflowNotDeterministic)
					} else {
						require.ErrorIs(t, err, jobdb.ErrWorkflowNotDeterministic)
						require.ErrorContains(t, err, tc.want)
					}
					require.NotContains(t, err.Error(), "recorded-secret-value")
					require.NotContains(t, err.Error(), "current-secret-value")
					require.EqualValues(t, 1, builds.Load())
					require.EqualValues(t, 1, runs.Load())
					after, err := rt.ListChapters(context.Background(), jobdb.ListChaptersRequest{JobKey: key})
					require.NoError(t, err)
					require.Equal(t, before, after, "replay must not rewrite history")
				})
			}
		})
	}
}
