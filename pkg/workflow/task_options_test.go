package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type alternateTestLease struct {
	*fakeExecutionLease
	state ExecutionState
}

func (l alternateTestLease) ExecutionState() ExecutionState { return l.state }

type declaredAlternateJob struct{ at time.Time }

func (declaredAlternateJob) Name() string { return "alternate-identity" }
func (w declaredAlternateJob) Run(ctx JobContext, data JobData) (JobData, error) {
	return ctx.DoTask(RunPolicy{}, "external", WithTaskOptions(data, TaskOptions{Alternate: &TaskAlternate{TaskType: "alternate", At: w.at}}))
}

func TestAlternateDispatchRequiresMatchingLeaseIdentity(t *testing.T) {
	for _, test := range []string{"match", "route", "ordinal", "input-ordinal", "hash", "job", "early"} {
		t.Run(test, func(t *testing.T) {
			rt := newRunnerTestRuntime()
			key := JobKey{TenantId: "tenant", JobId: test}
			data := NewTaskDataOrPanic(map[string]any{"request": "original"})
			hash, err := computeInputHash(context.Background(), data)
			require.NoError(t, err)
			w := declaredAlternateJob{at: time.Now().Add(-time.Minute)}
			var count atomic.Int32
			task := countingTaskWorker{name: "alternate", counter: &count}
			ws := mustWorkSetForRunnerTest(t, w, task)
			seedJobStartForTest(t, rt, key, w.Name(), data, RunPolicy{})
			base := &fakeExecutionLease{runtime: rt, job: JobHandle{JobKey: key}, route: Route{JobType: w.Name(), TaskType: "alternate"}}
			wait := &TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: hash, ResumeJobType: w.Name()}
			switch test {
			case "route":
				base.route.TaskType = "external"
			case "ordinal":
				wait.OutputOrdinal = 2
			case "input-ordinal":
				wait.InputOrdinal = 3
			case "hash":
				wait.InputHash = "other"
			case "job":
				wait.ResumeJobType = "other"
			case "early":
				w.at = time.Now().Add(time.Minute)
			}
			ws.JobWorker = w
			runner := newWorkerRunner(rt, ws, alternateTestLease{base, ExecutionState{TaskWait: wait}}, workerRunnerOptions{})
			_, err = runner.DoJob(context.Background())
			require.NoError(t, err)
			if test == "match" {
				require.EqualValues(t, 1, count.Load())
			} else {
				require.Zero(t, count.Load())
				require.Len(t, base.rescheduleCalls, 1)
			}
		})
	}
}
