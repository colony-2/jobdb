package workflow

import (
	"context"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"testing"
)

type reviewDependencyRuntime struct {
	WorkflowRuntime
	failure error
}

func (r reviewDependencyRuntime) GetDependencyWait(_ context.Context, key jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	if key.WaitID == "submission" {
		return jobdb.DependencyWaitInfo{}, jobdb.ErrDependencyWaitNotFound
	}
	return jobdb.DependencyWaitInfo{}, r.failure
}
func TestInfrastructureIgnoredDependencyInfrastructureFailure(t *testing.T) {
	cause := jobdb.ClassifyExecutionInfrastructure(errors.New("storage unavailable"), jobdb.ExecutionInfrastructureRuntimeIO, "get_dependency_wait", nil, nil)
	runner, lease, _ := infrastructureRunner(t, func(c ExecutionContext, in JobData) (JobData, error) {
		_, err := c.AwaitGoals("goal")
		if !errors.Is(err, cause) {
			t.Fatalf("expected injected error: %v", err)
		}
		return in, nil
	})
	guarded := runner.runtime.(*guardedRuntime)
	guarded.WorkflowRuntime = reviewDependencyRuntime{guarded.WorkflowRuntime, cause}
	_, err := runner.DoJob(celltest.Context(context.Background()))
	_, _, complete, reschedule := lease.snapshot()
	t.Logf("err=%v complete=%d reschedule=%d", err, len(complete), len(reschedule))
	if !jobdb.IsExecutionInfrastructureError(err) || len(complete) != 0 || len(reschedule) != 0 {
		t.Fatalf("ignored SDK read failure escaped latch: err=%v complete=%d reschedule=%d", err, len(complete), len(reschedule))
	}
}

// Dependency results use their own record kinds and may share a reconstruction
// chapter. Report that physical chapter, never a logical execution index.
func TestInfrastructureMalformedDependencyResultLatches(t *testing.T) {
	for _, kind := range []string{"join_result", "admission"} {
		t.Run(kind, func(t *testing.T) {
			var observed error
			var before int
			runner, lease, rt := infrastructureRunner(t, func(c ExecutionContext, in JobData) (JobData, error) {
				x := c.(*executionContext)
				rec := jobdb.ExecutionRecord{Kind: kind, ID: "stored", Output: &jobdb.ExecutionSnapshot{Data: []byte(`[]`)}}
				x.run.mu.Lock()
				x.run.events = append(x.run.events, executionEvent{record: rec, ordinal: 7})
				x.run.mu.Unlock()
				_, observed = x.dependencyResult(rec)
				x.run.r.runtime.(*guardedRuntime).WorkflowRuntime.(*runnerTestRuntime).mu.Lock()
				before = len(x.run.r.runtime.(*guardedRuntime).WorkflowRuntime.(*runnerTestRuntime).chapters[x.GetJobKey()])
				x.run.r.runtime.(*guardedRuntime).WorkflowRuntime.(*runnerTestRuntime).mu.Unlock()
				_, err := c.DoTask(TaskCall{Site: "late", TaskType: "late", ContractVersion: 1}, func(context.Context) (TaskData, error) { t.Error("late task dispatched"); return in, nil })
				if !jobdb.IsExecutionInfrastructureError(err) {
					t.Errorf("late task: %v", err)
				}
				return in, nil // Deliberately ignore the decoding error.
			})
			_, err := runner.DoJob(celltest.Context(context.Background()))
			var typed *jobdb.ExecutionInfrastructureError
			if !errors.As(err, &typed) || !errors.Is(err, observed) {
				t.Fatalf("missing latched error: %v", err)
			}
			if typed.Reason != jobdb.ExecutionInfrastructureDurableDecode || typed.Ordinal == nil || *typed.Ordinal != 7 || typed.JobKey == nil || *typed.JobKey != runner.jobKey {
				t.Fatalf("incorrect source: %+v", typed)
			}
			_, _, complete, reschedule := lease.snapshot()
			if len(complete) != 0 || len(reschedule) != 0 || len(rt.chapters[runner.jobKey]) != before {
				t.Fatal("mutation after decoding failure")
			}
		})
	}
}
