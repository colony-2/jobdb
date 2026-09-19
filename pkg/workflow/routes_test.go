package workflow

import (
	"sync/atomic"
	"testing"
)

func TestRouteIdentifiersValidatedBeforeExecution(t *testing.T) {
	for _, invalid := range []string{"", "bad\x00name", "bad\xffname"} {
		if _, err := AsWorkSet(countingJobWorker{name: invalid}); err == nil {
			t.Errorf("registered invalid job %q", invalid)
		}
		if _, err := AsWorkSet(countingJobWorker{name: "job:valid"}, countingTaskWorker{name: invalid, counter: &atomic.Int32{}}); err == nil {
			t.Errorf("registered invalid task %q", invalid)
		}
		runner := &workerRunner{}
		if _, err := runner.DoTask(RunPolicy{}, invalid, nil); err == nil {
			t.Errorf("invoked invalid task %q", invalid)
		}
	}
	if _, err := AsWorkSet(countingJobWorker{name: "job:valid"}, countingTaskWorker{name: "input:collect_user_input", counter: &atomic.Int32{}}); err != nil {
		t.Fatal(err)
	}
}
