package workflow

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

// Public orchestration rejects concurrent calls within a scope. Exercise the
// shared delivery boundary directly to guard serialization without introducing
// a parallel execution API or depending on branch scheduling.
func TestTaskResultObserverSerializedDelivery(t *testing.T) {
	var active, received atomic.Int32
	r := &workerRunner{taskResultObserver: func(TaskEndEvent) {
		if active.Add(1) != 1 {
			t.Error("overlapping callback delivery")
		}
		runtime.Gosched()
		received.Add(1)
		active.Add(-1)
	}}
	e := &executionRun{r: r}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() { e.emitTask(TaskCall{TaskType: "echo"}, fmt.Sprint(i), 1, time.Now(), jobdb.ExecutionRecord{}) })
	}
	wg.Wait()
	require.EqualValues(t, 16, received.Load())
}
