package toyimpl

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
)

func TestResourceBackoffConformance(t *testing.T) {
	runtimetest.RunResourceBackoffConformance(t, runtimetest.ResourceBackoffHarness{Name: "toy-memory", New: func(t testing.TB) runtimetest.ResourceBackoffFixture {
		r := New(WithProjects(celltest.UnrestrictedProjects{}))
		var clock atomic.Int64
		clock.Store(time.Now().UnixNano())
		var random atomic.Uint64
		random.Store(math.Float64bits(.5))
		r.engine.clock = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
		r.engine.resourceSample = func() float64 { return math.Float64frombits(random.Load()) }
		return runtimetest.ResourceBackoffFixture{Runtime: r, ClaimAppend: func(ctx context.Context, key jobdb.JobKey, at time.Time) error {
			s := appendScheduler{r}
			claim, err := s.ClaimAppend(ctx, runtimecore.ClaimAppendRequest{JobKey: key, WorkerID: runtimecore.AppendWorkerPrefix + "resource", Now: at, Duration: time.Minute})
			if err == nil {
				err = s.ReleaseAppend(ctx, claim)
			}
			return err
		}, SetTime: func(at time.Time) { clock.Store(at.UnixNano()) }, SetRandom: func(v float64) { random.Store(math.Float64bits(v)) }}
	}})
}
