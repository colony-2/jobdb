package toyimpl

import (
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leasetest"
)

func TestLastLeaseUpdateLifecycle(t *testing.T) {
	runtime := New(WithProjects(celltest.UnrestrictedProjects{}))
	leasetest.Run(t, runtime, func(clock func() time.Time) { runtime.engine.clock = clock }, false)
}
