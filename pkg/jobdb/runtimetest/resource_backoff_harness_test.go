package runtimetest

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestResourceBackoffHarnessH01(t *testing.T) {
	f := ResourceBackoffFixture{}
	require.Error(t, f.Validate(true))
	f.SetTime = func(time.Time) {}
	f.SetRandom = func(float64) {}
	require.Error(t, f.Validate(true))
	require.Error(t, f.Validate(false))
}

func TestResourceBackoffHarnessMissingFixturesH01(t *testing.T) {
	f := ResourceBackoffFixture{Runtime: toy.New(), ClaimAppend: func(context.Context, jobdb.JobKey, time.Time) error { return nil }, SetTime: func(time.Time) {}, SetRandom: func(float64) {}, Reopen: func() jobdb.ConsumerRuntime { return nil }, Fault: func(string) {}, Barrier: func() (<-chan struct{}, func()) { return nil, nil }}
	require.NoError(t, f.Validate(true))
	for _, missing := range []string{"clock", "rng", "reopen", "fault", "barrier"} {
		t.Run(missing, func(t *testing.T) {
			copy := f
			switch missing {
			case "clock":
				copy.SetTime = nil
			case "rng":
				copy.SetRandom = nil
			case "reopen":
				copy.Reopen = nil
			case "fault":
				copy.Fault = nil
			case "barrier":
				copy.Barrier = nil
			}
			require.Error(t, copy.Validate(true))
		})
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(executable, "-test.run=^TestResourceBackoffHarnessSelectionProbe$/^does-not-exist$")
	cmd.Env = append(os.Environ(), "JOBDB_RESOURCE_SELECTION_PROBE=1")
	raw, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(raw), "selected zero checks")
	cmd = exec.Command(executable, "-test.run=^TestResourceBackoffHarnessSkippedFixtureProbe$")
	cmd.Env = append(os.Environ(), "JOBDB_RESOURCE_SKIP_PROBE=1")
	raw, err = cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(raw), "selected zero checks")
}

func TestResourceBackoffHarnessSkippedFixtureProbe(t *testing.T) {
	if os.Getenv("JOBDB_RESOURCE_SKIP_PROBE") != "1" {
		return
	}
	RunResourceBackoffConformance(t, ResourceBackoffHarness{Name: "skipped", Durable: true, New: func(tb testing.TB) ResourceBackoffFixture {
		tb.Skip("storage unavailable")
		return ResourceBackoffFixture{}
	}})
}
func TestResourceBackoffHarnessSelectionProbe(t *testing.T) {
	if os.Getenv("JOBDB_RESOURCE_SELECTION_PROBE") != "1" {
		return
	}
	RunResourceBackoffConformance(t, ResourceBackoffHarness{Name: "empty", New: func(testing.TB) ResourceBackoffFixture { panic("excluded fixture must not execute") }})
}
