package jobdb_test

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestResourceBackoffPolicy(t *testing.T) {
	t.Run("B03_defaults", func(t *testing.T) {
		p := jobdb.DefaultResourceBackoffPolicy()
		for i, want := range []time.Duration{2, 4, 8, 16, 30, 30} {
			got, err := jobdb.ResourceBackoffDelay(p, int64(i+1), .5)
			require.NoError(t, err)
			require.Equal(t, want*time.Second, got)
		}
		for _, sample := range []float64{0, 1} {
			got, err := jobdb.ResourceBackoffDelay(p, 1, sample)
			require.NoError(t, err)
			require.Equal(t, time.Duration(float64(2*time.Second)*(.8+.4*sample)), got)
		}
	})
	t.Run("B03a_fixed", func(t *testing.T) {
		p := jobdb.DefaultResourceBackoffPolicy()
		p.Growth = 1
		p.Maximum = 3 * time.Second
		p.Jitter = 0
		got, err := jobdb.ResourceBackoffDelay(p, math.MaxInt64, 1)
		require.NoError(t, err)
		require.Equal(t, 2*time.Second, got)
	})
	t.Run("B03b_B03c_invalid", func(t *testing.T) {
		p := jobdb.DefaultResourceBackoffPolicy()
		for _, mutate := range []func(*jobdb.ResourceBackoffPolicy){func(p *jobdb.ResourceBackoffPolicy) { p.Initial = 0 }, func(p *jobdb.ResourceBackoffPolicy) { p.Initial = time.Millisecond + 1 }, func(p *jobdb.ResourceBackoffPolicy) { p.Maximum = 31 * time.Second }, func(p *jobdb.ResourceBackoffPolicy) { p.Growth = math.Inf(1) }, func(p *jobdb.ResourceBackoffPolicy) { p.Growth = math.NaN() }, func(p *jobdb.ResourceBackoffPolicy) { p.Growth = .5 }, func(p *jobdb.ResourceBackoffPolicy) { p.Jitter = math.NaN() }, func(p *jobdb.ResourceBackoffPolicy) { p.Jitter = .21 }, func(p *jobdb.ResourceBackoffPolicy) { n := int64(0); p.CrashAllowance = &n }, func(p *jobdb.ResourceBackoffPolicy) { p.Version = 2 }} {
			copy := p
			mutate(&copy)
			require.ErrorIs(t, copy.Validate(), jobdb.ErrInvalidResourceBackoff)
		}
		require.Nil(t, p.CrashAllowance)
		p.Jitter = 0
		require.NoError(t, p.Validate())
	})
	t.Run("B03e_saturation", func(t *testing.T) {
		p := jobdb.DefaultResourceBackoffPolicy()
		p.Growth = math.MaxFloat64
		for _, n := range []int64{1, 2, math.MaxInt64} {
			got, err := jobdb.ResourceBackoffDelay(p, n, 1)
			require.NoError(t, err)
			require.Greater(t, got, time.Duration(0))
			require.LessOrEqual(t, got, 30*time.Second)
		}
		nearLimit := time.Unix(0, math.MaxInt64-int64(time.Second)).UTC()
		require.Equal(t, time.Unix(0, math.MaxInt64).UTC(), jobdb.ResourceBackoffWake(nearLimit, 2*time.Second))
		require.Equal(t, nearLimit.Add(time.Millisecond), jobdb.ResourceBackoffWake(nearLimit, time.Millisecond))
	})
}

func TestResourceBackoffCheckpointIdentityB10(t *testing.T) {
	at := time.Now().UTC()
	total := at.Add(time.Minute)
	original := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "wait", Descriptor: json.RawMessage(`"echo"`), ExecutionTaskType: "echo", ID: `task:"work"#1/attempt:1`, At: at, Deadline: at.Add(time.Second), TotalDeadline: total, Input: &jobdb.ExecutionSnapshot{Data: json.RawMessage(`1`)}}
	encoded, err := json.Marshal(original)
	require.NoError(t, err)
	declaration := jobdb.Chapter{Body: jobdb.ExecutionChapter{Record: encoded}}
	selected := original
	selected.Kind = "resource_checkpoint"
	selected.ID = `task:"work"#1/attempt:2`
	selected.Deadline = at.Add(2 * time.Second)
	selected.Resource = &jobdb.ResourceCheckpoint{Version: 1, Invocation: selected.ID, Attempt: 2, Policy: jobdb.DefaultResourceBackoffPolicy(), ExecutionTaskType: "echo"}
	req := jobdb.ResourceBackoffRequest{Invocation: selected.ID, Attempt: 2}
	check := func() error {
		selected.Payload, _ = json.Marshal(selected.Resource)
		raw, err := json.Marshal(selected)
		require.NoError(t, err)
		return jobdb.ValidateResourceTaskCheckpoint(jobdb.Chapter{Body: jobdb.ExecutionChapter{Record: raw}}, declaration, req)
	}
	require.NoError(t, check(), "a real later recipe attempt may have a new attempt timer")
	selected.TotalDeadline = total.Add(time.Second)
	require.ErrorIs(t, check(), jobdb.ErrInvalidResourceBackoff)
	selected.TotalDeadline = total
	selected.Deadline = total.Add(time.Second)
	require.ErrorIs(t, check(), jobdb.ErrInvalidResourceBackoff)
	selected.Deadline = at.Add(2 * time.Second)
	req.Invocation = `task:"another"#1/attempt:2`
	require.ErrorIs(t, check(), jobdb.ErrInvalidResourceBackoff)
}
