package toyimpl

import (
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestJoinDeadlineBoundary(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Hour)
	for _, terminal := range []bool{false, true} {
		for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
			rt := New()
			child := &jobRecord{status: jobdb.JobStatusReady}
			if terminal {
				child.status = jobdb.JobStatusCompleted
			}
			rt.engine.jobRecords[jobdb.JobKey{TenantId: "tenant", JobId: "child"}] = child
			parent := &jobRecord{status: jobdb.JobStatusPendingJobs, waitFor: []string{"child"}, waitDeadline: deadline}
			rt.advanceRecordStateLocked("tenant", deadline.Add(offset), parent)
			if terminal || offset >= 0 {
				require.Equal(t, jobdb.JobStatusReady, parent.status)
				require.Empty(t, parent.waitFor)
				require.True(t, parent.waitDeadline.IsZero())
			} else {
				require.Equal(t, jobdb.JobStatusPendingJobs, parent.status)
			}
		}
	}
	// Submitted prerequisites and AvailableAt retain AND semantics.
	rt := New()
	rt.engine.jobRecords[jobdb.JobKey{TenantId: "tenant", JobId: "child"}] = &jobRecord{status: jobdb.JobStatusCompleted}
	parent := &jobRecord{status: jobdb.JobStatusPendingJobs, waitFor: []string{"child"}, availableAt: deadline}
	rt.advanceRecordStateLocked("tenant", deadline.Add(-time.Nanosecond), parent)
	require.Equal(t, jobdb.JobStatusAwaitingFuture, parent.status)
	rt.advanceRecordStateLocked("tenant", deadline, parent)
	require.Equal(t, jobdb.JobStatusReady, parent.status)
}
