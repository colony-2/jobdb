package toyimpl

import "github.com/colony-2/jobdb/pkg/jobdb/celltest"

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
			rt := New(WithProjects(celltest.UnrestrictedProjects{}))
			child := &jobRecord{crashBudgetRemaining: 5, status: jobdb.JobStatusReady}
			if terminal {
				child.status = jobdb.JobStatusCompleted
			}
			rt.engine.jobRecords[jobdb.JobKey{TenantId: "tenant", JobId: "child"}] = child
			parent := &jobRecord{crashBudgetRemaining: 5, status: jobdb.JobStatusPendingJobs, waitFor: []string{"child"}, waitDeadline: deadline}
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
	rt := New(WithProjects(celltest.UnrestrictedProjects{}))
	rt.engine.jobRecords[jobdb.JobKey{TenantId: "tenant", JobId: "child"}] = &jobRecord{crashBudgetRemaining: 5, status: jobdb.JobStatusCompleted}
	parent := &jobRecord{crashBudgetRemaining: 5, status: jobdb.JobStatusPendingJobs, waitFor: []string{"child"}, availableAt: deadline}
	rt.advanceRecordStateLocked("tenant", deadline.Add(-time.Nanosecond), parent)
	require.Equal(t, jobdb.JobStatusAwaitingFuture, parent.status)
	rt.advanceRecordStateLocked("tenant", deadline, parent)
	require.Equal(t, jobdb.JobStatusReady, parent.status)
}

func TestJoinDeadlineRetainsFutureAvailability(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Hour)
	available := deadline.Add(time.Hour)
	for _, complete := range []bool{false, true} {
		rt := New(WithProjects(celltest.UnrestrictedProjects{}))
		child := &jobRecord{crashBudgetRemaining: 5, status: jobdb.JobStatusReady}
		if complete {
			child.status = jobdb.JobStatusCompleted
		}
		rt.engine.jobRecords[jobdb.JobKey{TenantId: "tenant", JobId: "child"}] = child
		parent := &jobRecord{crashBudgetRemaining: 5, status: jobdb.JobStatusPendingJobs, waitFor: []string{"child"}, waitDeadline: deadline, availableAt: available}
		require.Equal(t, jobdb.JobStatusAwaitingFuture, rt.engine.availableStatusLocked("tenant", deadline, parent))
		require.Equal(t, jobdb.JobStatusAwaitingFuture, rt.engine.availableStatusLocked("tenant", available.Add(-time.Nanosecond), parent))
		require.Equal(t, jobdb.JobStatusReady, rt.engine.availableStatusLocked("tenant", available, parent))
	}
}
