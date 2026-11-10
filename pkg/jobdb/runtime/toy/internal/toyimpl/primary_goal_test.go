package toyimpl

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestPrimaryGoalUnknownIdentityAndPurge(t *testing.T) {
	for _, bad := range []string{"state", "identity", "retired", "purge"} {
		t.Run(bad, func(t *testing.T) {
			r := New()
			ctx := runtimetest.PrimaryGoalContext(context.Background())
			source, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "source", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
			require.NoError(t, err)
			req := jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: source.GoalID, Cell: source.Cell, CellRepo: source.CellRepo, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
			if bad == "purge" {
				h, err := r.SubmitJob(ctx, req)
				require.NoError(t, err)
				r.engine.mu.Lock()
				delete(r.engine.jobRecords, h.JobKey)
				delete(r.engine.runtimeChapters, h.JobKey)
				r.engine.mu.Unlock()
				require.NoError(t, r.DiscardJobPreparation(ctx, h.JobKey))
				replay, err := r.SubmitJob(ctx, req)
				require.NoError(t, err)
				require.Equal(t, h, replay)
				_, err = r.GetJob(ctx, h.JobKey)
				require.ErrorIs(t, err, jobdb.ErrJobNotFound)
				return
			}
			r.engine.mu.Lock()
			key := jobdb.GoalKey{TenantId: "tenant", GoalID: source.GoalID}
			g := r.engine.goals.GoalRows[key]
			if bad == "state" {
				g.State = "UNKNOWN"
			} else if bad == "identity" {
				g.CellID = ""
			} else {
				c := r.engine.cellsLocked("tenant").Cells[source.CellID]
				c.State = jobdb.CellRetired
				r.engine.cellsLocked("tenant").Cells[source.CellID] = c
			}
			r.engine.goals.GoalRows[key] = g
			r.engine.mu.Unlock()
			_, err = r.SubmitJob(ctx, req)
			require.Error(t, err)
			require.Len(t, r.engine.jobRecords, 1)
			require.Len(t, r.engine.goals.GoalRows, 1)
		})
	}
}
