package runtimecore_test

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
	"testing"
)

type goalCaptureScheduler struct {
	runtimecore.Scheduler
	preparation runtimecore.JobPreparation
	created     runtimecore.CreateJobRequest
	published   bool
	history     *goalCaptureHistory
}

func (s *goalCaptureScheduler) PrepareJob(_ context.Context, p runtimecore.JobPreparation) (runtimecore.PreparedJob, error) {
	s.preparation = p
	key := p.Key
	if key.JobId == "" {
		key.JobId = "reserved"
	}
	return runtimecore.PreparedJob{Key: key, GoalID: "reserved-goal", Published: s.published}, nil
}
func (s *goalCaptureScheduler) CreateJob(_ context.Context, req runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	if !s.history.created {
		panic("scheduler published before history")
	}
	s.created = req
	s.published = true
	return runtimecore.StoredJob{JobKey: req.JobKey, GoalID: req.GoalID, JobType: req.JobType, SchemaHash: req.SchemaHash, ParentJobID: req.ParentJobID, AppMetadata: req.AppMetadata, RunPolicy: req.RunPolicy, Schedule: req.Schedule}, nil
}

type goalCaptureHistory struct {
	runtimecore.ChapterLog
	created bool
}

func (h *goalCaptureHistory) Create(context.Context, runtimecore.ChapterLogKey, runtimecore.EncodedChapter) error {
	h.created = true
	return nil
}
func TestGoalCorePublicationAndRestartReservation(t *testing.T) {
	history := &goalCaptureHistory{}
	scheduler := &goalCaptureScheduler{history: history}
	rt, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: scheduler, Chapters: history, Schemas: readTestSchemas{}})
	require.NoError(t, err)
	deps := []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: "upstream"}, Condition: jobdb.JobPrereqSuccess}}
	handle, err := rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), Dependencies: deps}})
	require.NoError(t, err)
	require.Equal(t, "reserved-goal", handle.GoalID)
	require.Equal(t, deps, scheduler.created.Dependencies)
	require.Equal(t, handle.GoalID, scheduler.created.GoalID)
	// A committed restart retry must return before touching source history, even
	// if the source or destination has advanced since the original operation.
	history.created = false
	h, err := rt.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJobRequest{RequestID: "operation", Job: jobdb.SubmitRestartJob{PriorJobKey: jobdb.JobKey{TenantId: "tenant", JobId: "source"}, ShareGoal: true, ExecutionRestart: &jobdb.ExecutionRestart{ScopeID: "boundary"}}})
	require.NoError(t, err)
	require.Equal(t, handle, h)
	require.True(t, scheduler.preparation.ShareGoal)
	require.Equal(t, "operation", scheduler.preparation.RequestID)
	require.False(t, history.created)
}
