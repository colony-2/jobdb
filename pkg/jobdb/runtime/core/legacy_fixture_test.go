// Historical method set from f92ae253a5da7a6b81944d0bdf4cadb085772140.
// Do not regenerate from the current interfaces or embed them.
package runtimecore

import (
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
	"time"
)

var _ = json.RawMessage{}
var _ io.Reader
var _ time.Time

type legacyScheduler struct{}

func (legacyScheduler) CreateCell(context.Context, jobdb.CreateCellRequest) (jobdb.Cell, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) GetCell(context.Context, jobdb.CellKey) (jobdb.Cell, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ListCells(context.Context, jobdb.ListCellsRequest) (jobdb.ListCellsResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) UpdateCell(context.Context, jobdb.UpdateCellRequest) (jobdb.Cell, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) RetireCell(context.Context, jobdb.RetireCellRequest) (jobdb.Cell, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) CreateCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, jobdb.CreateCellRequest) (jobdb.Cell, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ResolveCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) LookupCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ListGoals(context.Context, jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ListDependencyWaits(context.Context, jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) GetGoal(context.Context, jobdb.GoalKey) (jobdb.GoalInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) AddGoalCandidate(context.Context, jobdb.AddGoalCandidateRequest) (jobdb.GoalCandidateInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) FailGoal(context.Context, jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) GetDependencyWait(context.Context, jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) PrepareJob(context.Context, JobPreparation) (PreparedJob, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) DiscardJobPreparation(context.Context, jobdb.JobKey) error {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ResolveScheduleCell(context.Context, string, string, string) (jobdb.ResolvedCell, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) CreateJob(ctx context.Context, req CreateJobRequest) (StoredJob, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) GetJob(ctx context.Context, key jobdb.JobKey) (StoredJob, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) CancelJob(ctx context.Context, req CancelJobMutation) (StoredJob, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ClearCrashConcern(context.Context, jobdb.ClearCrashConcernRequest) error {
	panic("compile-only historical fixture")
}
func (legacyScheduler) AcquireWork(ctx context.Context, req WorkRequest) ([]LeaseSnapshot, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) AcquireJobLease(ctx context.Context, req JobLeaseRequest) (*LeaseSnapshot, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ValidateLease(ctx context.Context, identity LeaseIdentity) (LeaseSnapshot, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) KeepAliveLease(ctx context.Context, mutation LeaseMutation) (LeaseSnapshot, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) CompleteLease(ctx context.Context, mutation CompletionMutation) (StoredJob, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) RescheduleLease(ctx context.Context, mutation RescheduleMutation) (StoredJob, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) GetWaitingTask(ctx context.Context, key jobdb.JobKey) (WaitingTaskSnapshot, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ClaimTask(ctx context.Context, req ClaimTaskRequest) (LeaseIdentity, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) UpsertSchedule(ctx context.Context, mutation StoredScheduleMutation) (StoredSchedule, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) GetSchedule(ctx context.Context, key jobdb.ScheduleKey) (StoredSchedule, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ListSchedules(ctx context.Context, req ListSchedulesRequest) (ListSchedulesResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) MutateSchedule(ctx context.Context, mutation ScheduleStateMutation) (StoredSchedule, error) {
	panic("compile-only historical fixture")
}
func (legacyScheduler) ListScheduleRuns(ctx context.Context, req ListScheduleRunsRequest) (ListScheduleRunsResponse, error) {
	panic("compile-only historical fixture")
}

var _ Scheduler = legacyScheduler{}

// Legacy scheduler construction needs no optional reader.
type compatibilityChapters struct{ ChapterLog }
type compatibilitySchemas struct{ SchemaStore }

func TestLegacySchedulerConstruction(t *testing.T) {
	rt, err := NewRuntime(Config{Scheduler: legacyScheduler{}, Configurations: &configuration.MemoryStore{}, Chapters: compatibilityChapters{}, Schemas: compatibilitySchemas{}})
	require.NoError(t, err)
	_, err = jobdb.ListLeaseEvents(celltest.Context(context.Background()), rt, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.ErrorIs(t, err, jobdb.ErrLeaseEventsUnsupported)
}
