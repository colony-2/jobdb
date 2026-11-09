// Historical method set from f92ae253a5da7a6b81944d0bdf4cadb085772140.
// Do not regenerate from the current interfaces or embed them.
package remote

import (
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"io"
	"testing"
	"time"
)

var _ = json.RawMessage{}
var _ io.Reader
var _ time.Time

type legacyRuntime struct{}

func (legacyRuntime) CreateCell(context.Context, jobdb.CreateCellRequest) (jobdb.Cell, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) GetCell(context.Context, jobdb.CellKey) (jobdb.Cell, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ListCells(context.Context, jobdb.ListCellsRequest) (jobdb.ListCellsResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) UpdateCell(context.Context, jobdb.UpdateCellRequest) (jobdb.Cell, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) RetireCell(context.Context, jobdb.RetireCellRequest) (jobdb.Cell, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ListGoals(context.Context, jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ListDependencyWaits(context.Context, jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) GetGoal(context.Context, jobdb.GoalKey) (jobdb.GoalInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) AddGoalCandidate(context.Context, jobdb.AddGoalCandidateRequest) (jobdb.GoalCandidateInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) FailGoal(context.Context, jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) GetDependencyWait(context.Context, jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) GetConfiguration(context.Context, jobdb.GetConfigurationRequest) (jobdb.ConfigurationSnapshot, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ResolveSecrets(context.Context, jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) CancelJob(ctx context.Context, req jobdb.CancelJobRequest) error {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ClearCrashConcern(ctx context.Context, req jobdb.ClearCrashConcernRequest) error {
	panic("compile-only historical fixture")
}
func (legacyRuntime) PollWork(ctx context.Context, req jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) CompleteTaskIfWaiting(ctx context.Context, req jobdb.CompleteTaskIfWaitingRequest) error {
	panic("compile-only historical fixture")
}
func (legacyRuntime) UpsertSchedule(ctx context.Context, req jobdb.UpsertScheduleRequest) (jobdb.ScheduleInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) GetSchedule(ctx context.Context, key jobdb.ScheduleKey) (jobdb.ScheduleInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ListSchedules(ctx context.Context, req jobdb.ListSchedulesRequest) (jobdb.ListSchedulesResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) PauseSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ResumeSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ArchiveSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) TriggerSchedule(ctx context.Context, req jobdb.TriggerScheduleRequest) (jobdb.JobHandle, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ListScheduleRuns(ctx context.Context, req jobdb.ListScheduleRunsRequest) (jobdb.ListScheduleRunsResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) GetJob(ctx context.Context, jobKey jobdb.JobKey) (jobdb.JobInfo, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	panic("compile-only historical fixture")
}
func (legacyRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	panic("compile-only historical fixture")
}
func (legacyRuntime) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	panic("compile-only historical fixture")
}

func TestHistoricalPortRequiresLeaseEvents(t *testing.T) {
	if _, ok := any(legacyRuntime{}).(jobdb.WorkflowRuntime); ok {
		t.Fatal("historical implementation without lease history satisfies mandatory port")
	}
}
