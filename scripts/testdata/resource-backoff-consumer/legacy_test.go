// Method sets frozen at 67cce3c7a7954cc334b06f5d881a2b16d3f9c490.
// Do not regenerate when required interfaces grow; these are compatibility controls.
package resourceconsumer

import (
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	wf "github.com/colony-2/jobdb/pkg/workflow"
	"io"
	"log/slog"
	"time"
)

var (
	_ context.Context
	_ json.RawMessage
	_ io.Reader
	_ *slog.Logger
	_ time.Time
)

type oldRuntime struct{}

func (oldRuntime) CancelJob(ctx context.Context, req jobdb.CancelJobRequest) error {
	panic("compile-only baseline control")
}
func (oldRuntime) ClearCrashConcern(ctx context.Context, req jobdb.ClearCrashConcernRequest) error {
	panic("compile-only baseline control")
}
func (oldRuntime) CompleteTaskIfWaiting(ctx context.Context, req jobdb.CompleteTaskIfWaitingRequest) error {
	panic("compile-only baseline control")
}
func (oldRuntime) CreateCell(context.Context, jobdb.CreateCellRequest) (jobdb.Cell, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) FailGoal(context.Context, jobdb.FailGoalRequest) (jobdb.GoalInfo, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) GetCell(context.Context, jobdb.CellKey) (jobdb.Cell, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) GetConfiguration(context.Context, jobdb.GetConfigurationRequest) (jobdb.ConfigurationSnapshot, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) GetDependencyWait(context.Context, jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) GetGoal(context.Context, jobdb.GoalKey) (jobdb.GoalInfo, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) GetJob(ctx context.Context, jobKey jobdb.JobKey) (jobdb.JobInfo, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) ListCells(context.Context, jobdb.ListCellsRequest) (jobdb.ListCellsResponse, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) ListDependencyWaits(context.Context, jobdb.ListDependencyWaitsRequest) (jobdb.ListDependencyWaitsResponse, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) ListGoals(context.Context, jobdb.ListGoalsRequest) (jobdb.ListGoalsResponse, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) ListLeaseEvents(context.Context, jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) PollWork(ctx context.Context, req jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	panic("compile-only baseline control")
}
func (oldRuntime) ResolveSecrets(context.Context, jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) RetireCell(context.Context, jobdb.RetireCellRequest) (jobdb.Cell, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) StartOrAppendJob(ctx context.Context, req jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	panic("compile-only baseline control")
}
func (oldRuntime) UpdateCell(context.Context, jobdb.UpdateCellRequest) (jobdb.Cell, error) {
	panic("compile-only baseline control")
}

var _ jobdb.ConsumerRuntime = oldRuntime{}

type oldLease struct{}

func (oldLease) ClientPayload() json.RawMessage { panic("compile-only baseline control") }
func (oldLease) ClientPayloadRevision() int64   { panic("compile-only baseline control") }
func (oldLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	panic("compile-only baseline control")
}
func (oldLease) CreateCell(context.Context, jobdb.CreateCellRequest) (jobdb.Cell, error) {
	panic("compile-only baseline control")
}
func (oldLease) ExecutionState() jobdb.ExecutionState { panic("compile-only baseline control") }
func (oldLease) GetConfiguration(context.Context) (jobdb.ConfigurationSnapshot, error) {
	panic("compile-only baseline control")
}
func (oldLease) Job() jobdb.JobHandle                { panic("compile-only baseline control") }
func (oldLease) KeepAlive(ctx context.Context) error { panic("compile-only baseline control") }
func (oldLease) LeaseID() string                     { panic("compile-only baseline control") }
func (oldLease) LookupCell(context.Context, string) (jobdb.ResolvedCell, error) {
	panic("compile-only baseline control")
}
func (oldLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	panic("compile-only baseline control")
}
func (oldLease) ResolveCell(context.Context, string) (jobdb.ResolvedCell, error) {
	panic("compile-only baseline control")
}
func (oldLease) ResolveSecrets(context.Context, []string) (jobdb.SecretResolveResult, error) {
	panic("compile-only baseline control")
}
func (oldLease) Route() jobdb.Route { panic("compile-only baseline control") }
func (oldLease) StartOrAppendJob(ctx context.Context, req jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	panic("compile-only baseline control")
}
func (oldLease) StopKeepAlive() { panic("compile-only baseline control") }
func (oldLease) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	panic("compile-only baseline control")
}
func (oldLease) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	panic("compile-only baseline control")
}

var _ jobdb.ExecutionLease = oldLease{}

type oldContext struct{}

func (oldContext) AwaitDependencies(...jobdb.Dependency) ([]jobdb.DependencyResolution, error) {
	panic("compile-only baseline control")
}
func (oldContext) AwaitDuration(wf.Duration) error { panic("compile-only baseline control") }
func (oldContext) AwaitGoals(...string) ([]jobdb.DependencyResolution, error) {
	panic("compile-only baseline control")
}
func (oldContext) AwaitJobs(...string) error      { panic("compile-only baseline control") }
func (oldContext) ClientPayload() json.RawMessage { panic("compile-only baseline control") }
func (oldContext) ClientPayloadRevision() int64   { panic("compile-only baseline control") }
func (oldContext) Context() context.Context       { panic("compile-only baseline control") }
func (oldContext) DoScope(string, wf.ScopeOptions, func(wf.ExecutionContext) (wf.JobData, error)) (wf.JobData, error) {
	panic("compile-only baseline control")
}
func (oldContext) DoTask(wf.TaskCall, func(context.Context) (wf.TaskData, error)) (wf.TaskData, error) {
	panic("compile-only baseline control")
}
func (oldContext) ExecutionVersions() jobdb.ExecutionVersions { panic("compile-only baseline control") }
func (oldContext) GetConfiguration(context.Context) (wf.ConfigurationSnapshot, error) {
	panic("compile-only baseline control")
}
func (oldContext) GetJobKey() wf.JobKey { panic("compile-only baseline control") }
func (oldContext) Job() wf.JobHandle    { panic("compile-only baseline control") }
func (oldContext) Logger() *slog.Logger { panic("compile-only baseline control") }
func (oldContext) ResolveSecrets(context.Context, []string) (wf.SecretResolveResult, error) {
	panic("compile-only baseline control")
}
func (oldContext) ResolveSecretsFresh(context.Context, []string) (wf.SecretResolveResult, error) {
	panic("compile-only baseline control")
}
func (oldContext) StartOrAppendJob(context.Context, wf.StartOrAppendJob) (wf.JobHandle, error) {
	panic("compile-only baseline control")
}
func (oldContext) SubmitJob(context.Context, wf.SubmitJob) (wf.JobHandle, error) {
	panic("compile-only baseline control")
}
func (oldContext) SubmitRestartJob(context.Context, wf.SubmitRestartJobRequest) (wf.JobHandle, error) {
	panic("compile-only baseline control")
}
func (oldContext) Yield(context.Context, wf.YieldRequest) error {
	panic("compile-only baseline control")
}

var _ wf.ExecutionContext = oldContext{}
