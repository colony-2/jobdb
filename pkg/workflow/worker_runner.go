package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type workerRunnerOptions struct {
	BeforeRun      func(context.Context, ExecutionLease) error
	Logger         *slog.Logger
	JobPolicy      RunPolicy
	WorkerID       string
	JobKey         JobKey
	GoalID         string
	Observer       ReplayObserver
	Replay         bool
	AwaitThreshold time.Duration
}

type workerRunner struct {
	handoff   *handoffState
	beforeRun func(context.Context, ExecutionLease) error
	runtime   WorkflowRuntime
	worker    *WorkSet
	lease     ExecutionLease
	logger    *slog.Logger

	jobPolicy       RunPolicy
	workerID        string
	observer        ReplayObserver
	replay          bool
	replayJobStatus jobdb.JobStatus
	awaitThreshold  time.Duration

	ctx                context.Context
	jobKey             JobKey
	goalID             string
	configurationCache attemptCache[*ConfigurationSnapshot]
	secretCache        *attemptCache[map[string]*SecretValue]
	secretCacheInit    sync.Once
	executionSecrets   *executionSecretResolver
}

func newWorkerRunner(runtime WorkflowRuntime, ws *WorkSet, lease ExecutionLease, opts workerRunnerOptions) *workerRunner {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	jobKey := JobKey{}
	goalID := opts.GoalID
	if lease != nil {
		jobKey = lease.Job().JobKey
		goalID = lease.Job().GoalID
	} else if opts.JobKey != (JobKey{}) {
		jobKey = opts.JobKey
	}
	return &workerRunner{
		handoff:        &handoffState{},
		runtime:        runtime,
		beforeRun:      opts.BeforeRun,
		worker:         ws,
		lease:          lease,
		logger:         logger,
		jobPolicy:      normalizeRunPolicy(opts.JobPolicy),
		workerID:       opts.WorkerID,
		observer:       opts.Observer,
		replay:         opts.Replay,
		awaitThreshold: opts.AwaitThreshold,
		jobKey:         jobKey,
		goalID:         goalID,
	}
}

func (r *workerRunner) GetJobKey() JobKey {
	return r.jobKey
}

func (r *workerRunner) Logger() *slog.Logger {
	return r.logger
}

func (r *workerRunner) observerOrNoop() ReplayObserver {
	if r.observer != nil {
		return r.observer
	}
	return noopReplayObserver{}
}

func (r *workerRunner) emitJobStart(attempt int, input JobData, at time.Time) {
	r.observerOrNoop().OnJobStart(JobStartEvent{
		JobKey:        r.GetJobKey(),
		AttemptNumber: attempt,
		Input:         input,
		At:            at,
	})
}

func (r *workerRunner) emitJobEnd(attempt int, output JobData, err error, at time.Time) {
	r.observerOrNoop().OnJobEnd(JobEndEvent{
		JobKey:        r.GetJobKey(),
		AttemptNumber: attempt,
		Output:        output,
		Err:           err,
		At:            at,
	})
}

func panicToAppError(rec interface{}) error {
	return &AppError{Payload: AppErrorPayload{Message: fmt.Sprintf("panic: %v", rec), Level: "error"}}
}

func (r *workerRunner) getChapter(ctx context.Context, ordinal int64) (Chapter, chapterMeta, error) {
	chapter, err := r.runtime.GetChapter(ctx, ChapterRef{
		JobKey:  r.GetJobKey(),
		Ordinal: ordinal,
	})
	if err != nil {
		return Chapter{}, chapterMeta{}, err
	}
	meta, err := chapterMetaFromChapter(chapter)
	if err != nil {
		return Chapter{}, chapterMeta{}, err
	}
	return chapter, meta, nil
}

func (r *workerRunner) awaitJobsComplete(ctx context.Context, jobIds []string) (bool, error) {
	if len(jobIds) == 0 {
		return true, nil
	}
	for _, id := range jobIds {
		if id == "" {
			return false, fmt.Errorf("jobId cannot be empty")
		}
		job, err := r.runtime.GetJob(ctx, JobKey{TenantId: r.GetJobKey().TenantId, JobId: id})
		if errors.Is(err, ErrJobNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if job.Status != JobStatusCompleted && job.Status != JobStatusCancelled {
			return false, nil
		}
	}
	return true, nil
}

func (r *workerRunner) SubmitJob(ctx context.Context, submit SubmitJob) (JobHandle, error) {
	if r.ctx != nil && context.Cause(r.ctx) != nil {
		return JobHandle{}, context.Cause(r.ctx)
	}
	if r.lease == nil || r.replay {
		return JobHandle{}, fmt.Errorf("submitting jobs requires an active execution lease")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if submit.TenantId == "" {
		submit.TenantId = r.GetJobKey().TenantId
	}
	handle, err := r.lease.SubmitJob(ctx, SubmitJobRequest{
		Job:         submit,
		RequestTime: nowUTC(),
		WorkerID:    r.workerID,
	})
	if err != nil {
		return JobHandle{}, err
	}
	return handle, nil
}

func (r *workerRunner) SubmitRestartJob(ctx context.Context, restart SubmitRestartJobRequest) (JobHandle, error) {
	if r.ctx != nil && context.Cause(r.ctx) != nil {
		return JobHandle{}, context.Cause(r.ctx)
	}
	if r.lease == nil || r.replay {
		return JobHandle{}, fmt.Errorf("submitting restart jobs requires an active execution lease")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if restart.Job.PriorJobKey.TenantId == "" {
		restart.Job.PriorJobKey.TenantId = r.GetJobKey().TenantId
	}
	if restart.WorkerID == "" {
		restart.WorkerID = r.workerID
	}
	if restart.RequestTime.IsZero() {
		restart.RequestTime = nowUTC()
	}
	handle, err := r.lease.SubmitRestartJob(ctx, restart)
	if err != nil {
		return JobHandle{}, err
	}
	return handle, nil
}

func (r *workerRunner) loadInitialChapterAndPolicy(ctx context.Context) (TaskData, Chapter, chapterMeta, error) {
	chapter, meta, err := r.getChapter(ctx, 0)
	if err != nil {
		return nil, Chapter{}, chapterMeta{}, err
	}
	if !chapterIs(chapter, chapterTypeJobStart) {
		got, _ := chapterType(chapter)
		return nil, Chapter{}, chapterMeta{}, fmt.Errorf("%w: unexpected chapter type %q at ordinal 0", ErrWorkflowNotDeterministic, got)
	}
	if meta.RunPolicy != nil {
		r.jobPolicy = mergeRunPolicy(*meta.RunPolicy, r.jobPolicy)
	}
	inputData, err := chapterToTaskData(r.runtime, r.GetJobKey(), chapter)
	if err != nil {
		return nil, Chapter{}, chapterMeta{}, err
	}
	return inputData, chapter, meta, nil
}

func (r *workerRunner) completeLeaseWithChapter(ctx context.Context, chapter Chapter, err error) error {
	if r.replay {
		return ErrReplayShouldNeverMutate
	}
	if r.lease == nil {
		return fmt.Errorf("lease is required")
	}
	status, detail := completionStatusAndDetail(err)
	if completeErr := r.lease.Complete(ctx, CompleteExecutionRequest{
		Status:          status,
		Detail:          detail,
		Chapter:         &chapter,
		ArtifactUploads: nil,
	}); completeErr != nil {
		return completeErr
	}
	return nil
}

type prereqFailedError struct {
	app AppError
}

func newPrereqFailedError(message string) error {
	return &prereqFailedError{
		app: AppError{Payload: AppErrorPayload{Message: message, Level: "error"}},
	}
}

func (e *prereqFailedError) Error() string      { return e.app.Error() }
func (e *prereqFailedError) NonRetryable() bool { return true }
func (e *prereqFailedError) Unwrap() error      { return e.app }

func (r *workerRunner) checkPrerequisites(ctx context.Context, meta chapterMeta) error {
	for _, prereq := range meta.Prerequisites {
		if prereq.Condition != JobPrereqSuccess {
			continue
		}
		key := JobKey{TenantId: r.GetJobKey().TenantId, JobId: prereq.JobID}
		job, err := r.runtime.GetJob(ctx, key)
		if err != nil {
			return fmt.Errorf("check prerequisite job %s: %w", prereq.JobID, err)
		}
		if job.Status != JobStatusCompleted {
			return newPrereqFailedError(fmt.Sprintf("prerequisite job %s not completed", prereq.JobID))
		}
		if job.Data == nil {
			return newPrereqFailedError(fmt.Sprintf("prerequisite job %s did not expose result data", prereq.JobID))
		}
		if _, err := job.Data.GetData(); err != nil {
			msg := fmt.Sprintf("prerequisite job %s did not succeed", prereq.JobID)
			if err.Error() != "" {
				msg += ": " + err.Error()
			}
			return newPrereqFailedError(msg)
		}
	}
	return nil
}

func (r *workerRunner) DoJob(ctx context.Context) (output JobData, runErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var session *leaseSession
	if r.lease != nil && !r.replay {
		if renewable, ok := r.lease.(jobdb.RenewableExecutionLease); ok {
			var err error
			session, err = startLeaseSession(ctx, renewable)
			if err != nil {
				return nil, err
			}
			ctx = session.ctx
			r.lease = session
			r.jobPolicy = normalizeRunPolicy(session.ExecutionState().RunPolicy)
			r.workerID = session.LeaseWorkerID()
			defer func() {
				// Capture the failure before cleanup cancels the execution context.
				session.stop()
				<-session.done
				cause := context.Cause(session.ctx)
				session.close()
				if cause != nil {
					output, runErr = nil, cause
				}
			}()
		} else {
			if err := r.lease.KeepAlive(ctx); err != nil {
				return nil, &jobdb.LeaseRenewalError{Err: err}
			}
			defer r.lease.StopKeepAlive()
		}
	}
	ctx, closeExecution := context.WithCancel(ctx)
	defer closeExecution()
	if !r.replay {
		r.executionSecrets = &executionSecretResolver{owner: r, bootstrap: true}
		ctx = context.WithValue(ctx, secretResolverKey{}, r.executionSecrets)
	}
	r.ctx = ctx
	if r.beforeRun != nil {
		if err := r.beforeRun(ctx, r.lease); err != nil {
			return nil, err
		}
		if session != nil && session.isReleased() {
			return nil, nil
		}
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}

	inputData, _, meta, err := r.loadInitialChapterAndPolicy(ctx)
	if err != nil {
		return nil, err
	}
	return r.runExecution(ctx, r.worker.JobWorker, inputData, meta)
}

func completionStatusAndDetail(err error) (string, string) {
	if err == nil {
		return "success", ""
	}
	if errors.Is(err, ErrJobCancelled) || errors.Is(err, context.Canceled) {
		return "cancelled", err.Error()
	}
	var te TimeoutError
	if errors.As(err, &te) {
		return "failed_timeout", messageOrFallback(te.Payload.Message, err)
	}
	var ae AppError
	if errors.As(err, &ae) {
		return "failed_app", messageOrFallback(ae.Payload.Message, err)
	}
	var se SystemError
	if errors.As(err, &se) {
		return "failed_system", messageOrFallback(se.Payload.Message, err)
	}
	return "failed_system", messageOrFallback("", err)
}

func messageOrFallback(message string, err error) string {
	if message != "" {
		return message
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func isReplayCacheMiss(err error) bool {
	var miss ReplayCacheMissError
	return errors.As(err, &miss)
}

type noopReplayObserver struct{}

func (noopReplayObserver) OnJobStart(JobStartEvent)   {}
func (noopReplayObserver) OnTaskStart(TaskStartEvent) {}
func (noopReplayObserver) OnTaskEnd(TaskEndEvent)     {}
func (noopReplayObserver) OnJobEnd(JobEndEvent)       {}

func mustMarshalJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

// ClientPayload returns the payload snapshot acquired with this invocation's lease.
func (r *workerRunner) ClientPayload() json.RawMessage {
	if r.lease == nil || r.replay {
		return nil
	}
	return r.lease.ClientPayload()
}
func (r *workerRunner) ClientPayloadRevision() int64 {
	if r.lease == nil || r.replay {
		return 0
	}
	return r.lease.ClientPayloadRevision()
}

func (r *workerRunner) prepareJobResultPayload(output JobData, originalErr error, inputRef *InputReference) (json.RawMessage, []Artifact, string, error) {
	artifacts := []Artifact{}
	if originalErr != nil {
		payload, kind, err := errorPayloadFromError(originalErr, inputRef)
		if err != nil {
			return nil, nil, "", err
		}
		if output != nil {
			if arts, artsErr := output.GetArtifacts(); artsErr == nil {
				artifacts = arts
			}
		}
		return payload, artifacts, kind, nil
	}
	if output == nil {
		raw, _ := json.Marshal(SystemErrorPayload{Message: "missing job output", InputRef: inputRef})
		return json.RawMessage(raw), artifacts, payloadKindSystemError, nil
	}
	dataBytes, err := output.GetData()
	if err != nil {
		return nil, nil, "", err
	}
	artifacts, err = output.GetArtifacts()
	if err != nil {
		return nil, nil, "", err
	}
	return dataBytes, artifacts, payloadKindApp, nil
}

type taskResult struct {
	output TaskData
	err    error
}
