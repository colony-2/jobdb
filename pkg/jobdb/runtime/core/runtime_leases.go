package runtimecore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
)

// PollWork claims typed scheduler routes and returns JobDB execution leases.
func (r *Runtime) PollWork(ctx context.Context, req jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if req.TenantId == "" {
		return nil, fmt.Errorf("tenant_id is required for PollWork")
	}
	selector, err := selectorFromCapabilities(req.Capabilities)
	if err != nil {
		return nil, err
	}
	if len(selector.JobTypes) == 0 && len(selector.Tasks) == 0 {
		return nil, nil
	}
	workerID := req.WorkerID
	if workerID == "" {
		workerID = "jobdb-core-" + ksuid.New().String()
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 1
	}
	out := make([]jobdb.ExecutionLease, 0, limit)
	attemptBudget := limit * 4
	if attemptBudget < 8 {
		attemptBudget = 8
	}
	for len(out) < limit && attemptBudget > 0 {
		attemptBudget--
		snapshots, err := r.scheduler.AcquireWork(ctx, WorkRequest{
			TenantId: req.TenantId, WorkerID: workerID, Selector: selector,
			Limit: limit - len(out), LeaseDuration: req.LeaseDuration,
			MetadataEquals: req.MetadataEquals, Now: r.now(),
		})
		if err != nil {
			return nil, err
		}
		if len(snapshots) == 0 {
			break
		}
		for _, snapshot := range snapshots {
			lease, err := r.wrapLease(snapshot)
			if err != nil {
				return nil, err
			}
			ok, err := r.preflightScheduleLease(ctx, lease)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, lease)
			}
		}
	}
	return out, nil
}

// GetJobLease claims one eligible job or task route.
func (r *Runtime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if err := req.JobKey.Validate(); err != nil {
		return nil, err
	}
	selector, err := selectorFromCapabilities(req.Capabilities)
	if err != nil {
		return nil, err
	}
	if len(selector.JobTypes) == 0 && len(selector.Tasks) == 0 {
		return nil, nil
	}
	workerID := req.WorkerID
	if workerID == "" {
		workerID = "jobdb-core-" + ksuid.New().String()
	}
	snapshot, err := r.scheduler.AcquireJobLease(ctx, JobLeaseRequest{
		JobKey: req.JobKey, WorkerID: workerID, Selector: selector,
		LeaseDuration: req.LeaseDuration, Now: r.now(),
	})
	if err != nil || snapshot == nil {
		return nil, err
	}
	lease, err := r.wrapLease(*snapshot)
	if err != nil {
		return nil, err
	}
	ok, err := r.preflightScheduleLease(ctx, lease)
	if err != nil || !ok {
		return nil, err
	}
	return lease, nil
}

func selectorFromCapabilities(capabilities []string) (WorkSelector, error) {
	var out WorkSelector
	for _, capability := range capabilities {
		if capability == "" {
			continue
		}
		if jobType, taskType, task := strings.Cut(capability, ":"); task {
			if jobType == "" || taskType == "" || strings.Contains(taskType, ":") {
				return WorkSelector{}, fmt.Errorf("invalid task capability %q", capability)
			}
			out.Tasks = append(out.Tasks, jobdb.JobTaskFilter{
				JobType: jobType, TaskType: taskType,
			})
		} else {
			out.JobTypes = append(out.JobTypes, capability)
		}
	}
	return out, nil
}

type executionLease struct {
	runtime  *Runtime
	snapshot LeaseSnapshot
	payload  json.RawMessage

	mu              sync.Mutex
	expiresAt       time.Time
	keepAliveCancel context.CancelFunc
}

var _ jobdb.ExecutionLease = (*executionLease)(nil)

func (r *Runtime) wrapLease(snapshot LeaseSnapshot) (*executionLease, error) {
	payload, err := ProjectLeasePayload(LeasePayloadProjection{
		RunPolicy: snapshot.RunPolicy, TaskWork: snapshot.TaskWork,
		Opaque: snapshot.LeasePayload, OpaquePresent: snapshot.LeasePayloadVisible,
	})
	if err != nil {
		return nil, err
	}
	return &executionLease{runtime: r, snapshot: snapshot, payload: payload,
		expiresAt: snapshot.Identity.ExpiresAt}, nil
}

func (l *executionLease) LeaseID() string { return l.snapshot.Identity.LeaseID }

func (l *executionLease) Job() jobdb.JobHandle {
	return jobdb.JobHandle{JobKey: l.snapshot.Identity.JobKey}
}

func (l *executionLease) Capability() string {
	if l.snapshot.WorkKind == WorkKindTask && l.snapshot.TaskWork != nil {
		return l.snapshot.RouteJobType + ":" + l.snapshot.TaskWork.TaskType
	}
	return l.snapshot.RouteJobType
}

func (l *executionLease) Payload() json.RawMessage {
	return append(json.RawMessage(nil), l.payload...)
}

func (l *executionLease) LeaseWorkerID() string { return l.snapshot.Identity.WorkerID }
func (l *executionLease) LeaseExpiry() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expiresAt
}
func (l *executionLease) LeaseSchemaHash() string { return l.snapshot.SchemaHash }

func (l *executionLease) KeepAlive(ctx context.Context) error {
	if l == nil || l.runtime == nil {
		return fmt.Errorf("runtime is required for lease keepalive")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.keepAliveCancel != nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	keepaliveCtx, cancel := context.WithCancel(ctx)
	l.keepAliveCancel = cancel
	interval := l.snapshot.Duration / 2
	if interval < time.Second {
		interval = time.Second
	}
	go l.renewUntilStopped(keepaliveCtx, interval)
	return nil
}

func (l *executionLease) renewUntilStopped(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			identity, duration := l.snapshot.Identity, l.snapshot.Duration
			updated, err := l.runtime.scheduler.KeepAliveLease(ctx, LeaseMutation{
				Identity: identity, Duration: duration, Now: l.runtime.now(),
			})
			if err != nil {
				return
			}
			l.mu.Lock()
			l.expiresAt = updated.Identity.ExpiresAt
			l.mu.Unlock()
		}
	}
}

func (l *executionLease) StopKeepAlive() {
	if l == nil {
		return
	}
	l.mu.Lock()
	cancel := l.keepAliveCancel
	l.keepAliveCancel = nil
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (l *executionLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	if err := l.requireCurrent(ctx); err != nil {
		return err
	}
	if err := l.runtime.ensureCompletionChapter(ctx, l.snapshot.Identity.JobKey, req); err != nil {
		return err
	}
	status, err := completionStatus(req.Status)
	if err != nil {
		return err
	}
	errorKind, retryable := completionDetails(req.Chapter)
	_, err = l.runtime.scheduler.CompleteLease(ctx, CompletionMutation{
		Identity: l.snapshot.Identity, Status: status, Detail: req.Detail,
		ErrorKind: errorKind, Retryable: retryable, Now: l.runtime.now(),
	})
	if err == nil {
		l.StopKeepAlive()
	}
	return err
}

func (l *executionLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	if err := l.requireCurrent(ctx); err != nil {
		return err
	}
	jobType, taskType, isTask := strings.Cut(req.NextNeed, ":")
	if jobType == "" || (isTask && (taskType == "" || strings.Contains(taskType, ":"))) {
		return fmt.Errorf("invalid next need %q", req.NextNeed)
	}
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	decoded, err := runtimecodec.SchedulerPayloadFromJSONView(payload)
	if err != nil {
		return err
	}
	mutation := RescheduleMutation{
		Identity: l.snapshot.Identity, RouteJobType: jobType,
		WorkKind: WorkKindJob, WaitUntil: req.WaitUntil,
		WaitForJobIDs: append([]string(nil), req.WaitForJobIDs...),
		Now:           l.runtime.now(),
	}
	if isTask {
		if decoded.TaskWait == nil || decoded.TaskWait.Next == "" || decoded.TaskWait.InputHash == "" {
			return fmt.Errorf("task route %q requires task_wait coordinates", req.NextNeed)
		}
		mutation.WorkKind = WorkKindTask
		mutation.TaskWork = &TaskWork{
			TaskType: taskType, ResumeJobType: decoded.TaskWait.Next,
			InputOrdinal:  decoded.TaskWait.InputStep,
			OutputOrdinal: decoded.TaskWait.OutputStep,
			InputHash:     decoded.TaskWait.InputHash,
		}
	}
	if len(decoded.VisiblePayload) > 0 {
		mutation.LeasePayload = decoded.VisiblePayload
	} else {
		mutation.ClearLeasePayload = true
	}
	if req.AlternateNeed != "" {
		alternateJob, alternateTask, alternateIsTask := strings.Cut(req.AlternateNeed, ":")
		if alternateJob == "" || (alternateIsTask && alternateTask == "") {
			return fmt.Errorf("invalid alternate need %q", req.AlternateNeed)
		}
		mutation.AlternateRoute = &AlternateRoute{JobType: alternateJob}
		if alternateIsTask {
			mutation.AlternateRoute.TaskType = alternateTask
		}
		if req.AlternateAfter != nil {
			mutation.AlternateRoute.After = *req.AlternateAfter
		}
	}
	_, err = l.runtime.scheduler.RescheduleLease(ctx, mutation)
	if err == nil {
		l.StopKeepAlive()
	}
	return err
}

func (l *executionLease) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	if err := l.requireCurrent(ctx); err != nil {
		return jobdb.JobHandle{}, err
	}
	req.Job.TenantId = l.snapshot.Identity.JobKey.TenantId
	return l.runtime.submitJobWithParent(ctx, req, l.snapshot.Identity.JobKey.JobId)
}

func (l *executionLease) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	if err := l.requireCurrent(ctx); err != nil {
		return jobdb.JobHandle{}, err
	}
	if req.Job.PriorJobKey.TenantId != "" && req.Job.PriorJobKey.TenantId != l.snapshot.Identity.JobKey.TenantId {
		return jobdb.JobHandle{}, fmt.Errorf("prior job tenantId must match parent tenantId")
	}
	req.Job.PriorJobKey.TenantId = l.snapshot.Identity.JobKey.TenantId
	return l.runtime.submitRestartJobWithParent(ctx, req, l.snapshot.Identity.JobKey.JobId)
}

func (l *executionLease) requireCurrent(ctx context.Context) error {
	if l == nil || l.runtime == nil {
		return fmt.Errorf("runtime is required for lease operation")
	}
	_, err := l.runtime.scheduler.ValidateLease(ctx, l.snapshot.Identity)
	return err
}

func completionStatus(status string) (string, error) {
	switch status {
	case "", "success", "succeeded":
		return "success", nil
	case "failed_app", "failed_system", "failed_timeout", "cancelled":
		return status, nil
	default:
		return "", fmt.Errorf("invalid completion status %q", status)
	}
}

func completionDetails(chapter *jobdb.Chapter) (string, *bool) {
	if chapter == nil {
		return "", nil
	}
	final, ok := chapter.Body.(jobdb.JobAttemptOutcomeChapter)
	if !ok {
		return "", nil
	}
	switch outcome := final.Outcome.(type) {
	case jobdb.AppErrorOutcome:
		return "AppError", nil
	case jobdb.SystemErrorOutcome:
		retryable := outcome.Error.Retryable
		return "SystemError", &retryable
	case jobdb.TimeoutOutcome:
		retryable := outcome.Timeout.Retryable
		return "Timeout", &retryable
	default:
		return "", nil
	}
}

func (r *Runtime) ensureCompletionChapter(ctx context.Context, key jobdb.JobKey,
	req jobdb.CompleteExecutionRequest) error {
	if req.Chapter == nil {
		return fmt.Errorf("complete lease requires final chapter")
	}
	if !runtimecodec.ChapterIs(*req.Chapter, runtimecodec.ChapterTypeJobAttemptOutcome) {
		return fmt.Errorf("complete lease chapter must be JobAttemptOutcome")
	}
	if req.Chapter.Ordinal < 0 {
		return fmt.Errorf("chapter ordinal must be nonnegative")
	}
	stored, err := r.scheduler.GetJob(ctx, key)
	if err != nil {
		return err
	}
	chapter, uploads, err := prepareChapterWrite(*req.Chapter, req.ArtifactUploads)
	if err != nil {
		return err
	}
	logKey := ChapterLogKey{JobKey: key}
	count, err := r.chapters.Count(ctx, logKey)
	if err != nil {
		return err
	}
	if chapter.Ordinal < count {
		existing, err := r.chapters.Get(ctx, logKey, chapter.Ordinal)
		if err != nil {
			return err
		}
		decoded, err := DecodeChapter(existing)
		if err != nil {
			return err
		}
		same, err := sameCompletionChapter(decoded, chapter)
		if err != nil {
			return err
		}
		if !same {
			return fmt.Errorf("%w: completion chapter %d differs", jobdb.ErrConflict, chapter.Ordinal)
		}
		return nil
	}
	if chapter.Ordinal != count {
		return fmt.Errorf("%w: completion chapter ordinal %d is not appendable", jobdb.ErrConflict, chapter.Ordinal)
	}
	if err := ValidateLastChapter(ctx, r.schemas,
		jobdb.JobSchemaKey{TenantId: key.TenantId, SchemaHash: stored.SchemaHash}, chapter); err != nil {
		return err
	}
	encoded, err := EncodeChapter(EncodeChapterRequest{Chapter: chapter, ArtifactUploads: uploads})
	if err != nil {
		return err
	}
	return r.chapters.Append(ctx, logKey, encoded)
}

func sameCompletionChapter(left, right jobdb.Chapter) (bool, error) {
	leftEncoded, err := EncodeChapter(EncodeChapterRequest{Chapter: left})
	if err != nil {
		return false, err
	}
	rightEncoded, err := EncodeChapter(EncodeChapterRequest{Chapter: right})
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftEncoded.Payload, rightEncoded.Payload) &&
		slices.Equal(left.Artifacts, right.Artifacts) &&
		reflect.DeepEqual(left.Body, right.Body), nil
}
