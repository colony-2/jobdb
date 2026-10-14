package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
)

var errExecutionSuspended = errors.New("execution suspended")
var errRecordedBoundary = errors.New("recorded enclosing boundary ended")

type executionControlError struct{ cause error }

func (e *executionControlError) Error() string { return e.cause.Error() }
func (e *executionControlError) Unwrap() error { return e.cause }

type executionEvent struct {
	record  jobdb.ExecutionRecord
	ordinal int64
}
type executionRun struct {
	jobInput        *jobdb.ExecutionSnapshot
	observerMu      sync.Mutex
	suspended       bool
	restarts        map[string]*jobdb.ExecutionRecord
	restartApplied  bool
	initialVersions jobdb.ExecutionVersions
	budgets         map[string]continuationBudget
	r               *workerRunner
	mu              sync.Mutex
	events          []executionEvent
	cursor          int
	ordinal         int64
	activation      string
	activated       bool
	versions        jobdb.ExecutionVersions
	fatal           error
	live            bool
	active          map[*executionContext]bool
	final           *Chapter
	restart         *jobdb.ExecutionRecord
}
type executionContext struct {
	policy        executionPolicy
	attempt       int
	run           *executionRun
	ctx           context.Context
	cancel        context.CancelCauseFunc
	parent        *executionContext
	id            string
	deadline      time.Time
	totalDeadline time.Time
	timer         *time.Timer
	counts        map[string]int
	calls         sync.Mutex
	cache         *workerRunner
}

func jobdbBuildVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, d := range info.Deps {
			if d.Path == "github.com/colony-2/jobdb" {
				return d.Version
			}
		}
		if info.Main.Path == "github.com/colony-2/jobdb" && info.Main.Version != "" {
			return info.Main.Version
		}
	}
	return "(devel)"
}
func (e *executionRun) control(err error) error {
	if err == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.controlLocked(err)
}
func isExecutionControl(err error) bool {
	var c *executionControlError
	return errors.As(err, &c) || errors.Is(err, errExecutionSuspended) || errors.Is(err, errRecordedBoundary) || errors.Is(err, context.Canceled) || IsExecutionLeaseLost(err) || isReplayCacheMiss(err)
}
func (e *executionRun) peekLocked() *executionEvent {
	for e.cursor < len(e.events) && (e.events[e.cursor].record.Kind == "activation" || e.events[e.cursor].record.Kind == "restart") {
		e.cursor++
	}
	if e.cursor < len(e.events) {
		return &e.events[e.cursor]
	}
	return nil
}
func (e *executionRun) peek() *executionEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	v := e.peekLocked()
	if v == nil {
		return nil
	}
	copy := *v
	return &copy
}
func (c *executionContext) checkLocked() error {
	if c.run.suspended {
		return errExecutionSuspended
	}
	if c.run.fatal != nil {
		return c.run.fatal
	}
	if c.run.live {
		// Timer delivery may lag the deadline. Admission and callback completion
		// must enforce the budget even before the timer goroutine is scheduled.
		c.armLocked()
	}
	if err := context.Cause(c.ctx); err != nil {
		return err
	}
	return nil
}
func (c *executionContext) liveLocked() error {
	e := c.run
	if e.restart != nil && !e.restartApplied {
		return c.checkLocked()
	}
	if e.r.replay {
		return ReplayCacheMissError{JobKey: e.r.jobKey, Ordinal: e.ordinal, Reason: ReplayCacheMissTaskResultMissing}
	}
	if !e.live {
		e.live = true
		for scope := range e.active {
			scope.armLocked()
		}
	}
	return c.checkLocked()
}
func (c *executionContext) armLocked() {
	if c.deadline.IsZero() {
		return
	}
	err := NewTimeoutError("scope", 0, TimeoutScopeInvocation, nil, true)
	if !c.totalDeadline.IsZero() && !c.deadline.Before(c.totalDeadline) {
		err = NewTimeoutError("scope", 0, TimeoutScopeTotal, nil, false)
	}
	if !c.deadline.After(time.Now()) {
		c.cancel(err)
		return
	}
	if c.timer != nil {
		return
	}
	c.timer = time.AfterFunc(time.Until(c.deadline), func() { c.cancel(err) })
}
func (c *executionContext) child(id string, deadline, total time.Time) *executionContext {
	ctx, cancel := context.WithCancelCause(c.ctx)
	out := &executionContext{run: c.run, ctx: ctx, cancel: cancel, parent: c, id: id, deadline: deadline, totalDeadline: total, counts: map[string]int{}}
	e := c.run
	e.mu.Lock()
	defer e.mu.Unlock()
	e.active[out] = true
	if e.live {
		out.armLocked()
	}
	return out
}
func (c *executionContext) close() {
	e := c.run
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.active, c)
	if c.timer != nil {
		c.timer.Stop()
	}
	c.cancel(context.Canceled)
}
func (c *executionContext) occurrence(kind, site string) (string, error) {
	if err := context.Cause(c.ctx); err != nil {
		return "", err
	}
	if !c.calls.TryLock() {
		return "", c.run.control(fmt.Errorf("%w: concurrent calls in scope %s", ErrWorkflowNotDeterministic, c.id))
	}
	if site == "" {
		c.calls.Unlock()
		return "", fmt.Errorf("call site is required")
	}
	key := kind + ":" + site
	c.counts[key]++
	encoded, _ := json.Marshal(site)
	return fmt.Sprintf("%s/%s:%s#%d", c.id, kind, encoded, c.counts[key]), nil
}

// event consumes the exact expected record or commits it before advancing.
// The callback is only evaluated for live records (clock/predicates/builders
// must never be consulted while replaying recorded decisions).
func (c *executionContext) event(kind, id string, build func() *jobdb.ExecutionRecord) (jobdb.ExecutionRecord, error) {
	e := c.run
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := c.checkLocked(); err != nil {
		return jobdb.ExecutionRecord{}, err
	}
	if next := e.peekLocked(); next != nil {
		if next.record.Kind != kind || next.record.ID != id {
			if next.record.Kind == "result" && next.record.ID != id && next.record.ErrorKind == payloadKindTimeout && strings.HasPrefix(id, next.record.ID+"/") {
				return jobdb.ExecutionRecord{}, errRecordedBoundary
			}
			err := fmt.Errorf("%w: expected %s %s, recorded %s %s at %d", ErrWorkflowNotDeterministic, kind, id, next.record.Kind, next.record.ID, next.ordinal)
			return jobdb.ExecutionRecord{}, e.controlLocked(err)
		}
		e.cursor++
		return next.record, nil
	}
	if e.restart != nil && !e.restartApplied && kind != "continuation" {
		return jobdb.ExecutionRecord{}, e.controlLocked(fmt.Errorf("%w: restart target %s was not reached", ErrWorkflowNotDeterministic, e.restart.ID))
	}
	if err := c.liveLocked(); err != nil {
		return jobdb.ExecutionRecord{}, err
	}
	if !e.activated {
		activation := jobdb.ExecutionRecord{Kind: "activation", Versions: &e.versions, Activation: e.activation}
		if err := e.appendLocked(&activation); err != nil {
			return jobdb.ExecutionRecord{}, err
		}
		e.activated = true
	}
	rec := build()
	rec.Kind = kind
	rec.ID = id
	if err := e.appendLocked(rec); err != nil {
		return jobdb.ExecutionRecord{}, err
	}
	return *rec, nil
}
func (e *executionRun) appendLocked(rec *jobdb.ExecutionRecord) error {
	rec.Version = e.initialVersions.ExecutionPatternVersion
	rec.Activation = e.activation
	if rec.At.IsZero() {
		rec.At = time.Now().UTC()
	}
	var uploads []ArtifactUpload
	for label, snapshot := range map[string]*jobdb.ExecutionSnapshot{"input": rec.Input, "output": rec.Output} {
		if snapshot == nil {
			continue
		}
		for i := range snapshot.Artifacts {
			art := &snapshot.Artifacts[i]
			if art.Key != nil {
				continue
			}
			name := fmt.Sprintf("execution-%s-%d-%s", label, i, art.Name)
			art.Key = &ArtifactKey{JobId: e.r.jobKey.JobId, TaskOrdinal: e.ordinal, Name: name, SizeBytes: int64(len(art.Data))}
			data := append([]byte(nil), art.Data...)
			uploads = append(uploads, ArtifactUpload{Name: name, Size: int64(len(data)), Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(data)), nil }})
		}
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err = jobdb.DecodeExecutionRecord(raw); err != nil {
		return err
	}
	ch := Chapter{Ordinal: e.ordinal, TaskType: "__execution__", CreatedAt: rec.At, Body: jobdb.ExecutionChapter{Record: raw}}
	ref := ChapterRef{JobKey: e.r.jobKey, Ordinal: e.ordinal}
	err = e.r.runtime.PutChapter(e.r.ctx, PutChapterRequest{LeaseID: e.r.lease.LeaseID(), LeaseToken: executionLeaseToken(e.r.lease), Ref: ref, Chapter: ch, ArtifactUploads: uploads})
	if err != nil {
		// A lost acknowledgement must not cause execution to repeat a committed step.
		got, readErr := e.r.runtime.GetChapter(e.r.ctx, ref)
		body, ok := got.Body.(jobdb.ExecutionChapter)
		if readErr != nil || !ok || !bytes.Equal(body.Record, raw) {
			return e.controlLocked(fmt.Errorf("execution append at %d: %w", e.ordinal, err))
		}
	}
	e.ordinal++
	return nil
}
func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if !json.Valid(raw) {
		return nil, fmt.Errorf("command must be valid JSON")
	}
	if err := d.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
func executionSnapshot(ctx context.Context, data TaskData) (*jobdb.ExecutionSnapshot, error) {
	if data == nil {
		data = NewTaskDataOrPanic(nil)
	}
	raw, err := data.GetData()
	if err != nil {
		return nil, err
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("task data must be valid JSON")
	}
	out := &jobdb.ExecutionSnapshot{Data: append(json.RawMessage(nil), raw...)}
	arts, err := data.GetArtifacts()
	if err != nil {
		return nil, err
	}
	defer cleanupArtifacts(arts, nil)
	names := map[string]bool{}
	for _, a := range arts {
		if a == nil || a.Name() == "" {
			return nil, fmt.Errorf("artifact name is required")
		}
		if names[a.Name()] {
			return nil, fmt.Errorf("duplicate artifact %q", a.Name())
		}
		names[a.Name()] = true
		b, err := a.Bytes(ctx)
		if err != nil {
			return nil, err
		}
		artifact := jobdb.ExecutionArtifact{Name: a.Name(), Data: append([]byte(nil), b...)}
		if key, err := a.ArtifactKey(); err == nil {
			artifact.Key = &key
		}
		out.Artifacts = append(out.Artifacts, artifact)
	}
	return out, nil
}
func snapshotData(s *jobdb.ExecutionSnapshot) TaskData {
	if s == nil {
		return nil
	}
	d := &SimpleTaskData{Data: append(Data(nil), s.Data...)}
	for _, a := range s.Artifacts {
		artifact := jobdb.NewArtifactFromBytes(a.Name, append([]byte(nil), a.Data...))
		if a.Key != nil {
			jobdb.AssignArtifactKey(artifact, *a.Key)
		}
		d.Artifacts = append(d.Artifacts, artifact)
	}
	return d
}
func recordResult(ctx context.Context, out TaskData, err error) (*jobdb.ExecutionRecord, error) {
	rec := &jobdb.ExecutionRecord{}
	if err != nil {
		raw, kind, e := errorPayloadFromError(err, nil)
		if e != nil {
			return nil, e
		}
		rec.Error = raw
		rec.ErrorKind = kind
		return rec, nil
	}
	s, e := executionSnapshot(ctx, out)
	rec.Output = s
	return rec, e
}
func resultData(rec jobdb.ExecutionRecord) (TaskData, error) {
	if rec.ErrorKind != "" {
		_, err := envelopeToTaskData(chapterEnvelope{PayloadKind: rec.ErrorKind, Payload: rec.Error}, nil)
		return nil, err
	}
	return snapshotData(rec.Output), nil
}
func (c *executionContext) ExecutionVersions() jobdb.ExecutionVersions {
	v := c.run.initialVersions
	v.ConsumerVersions = maps.Clone(v.ConsumerVersions)
	return v
}
func (r *workerRunner) runExecution(ctx context.Context, w ExecutionWorker, input JobData, meta chapterMeta) (JobData, error) {
	if w.Execute == nil {
		return nil, fmt.Errorf("execution worker callback is required")
	}
	chapters, err := r.runtime.ListChapters(ctx, ListChaptersRequest{JobKey: r.jobKey})
	if err != nil {
		return nil, err
	}
	e := &executionRun{restarts: map[string]*jobdb.ExecutionRecord{}, r: r, ordinal: int64(len(chapters)), activation: ksuid.New().String(), active: map[*executionContext]bool{}, versions: jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion, JobDBVersion: jobdbBuildVersion(), ConsumerVersions: maps.Clone(r.worker.Options.ConsumerVersions)}}
	for i, ch := range chapters {
		if i == 0 {
			continue
		}
		switch body := ch.Body.(type) {
		case jobdb.ExecutionChapter:
			rec, err := jobdb.DecodeExecutionRecord(body.Record)
			if err != nil {
				return nil, fmt.Errorf("%w: %v (worker versions %+v)", ErrWorkflowNotDeterministic, err, e.versions)
			}
			e.events = append(e.events, executionEvent{rec, ch.Ordinal})
			if rec.Kind == "restart" {
				copy := rec
				e.restart = &copy
				e.restarts[rec.ID] = &copy
			}
		case JobAttemptOutcomeChapter:
			copy := ch
			e.final = &copy
		default:
			return nil, fmt.Errorf("%w: expected execution history at %d, got %T", ErrWorkflowNotDeterministic, ch.Ordinal, ch.Body)
		}
	}
	rootCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	root := &executionContext{run: e, ctx: rootCtx, cancel: cancel, id: "", counts: map[string]int{}}
	e.jobInput, err = executionSnapshot(ctx, input)
	if err != nil {
		return nil, err
	}
	e.initialVersions = e.versions
	if len(e.events) == 0 {
		if r.replay {
			return nil, ReplayCacheMissError{JobKey: r.jobKey, Ordinal: 1, Reason: ReplayCacheMissJobResultMissing}
		}
		rec := jobdb.ExecutionRecord{Kind: "header", Versions: &e.versions}
		if err = e.appendLocked(&rec); err != nil {
			return nil, err
		}
	} else {
		if e.events[0].record.Kind != "header" {
			return nil, fmt.Errorf("execution header missing")
		}
		e.cursor = 1
		e.initialVersions = *e.events[0].record.Versions
		e.versions.ExecutionPatternVersion = e.initialVersions.ExecutionPatternVersion
		for _, event := range e.events {
			if event.record.Version != e.initialVersions.ExecutionPatternVersion {
				return nil, fmt.Errorf("%w: execution record version differs from pinned header", ErrWorkflowNotDeterministic)
			}
		}
	}
	if len(meta.Prerequisites) > 0 {
		admission, err := root.event("admission", "$job", func() *jobdb.ExecutionRecord {
			failure := r.checkPrerequisites(ctx, meta)
			record, _ := recordResult(ctx, NewTaskDataOrPanic(true), failure)
			return record
		})
		if err != nil {
			return nil, err
		}
		if _, failure := resultData(admission); failure != nil {
			if e.final != nil {
				recorded, recordErr := chapterToTaskData(r.runtime, r.jobKey, *e.final)
				if !r.replay {
					if err := r.completeLeaseWithChapter(ctx, *e.final, recordErr); err != nil {
						return nil, err
					}
				}
				return recorded, recordErr
			}
			if r.replay {
				return nil, ReplayCacheMissError{JobKey: r.jobKey, Ordinal: e.ordinal, Reason: ReplayCacheMissJobResultMissing}
			}
			return r.completeExecutionFailure(ctx, e.ordinal, w.Name(), failure)
		}
	}
	policy := r.jobPolicy
	out, err := root.DoScope("$job", ScopeOptions{TotalTimeout: durationPtrToDuration(policy.TotalTimeout), AttemptTimeout: durationPtrToDuration(policy.InvocationTimeout), Retry: policy.Retry}, func(c ExecutionContext) (JobData, error) { return w.Execute(c, snapshotData(e.jobInput)) })
	if errors.Is(err, errExecutionSuspended) {
		return nil, nil
	}
	e.mu.Lock()
	fatal := e.fatal
	e.mu.Unlock()
	if fatal != nil {
		if !r.replay && errors.Is(fatal, ErrWorkflowNotDeterministic) {
			return r.completeExecutionFailure(ctx, e.ordinal, w.Name(), fatal)
		}
		return nil, fatal
	}
	if isExecutionControl(err) {
		return nil, err
	}
	if next := e.peek(); next != nil {
		return nil, fmt.Errorf("%w: unconsumed execution record at %d", ErrWorkflowNotDeterministic, next.ordinal)
	}
	if e.final != nil {
		recorded, recordErr := chapterToTaskData(r.runtime, r.jobKey, *e.final)
		if !r.replay {
			if completeErr := r.completeLeaseWithChapter(ctx, *e.final, recordErr); completeErr != nil {
				return nil, completeErr
			}
		}
		return recorded, recordErr
	}
	if r.replay {
		return nil, ReplayCacheMissError{JobKey: r.jobKey, Ordinal: e.ordinal, Reason: ReplayCacheMissJobResultMissing}
	}
	payload, arts, kind, prepErr := r.prepareJobResultPayload(out, err, nil)
	if prepErr != nil {
		return nil, prepErr
	}
	ch, uploads, prepErr := prepareTaskDataChapterWrite(ctx, ChapterRef{JobKey: r.jobKey, Ordinal: e.ordinal}, w.Name(), chapterTypeJobAttemptOutcome, kind, "", time.Now().UTC(), chapterMeta{Attempt: 1}, payload, arts)
	if prepErr != nil {
		return nil, prepErr
	}
	status, detail := completionStatusAndDetail(err)
	if completeErr := r.lease.Complete(ctx, CompleteExecutionRequest{Status: status, Detail: detail, Chapter: &ch, ArtifactUploads: uploads}); completeErr != nil {
		return nil, completeErr
	}
	return out, err
}

func (e *executionRun) nextOrdinal() int64 { e.mu.Lock(); defer e.mu.Unlock(); return e.ordinal }
func (e *executionRun) continuationBudget(id string) (continuationBudget, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	scopeID := id[:strings.LastIndex(id, "/attempt:")]
	for key, budget := range e.budgets {
		i := strings.LastIndex(key, "/attempt:")
		if i >= 0 && key[:i] == scopeID {
			return budget, true
		}
	}
	return continuationBudget{}, false
}
func (e *executionRun) suspend() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.suspended = true
	for c := range e.active {
		c.cancel(errExecutionSuspended)
	}
	return errExecutionSuspended
}
func (r *workerRunner) completeExecutionFailure(ctx context.Context, ordinal int64, name string, failure error) (JobData, error) {
	payload, _, kind, err := r.prepareJobResultPayload(nil, failure, nil)
	if err != nil {
		return nil, err
	}
	chapter, uploads, err := prepareTaskDataChapterWrite(ctx, ChapterRef{JobKey: r.jobKey, Ordinal: ordinal}, name, chapterTypeJobAttemptOutcome, kind, "", time.Now().UTC(), chapterMeta{Attempt: 1}, payload, nil)
	if err != nil {
		return nil, err
	}
	status, detail := completionStatusAndDetail(failure)
	if err = r.lease.Complete(ctx, CompleteExecutionRequest{Status: status, Detail: detail, Chapter: &chapter, ArtifactUploads: uploads}); err != nil {
		return nil, err
	}
	return nil, failure
}

func (e *executionRun) resultOrdinal(id string) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, event := range e.events {
		if event.record.Kind == "result" && event.record.ID == id {
			return event.ordinal
		}
	}
	return e.ordinal - 1
}
func (e *executionRun) emitTask(call TaskCall, id string, attempt int, start time.Time, rec jobdb.ExecutionRecord) {
	ordinal := e.resultOrdinal(id)
	out, err := resultData(rec)
	e.observerMu.Lock()
	defer e.observerMu.Unlock()
	observer := e.r.observerOrNoop()
	observer.OnTaskStart(TaskStartEvent{InvocationID: id, JobKey: e.r.jobKey, TaskType: call.TaskType, Ordinal: ordinal, AttemptNumber: attempt, Input: snapshotData(rec.Input), At: start})
	observer.OnTaskEnd(TaskEndEvent{InvocationID: id, JobKey: e.r.jobKey, TaskType: call.TaskType, Ordinal: ordinal, AttemptNumber: attempt, Output: out, Err: err, At: rec.At})
}

func (e *executionRun) controlLocked(err error) error {
	if e.fatal == nil {
		e.fatal = &executionControlError{err}
		for c := range e.active {
			c.cancel(e.fatal)
		}
	}
	return e.fatal
}
