package workflow

import (
	"context"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"io"
	"sync"
)

// The latch never holds its lock over callbacks or runtime operations. Operations
// admitted before observation may commit; later admissions see the first cause.
type infrastructureGuard struct {
	mu     sync.Mutex
	err    error
	cancel context.CancelCauseFunc
}

func (g *infrastructureGuard) failure() error { g.mu.Lock(); defer g.mu.Unlock(); return g.err }
func (g *infrastructureGuard) observe(err error) error {
	if !jobdb.IsExecutionInfrastructureError(err) {
		return err
	}
	g.mu.Lock()
	if g.err == nil {
		g.err = err
	}
	cause, cancel := g.err, g.cancel
	g.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
	return err
}
func (g *infrastructureGuard) bind(cancel context.CancelCauseFunc) {
	g.mu.Lock()
	g.cancel = cancel
	err := g.err
	g.mu.Unlock()
	if err != nil {
		cancel(err)
	}
}

func (g *infrastructureGuard) io(err error, op string, key JobKey, ordinal *int64) error {
	return g.observe(jobdb.ClassifyRuntimeIO(err, op, &key, ordinal))
}

type guardedRuntime struct {
	WorkflowRuntime
	guard *infrastructureGuard
}

func (r *guardedRuntime) GetChapter(ctx context.Context, ref ChapterRef) (Chapter, error) {
	if err := r.guard.failure(); err != nil {
		return Chapter{}, err
	}
	ch, err := r.WorkflowRuntime.GetChapter(ctx, ref)
	return ch, r.guard.io(err, "get_chapter", ref.JobKey, &ref.Ordinal)
}
func (r *guardedRuntime) ListChapters(ctx context.Context, req ListChaptersRequest) ([]Chapter, error) {
	if err := r.guard.failure(); err != nil {
		return nil, err
	}
	ch, err := r.WorkflowRuntime.ListChapters(ctx, req)
	return ch, r.guard.io(err, "list_chapters", req.JobKey, nil)
}
func (r *guardedRuntime) PutChapter(ctx context.Context, req PutChapterRequest) error {
	if err := r.guard.failure(); err != nil {
		return err
	}
	return r.guard.io(r.WorkflowRuntime.PutChapter(ctx, req), "append_execution", req.Ref.JobKey, &req.Ref.Ordinal)
}
func (r *guardedRuntime) GetJob(ctx context.Context, key JobKey) (JobInfo, error) {
	if err := r.guard.failure(); err != nil {
		return JobInfo{}, err
	}
	info, err := r.WorkflowRuntime.GetJob(ctx, key)
	if info.Data != nil {
		info.Data = &guardedData{TaskData: info.Data, guard: r.guard, key: &key}
	}
	return info, r.guard.io(err, "get_job", key, nil)
}
func (r *guardedRuntime) GetDependencyWait(ctx context.Context, key jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	if err := r.guard.failure(); err != nil {
		return jobdb.DependencyWaitInfo{}, err
	}
	wait, err := r.WorkflowRuntime.GetDependencyWait(ctx, key)
	return wait, r.guard.io(err, "get_dependency_wait", key.JobKey, nil)
}
func (r *guardedRuntime) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	if err := r.guard.failure(); err != nil {
		return nil, err
	}
	reader, err := r.WorkflowRuntime.OpenArtifact(ctx, ref)
	if err != nil {
		return nil, r.guard.io(err, "open_artifact", ref.JobKey, &ref.Ordinal)
	}
	return &guardedArtifactReader{ArtifactReader: reader, guard: r.guard, key: ref.JobKey, ordinal: ref.Ordinal}, nil
}

type guardedArtifactReader struct {
	jobdb.ArtifactReader
	guard   *infrastructureGuard
	key     JobKey
	ordinal int64
}

func (a *guardedArtifactReader) Open() (io.ReadCloser, error) {
	if err := a.guard.failure(); err != nil {
		return nil, err
	}
	stream, err := a.ArtifactReader.Open()
	if err != nil {
		return nil, a.guard.io(err, "open_artifact", a.key, &a.ordinal)
	}
	return &guardedStream{ReadCloser: stream, guard: a.guard, key: a.key, ordinal: a.ordinal}, nil
}

type guardedStream struct {
	io.ReadCloser
	guard   *infrastructureGuard
	key     JobKey
	ordinal int64
}

func (s *guardedStream) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		err = s.guard.io(err, "read_artifact", s.key, &s.ordinal)
	}
	return n, err
}
func (s *guardedStream) Close() error {
	return s.guard.io(s.ReadCloser.Close(), "close_artifact", s.key, &s.ordinal)
}

type guardedData struct {
	TaskData
	key   *JobKey
	guard *infrastructureGuard
}

func (d *guardedData) GetData() (Data, error) {
	raw, err := d.TaskData.GetData()
	return raw, d.guard.observe(jobdb.ClassifyRuntimeIO(err, "read_result", d.key, nil))
}
func (d *guardedData) GetDataOrPanic() Data {
	data, err := d.GetData()
	if err != nil {
		panic(err)
	}
	return data
}
func (d *guardedData) GetArtifacts() ([]Artifact, error) {
	arts, err := d.TaskData.GetArtifacts()
	if err != nil {
		return arts, d.guard.observe(jobdb.ClassifyRuntimeIO(err, "list_result_artifacts", d.key, nil))
	}
	wrapped := make([]Artifact, 0, len(arts))
	for _, a := range arts {
		if a == nil {
			wrapped = append(wrapped, nil)
			continue
		}
		original := a
		key, keyErr := original.ArtifactKey()
		var ordinal *int64
		if keyErr == nil {
			n := key.TaskOrdinal
			ordinal = &n
		}
		observe := func(err error, op string) error {
			return d.guard.observe(jobdb.ClassifyRuntimeIO(err, op, d.key, ordinal))
		}
		guarded := jobdb.NewArtifact(a.Name(), func() (io.ReadCloser, int64, error) {
			if err := d.guard.failure(); err != nil {
				return nil, 0, err
			}
			stream, err := original.Open()
			if err != nil {
				return nil, 0, observe(err, "open_artifact")
			}
			return &observedStream{ReadCloser: stream, observe: observe}, original.Size(), nil
		}, original.Cleanup)
		if keyErr == nil {
			jobdb.AssignArtifactKey(guarded, key)
		}
		wrapped = append(wrapped, guarded)
	}
	return wrapped, nil
}

type observedStream struct {
	io.ReadCloser
	observe func(error, string) error
}

func (s *observedStream) Read(p []byte) (int, error) {
	n, err := s.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		err = s.observe(err, "read_artifact")
	}
	return n, err
}
func (s *observedStream) Close() error { return s.observe(s.ReadCloser.Close(), "close_artifact") }

type guardedLease struct {
	ExecutionLease
	guard *infrastructureGuard
}

func (l *guardedLease) LeaseToken() string { return executionLeaseToken(l.ExecutionLease) }
func (l *guardedLease) LeaseWorkerID() string {
	if v, ok := l.ExecutionLease.(interface{ LeaseWorkerID() string }); ok {
		return v.LeaseWorkerID()
	}
	return ""
}
func (l *guardedLease) Delegation() *jobdb.DelegationInfo {
	if v, ok := l.ExecutionLease.(interface{ Delegation() *jobdb.DelegationInfo }); ok {
		return v.Delegation()
	}
	return nil
}
func (l *guardedLease) Complete(ctx context.Context, req CompleteExecutionRequest) error {
	if err := l.guard.failure(); err != nil {
		return err
	}
	var ordinal *int64
	if req.Chapter != nil {
		value := req.Chapter.Ordinal
		ordinal = &value
	}
	return l.guard.io(l.ExecutionLease.Complete(ctx, req), "complete", l.Job().JobKey, ordinal)
}
func (l *guardedLease) Reschedule(ctx context.Context, req RescheduleExecutionRequest) error {
	if err := l.guard.failure(); err != nil {
		return err
	}
	return l.guard.io(l.ExecutionLease.Reschedule(ctx, req), "reschedule", l.Job().JobKey, nil)
}

// Attempt caches have their own workerRunner, but share this execution lease
// and its guard. Observe classified provider errors at that shared boundary.
func (l *guardedLease) GetConfiguration(ctx context.Context) (ConfigurationSnapshot, error) {
	if err := l.guard.failure(); err != nil {
		return ConfigurationSnapshot{}, err
	}
	value, err := l.ExecutionLease.GetConfiguration(ctx)
	return value, l.guard.observe(err)
}
func (l *guardedLease) ResolveSecrets(ctx context.Context, names []string) (SecretResolveResult, error) {
	if err := l.guard.failure(); err != nil {
		return SecretResolveResult{}, err
	}
	value, err := l.ExecutionLease.ResolveSecrets(ctx, names)
	return value, l.guard.observe(err)
}
func (l *guardedLease) SubmitJob(ctx context.Context, req SubmitJobRequest) (JobHandle, error) {
	if err := l.guard.failure(); err != nil {
		return JobHandle{}, err
	}
	h, err := l.ExecutionLease.SubmitJob(ctx, req)
	return h, l.guard.observe(err)
}
func (l *guardedLease) SubmitRestartJob(ctx context.Context, req SubmitRestartJobRequest) (JobHandle, error) {
	if err := l.guard.failure(); err != nil {
		return JobHandle{}, err
	}
	h, err := l.ExecutionLease.SubmitRestartJob(ctx, req)
	return h, l.guard.observe(err)
}
func (l *guardedLease) StartOrAppendJob(ctx context.Context, req StartOrAppendJobRequest) (JobHandle, error) {
	if err := l.guard.failure(); err != nil {
		return JobHandle{}, err
	}
	h, err := l.ExecutionLease.StartOrAppendJob(ctx, req)
	return h, l.guard.observe(err)
}

func (r *guardedRuntime) CreateDelegation(ctx context.Context, parent jobdb.ExecutionLease, q jobdb.CreateDelegationRequest) (jobdb.RenewableExecutionLease, error) {
	if err := r.guard.failure(); err != nil {
		return nil, err
	}
	if d, ok := r.WorkflowRuntime.(jobdb.LeaseDelegator); ok {
		lease, err := d.CreateDelegation(ctx, parent, q)
		return lease, r.guard.observe(err)
	}
	return nil, jobdb.ErrLeaseDelegationUnsupported
}
func (r *guardedRuntime) ReadExecutionProvenance(ctx context.Context, q jobdb.ReadExecutionProvenanceRequest) (jobdb.ExecutionProvenance, error) {
	return jobdb.ReadExecutionProvenance(ctx, r.WorkflowRuntime, q)
}
