package toyimpl

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadata"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/segmentio/ksuid"
)

type appendScheduler struct{ *Runtime }
type appendChapters struct{ r *Runtime }

func (r *Runtime) StartOrAppendJob(ctx context.Context, req jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	return (runtimecore.AppendService{Scheduler: appendScheduler{r}, Chapters: appendChapters{r}, Schemas: r, PayloadLimit: r.ChapterApplicationPayloadLimit()}).StartOrAppendJob(ctx, req)
}
func (s appendScheduler) ResolveAppendCell(ctx context.Context, j jobdb.SubmitJob) (jobdb.ResolvedCell, error) {
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	return s.engine.prepareCellLocked(ctx, goalstate.Preparation{Key: jobdb.JobKey{TenantId: j.TenantId}, Cell: j.Cell, CellRepo: j.CellRepo})
}
func appendStored(key jobdb.JobKey, r *jobRecord) runtimecore.StoredJob {
	out := runtimecore.StoredJob{JobKey: key, Cell: r.cell, GoalID: r.goalID, JobType: r.jobType, AppendKey: r.appendKey, AppendGeneration: r.appendGeneration, ArchivedAt: r.archived, CancelRequested: r.cancelled, LeaseWorkerID: r.leaseWorkerID, SchemaHash: jobmetadata.SchemaHashFromStoredMetadata(r.metadata), Status: r.currentStatus(time.Now())}
	if r.leaseID != "" {
		t := r.leaseExpiresAt
		out.LeaseExpiresAt = &t
	}
	return out
}
func (s appendScheduler) GetAppendTail(ctx context.Context, chain runtimecore.AppendChain) (runtimecore.StoredJob, bool, error) {
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	var out runtimecore.StoredJob
	found := false
	for key, r := range s.engine.jobRecords {
		r.mu.Lock()
		if key.TenantId == chain.TenantID && r.cell.CellID == chain.CellID && r.jobType == chain.JobType && r.appendKey == chain.AppendKey && (!found || r.appendGeneration > out.AppendGeneration) {
			out = appendStored(key, r)
			found = true
		}
		r.mu.Unlock()
	}
	return out, found, nil
}
func (s appendScheduler) CreateJob(ctx context.Context, q runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	metadata, err := jobdb.BuildJobMetadataEnvelope(q.AppMetadata, jobdb.RuntimeJobMetadata{SchemaHash: q.SchemaHash})
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	payload, err := json.Marshal(workerJobPayload{RunPolicy: q.RunPolicy})
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	if r := s.engine.jobRecords[q.JobKey]; r != nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		return appendStored(q.JobKey, r), nil
	}
	at := q.CreatedAt
	if q.AvailableAt != nil {
		at = *q.AvailableAt
	}
	r := &jobRecord{appendKey: q.AppendKey, appendGeneration: q.AppendGeneration, clientPayload: q.ClientPayload, clientPayloadRevision: q.ClientPayloadRevision, initialPayloadDigest: q.InitialPayloadDigest, status: jobdb.JobStatusReady, jobType: q.JobType, createdAt: q.CreatedAt, metadata: metadata, payload: payload, route: jobdb.Route{JobType: q.JobType}, chapters: map[int64]*toyChapter{}, availableAt: at, waitFor: append([]string(nil), q.WaitForJobIDs...)}
	if len(r.waitFor) > 0 {
		r.status = jobdb.JobStatusPendingJobs
	} else if at.After(time.Now()) {
		r.status = jobdb.JobStatusAwaitingFuture
	}
	s.engine.jobRecords[q.JobKey] = r
	if err = s.engine.publishGoalLocked(ctx, q.JobKey, r); err != nil {
		delete(s.engine.jobRecords, q.JobKey)
		return runtimecore.StoredJob{}, err
	}
	return appendStored(q.JobKey, r), nil
}
func (s appendScheduler) ClaimAppend(ctx context.Context, q runtimecore.ClaimAppendRequest) (runtimecore.LeaseIdentity, error) {
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	if _, err := s.engine.cellParentLocked(ctx, q.JobKey.TenantId); err != nil {
		return runtimecore.LeaseIdentity{}, err
	}
	r := s.engine.jobRecords[q.JobKey]
	if r == nil {
		return runtimecore.LeaseIdentity{}, jobdb.ErrJobNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.archived != nil || r.cancelled || r.leaseExpiresAt.After(q.Now) || r.currentStatus(q.Now) == jobdb.JobStatusCrashConcern {
		return runtimecore.LeaseIdentity{}, jobdb.ErrConflict
	}
	old := r.leaseID
	if old != "" && !runtimecore.IsAppendWorker(r.leaseWorkerID) {
		r.consecutiveExpirations++
	}
	r.leased = true
	r.leaseID = ksuid.New().String()
	r.leaseWorkerID = q.WorkerID
	r.leaseExpiresAt = q.Now.Add(q.Duration)
	s.engine.appendLeaseEventLocked(r, q.Now, jobdb.LeaseAcquisitionAppend, old, nil)
	return runtimecore.LeaseIdentity{JobKey: q.JobKey, LeaseID: r.leaseID, WorkerID: r.leaseWorkerID, ExpiresAt: r.leaseExpiresAt}, nil
}
func (s appendScheduler) ReleaseAppend(ctx context.Context, q runtimecore.LeaseIdentity) error {
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	r := s.engine.jobRecords[q.JobKey]
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.leaseID == q.LeaseID {
		r.leased = false
		r.leaseID = ""
		r.leaseWorkerID = ""
		r.leaseExpiresAt = time.Time{}
	}
	return nil
}
func (c appendChapters) Get(ctx context.Context, key runtimecore.ChapterLogKey, n int64) (runtimecore.EncodedChapter, error) {
	c.r.engine.mu.Lock()
	defer c.r.engine.mu.Unlock()
	ch, ok := c.r.engine.runtimeChapters[key.JobKey][n]
	if !ok {
		return runtimecore.EncodedChapter{}, jobdb.ErrChapterNotFound
	}
	return runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: runtimecodec.CloneChapter(ch)})
}
func (c appendChapters) Count(ctx context.Context, key runtimecore.ChapterLogKey) (int64, error) {
	c.r.engine.mu.Lock()
	defer c.r.engine.mu.Unlock()
	return int64(len(c.r.engine.runtimeChapters[key.JobKey])), nil
}
func (c appendChapters) Create(ctx context.Context, key runtimecore.ChapterLogKey, ch runtimecore.EncodedChapter) error {
	return c.write(ctx, key, ch, true)
}
func (c appendChapters) Append(ctx context.Context, key runtimecore.ChapterLogKey, ch runtimecore.EncodedChapter) error {
	return c.write(ctx, key, ch, false)
}
func (c appendChapters) write(ctx context.Context, key runtimecore.ChapterLogKey, encoded runtimecore.EncodedChapter, create bool) error {
	ch, err := runtimecore.DecodeChapter(encoded)
	if err != nil {
		return err
	}
	blobs := map[string][]byte{}
	for _, up := range encoded.ArtifactUploads {
		reader, err := up.Open()
		if err != nil {
			return err
		}
		b, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			return err
		}
		blobs[up.Name] = b
	}
	c.r.engine.mu.Lock()
	defer c.r.engine.mu.Unlock()
	chapters := c.r.engine.runtimeChapters[key.JobKey]
	if create {
		if len(chapters) > 0 {
			return jobdb.ErrConflict
		}
		chapters = map[int64]jobdb.Chapter{}
		c.r.engine.runtimeChapters[key.JobKey] = chapters
	}
	if chapters == nil || int64(len(chapters)) != ch.Ordinal {
		return jobdb.ErrConflict
	}
	chapters[ch.Ordinal] = runtimecodec.CloneChapter(ch)
	for name, b := range blobs {
		c.r.engine.runtimeArtifacts[runtimeArtifactKey{key.JobKey, ch.Ordinal, name}] = b
	}
	return nil
}
func (l *runtimeLease) StartOrAppendJob(ctx context.Context, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	return l.runtime.StartOrAppendJobWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, q)
}
func (r *Runtime) StartOrAppendJobWithLeaseByID(ctx context.Context, key jobdb.JobKey, lease, worker string, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	if _, err := r.ValidateExecutionLeaseByID(ctx, key, lease, worker); err != nil {
		return jobdb.JobHandle{}, err
	}
	q.Job.Job.TenantId = key.TenantId
	return r.StartOrAppendJob(cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: key, LeaseID: lease, WorkerID: worker}), q)
}
