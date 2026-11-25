package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chaptercore "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadata"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/segmentio/ksuid"
)

type appendScheduler struct{ *Runtime }
type appendChapters struct{ r *Runtime }

func (r *Runtime) StartOrAppendJob(ctx context.Context, req jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	return (runtimecore.AppendService{Projects: r.projects, Scheduler: appendScheduler{r}, Chapters: appendChapters{r}, Schemas: r, PayloadLimit: r.ChapterApplicationPayloadLimit(), Now: r.now}).StartOrAppendJob(ctx, req)
}
func (s appendScheduler) ResolveAppendCell(ctx context.Context, j jobdb.SubmitJob) (jobdb.ResolvedCell, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return jobdb.ResolvedCell{}, err
	}
	defer tx.Rollback()
	return s.prepareCell(ctx, tx, &goalstate.Preparation{Key: jobdb.JobKey{TenantId: j.TenantId}, Cell: j.Cell, CellRepo: j.CellRepo})
}
func appendStored(r jobRow) runtimecore.StoredJob {
	return runtimecore.StoredJob{JobKey: jobdb.JobKey{TenantId: r.tenantID, JobId: r.jobID}, Cell: r.cell, GoalID: r.goalID, JobType: r.jobType, AppendKey: r.appendKey.String, AppendGeneration: r.appendGeneration.Int64, ArchivedAt: nullTimeFromNS(r.archivedAtNS), CancelRequested: r.cancelRequested, LeaseWorkerID: r.leaseWorkerID.String, LeaseExpiresAt: nullTimeFromNS(r.leaseExpiresAtNS), LastLeaseUpdatedAt: leaseUpdateTime(r.lastLeaseUpdatedAtNS), SchemaHash: jobmetadata.SchemaHashFromStoredMetadata(r.metadata)}
}
func (s appendScheduler) GetAppendTail(ctx context.Context, c runtimecore.AppendChain) (runtimecore.StoredJob, bool, error) {
	row, err := scanJobRow(s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobdb_jobs WHERE tenant_id=? AND cell_id=? AND job_type=? AND append_key=? ORDER BY append_gen DESC LIMIT 1`, c.TenantID, c.CellID, c.JobType, c.AppendKey))
	if errors.Is(err, sql.ErrNoRows) {
		return runtimecore.StoredJob{}, false, nil
	}
	if err != nil {
		return runtimecore.StoredJob{}, false, err
	}
	out := appendStored(row)
	if row.crashBudgetRemaining == 0 && row.archivedAtNS.Valid == false && (!row.leaseExpiresAtNS.Valid || !timeFromNS(row.leaseExpiresAtNS.Int64).After(s.now())) {
		out.Status = jobdb.JobStatusCrashConcern
	}
	return out, true, nil
}
func (s appendScheduler) CreateJob(ctx context.Context, q runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	metadata, err := jobdb.BuildJobMetadataEnvelope(q.AppMetadata, jobdb.RuntimeJobMetadata{SchemaHash: q.SchemaHash})
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	var update *jobdb.ClientPayloadUpdate
	if q.ClientPayload != nil {
		update = &jobdb.ClientPayloadUpdate{Mode: "reset", Value: q.ClientPayload}
	}
	ctx = context.WithValue(ctx, appendFactsKey{}, q)
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		if _, e := s.loadJobRowTx(ctx, tx, q.JobKey); e == nil {
			return nil
		} else if !errors.Is(e, jobdb.ErrJobNotFound) {
			return e
		}
		return s.insertJobRecordTx(ctx, tx, q.JobKey, q.JobType, metadata, q.WaitForJobIDs, jobPayload{RunPolicy: q.RunPolicy}, q.WorkerID, q.AvailableAt, update)
	})
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	row, err := s.loadJobRow(ctx, q.JobKey)
	return appendStored(row), err
}

type appendFactsKey struct{}

func (s appendScheduler) ClaimAppend(ctx context.Context, q runtimecore.ClaimAppendRequest) (out runtimecore.LeaseIdentity, err error) {
	out = runtimecore.LeaseIdentity{JobKey: q.JobKey, LeaseID: ksuid.New().String(), WorkerID: q.WorkerID, ExpiresAt: q.Now.Add(q.Duration)}
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		row, e := s.loadJobRowTx(ctx, tx, q.JobKey)
		if e != nil {
			return e
		}
		// Validate the caller's authority again after waiting for the writer lock.
		_, e = s.prepareCell(ctx, tx, &goalstate.Preparation{Key: q.JobKey, Cell: row.cell.Cell})
		if e != nil {
			return e
		}
		result, e := tx.ExecContext(ctx, `UPDATE jobdb_jobs SET crash_budget_remaining=CASE WHEN lease_id IS NOT NULL AND substr(lease_worker_id,1,13) <> 'jobdb-append-' THEN crash_budget_remaining-1 ELSE crash_budget_remaining END,lease_id=?,lease_worker_id=?,lease_expires_at_ns=?,last_lease_updated_at_ns=? WHERE tenant_id=? AND job_id=? AND archived_at_ns IS NULL AND cancel_requested=0 AND crash_budget_remaining>0 AND (resource_not_before_ns IS NULL OR resource_not_before_ns<=?) AND (lease_id IS NULL OR lease_expires_at_ns<=?)`, out.LeaseID, out.WorkerID, timeToNS(out.ExpiresAt), q.Now.UnixNano(), q.JobKey.TenantId, q.JobKey.JobId, timeToNS(q.Now), timeToNS(q.Now))
		if e != nil {
			return e
		}
		n, e := result.RowsAffected()
		if e != nil {
			return e
		}
		if n != 1 {
			return jobdb.ErrConflict
		}
		return s.insertLeaseEvent(ctx, tx, row, q.Now, jobdb.LeaseAcquisitionAppend, nil)
	})
	return
}
func (s appendScheduler) ReleaseAppend(ctx context.Context, q runtimecore.LeaseIdentity) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobdb_jobs SET lease_id=NULL,lease_worker_id=NULL,lease_expires_at_ns=NULL WHERE tenant_id=? AND job_id=? AND lease_id=?`, q.JobKey.TenantId, q.JobKey.JobId, q.LeaseID)
	return err
}
func (c appendChapters) Get(ctx context.Context, key runtimecore.ChapterLogKey, n int64) (runtimecore.EncodedChapter, error) {
	ch, err := c.r.chapterStore.Chapter(chapterContext(ctx), storyKeyForJob(key.JobKey), n)
	if errors.Is(err, storage.ErrChapterNotFound) || errors.Is(err, storage.ErrStoryNotFound) {
		return runtimecore.EncodedChapter{}, jobdb.ErrChapterNotFound
	}
	if err != nil {
		return runtimecore.EncodedChapter{}, err
	}
	typed, err := chapterFromStoryChapter(ch)
	err = jobdb.ClassifyExecutionInfrastructure(err, jobdb.ExecutionInfrastructureDurableDecode, "decode_append_chapter", &key.JobKey, &n)
	if err != nil {
		return runtimecore.EncodedChapter{}, err
	}
	return runtimecore.EncodedChapter{Ordinal: n, Payload: ch.Body(), Artifacts: typed.Artifacts}, nil
}
func (c appendChapters) Count(ctx context.Context, key runtimecore.ChapterLogKey) (int64, error) {
	st, err := c.r.chapterStore.Story(chapterContext(ctx), storyKeyForJob(key.JobKey))
	if err != nil {
		return 0, err
	}
	return st.ChapterCount(), nil
}
func (c appendChapters) Create(ctx context.Context, key runtimecore.ChapterLogKey, ch runtimecore.EncodedChapter) error {
	return c.write(ctx, key, ch, true)
}
func (c appendChapters) Append(ctx context.Context, key runtimecore.ChapterLogKey, ch runtimecore.EncodedChapter) error {
	return c.write(ctx, key, ch, false)
}
func (c appendChapters) write(ctx context.Context, key runtimecore.ChapterLogKey, encoded runtimecore.EncodedChapter, create bool) error {
	typed, err := runtimecore.DecodeChapter(encoded)
	if err != nil {
		return err
	}
	_, artifacts, err := c.r.prepareChapterWrite(ctx, jobdb.PutChapterRequest{Chapter: typed, ArtifactUploads: encoded.ArtifactUploads})
	if err != nil {
		return err
	}
	var ch story.Chapter = story.NewChapter().WithOrdinal(encoded.Ordinal).WithBytes(encoded.Payload)
	for _, a := range artifacts {
		ch = ch.AddArtifact(a)
	}
	if create {
		_, err = c.r.chapterStore.CreateStory(chapterContext(ctx), storyKeyForJob(key.JobKey), story.CreateOptions{InitialChapter: ch})
	} else {
		err = c.r.chapterStore.SaveChapter(chapterContext(ctx), storyKeyForJob(key.JobKey), ch)
	}
	if errors.Is(err, chaptercore.ErrConflict) {
		return jobdb.ErrConflict
	}
	return err
}
func (l *executionLease) StartOrAppendJob(ctx context.Context, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	return l.runtime.StartOrAppendJobWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, q)
}
func (r *Runtime) StartOrAppendJobWithLeaseByID(ctx context.Context, key jobdb.JobKey, lease, worker string, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	if _, err := r.ValidateExecutionLeaseByID(ctx, key, lease, worker); err != nil {
		return jobdb.JobHandle{}, submissionStorageError(err, "validate_parent_lease", key, nil)
	}
	q.Job.Job.TenantId = key.TenantId
	return r.StartOrAppendJob(cellstate.WithLease(ctx, cellstate.LeaseProof{JobKey: key, LeaseID: lease, WorkerID: worker}), q)
}
