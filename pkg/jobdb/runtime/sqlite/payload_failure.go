package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// The compact terminal path has no artifact writes and can atomically commit
// chapter, story position, scheduler completion and dependency/goal state.
func (r *Runtime) completePayloadFailure(ctx context.Context, key jobdb.JobKey, ch jobdb.Chapter, authorize func(*sql.Tx, jobRow) error) error {
	if !jobdb.ChapterPayloadFailure(ch) || len(ch.Artifacts) > 0 {
		return fmt.Errorf("invalid payload failure chapter")
	}
	body, err := encodeChapter(ch)
	if err != nil {
		return err
	}
	outcome := ch.Body.(jobdb.JobAttemptOutcomeChapter).Outcome.(jobdb.AppErrorOutcome)
	return r.withTx(ctx, func(tx *sql.Tx) error {
		row, err := r.loadJobRowTx(ctx, tx, key)
		if err != nil {
			return err
		}
		if err = authorize(tx, row); err != nil {
			return err
		}
		var latest int64
		var deleted, finalized bool
		if err = tx.QueryRowContext(ctx, "SELECT latest_ordinal,deleted,finalized FROM jobdb_chapter_stories WHERE anthology_id=? AND story_id=?", key.TenantId, key.JobId).Scan(&latest, &deleted, &finalized); err != nil {
			return err
		}
		if deleted || finalized || ch.Ordinal != latest+1 {
			return jobdb.ErrConflict
		}
		now := timeToNS(time.Now().UTC())
		if _, err = tx.ExecContext(ctx, "INSERT INTO jobdb_chapter_chapters(anthology_id,story_id,ordinal,body,created_at_ns) VALUES(?,?,?,?,?)", key.TenantId, key.JobId, ch.Ordinal, body, now); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE jobdb_chapter_stories SET latest_ordinal=?,chapter_count=?,updated_at_ns=? WHERE anthology_id=? AND story_id=?", ch.Ordinal, ch.Ordinal+1, now, key.TenantId, key.JobId); err != nil {
			return err
		}
		payload, err := decodeJobPayload(row.payload)
		if err != nil {
			return err
		}
		payload.TaskWait = nil
		raw, err := encodeJobPayload(payload)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE jobdb_jobs SET archived_at_ns=?,completion_status='failed_app',completion_detail=?,payload=?,route_task_type='',lease_id=NULL,lease_worker_id=NULL,lease_expires_at_ns=NULL,alternate_job_type=NULL,alternate_task_type=NULL,alternate_at_ns=NULL,wait_deadline_ns=NULL,updated_at_ns=? WHERE tenant_id=? AND job_id=?`, now, outcome.Error.Message, raw, now, key.TenantId, key.JobId); err != nil {
			return err
		}
		return goalEngine(ctx, tx).Complete(key)
	})
}
func (r *Runtime) failWaitingPayload(ctx context.Context, old jobRow, ch jobdb.Chapter, size *jobdb.ChapterApplicationPayloadError) error {
	key := jobdb.JobKey{TenantId: old.tenantID, JobId: old.jobID}
	err := r.completePayloadFailure(ctx, key, jobdb.PayloadFailureChapter(ch, size), func(tx *sql.Tx, current jobRow) error {
		now := time.Now().UTC()
		if current.archivedAtNS.Valid || current.cancelRequested || (current.leaseExpiresAtNS.Valid && current.leaseExpiresAtNS.Int64 > timeToNS(now)) || current.nextRoute != old.nextRoute || !bytes.Equal(current.payload, old.payload) {
			return jobdb.ErrConflict
		}
		status, err := statusFromRow(ctx, tx, current, now)
		if err != nil {
			return err
		}
		if status != jobdb.JobStatusReady {
			return jobdb.ErrConflict
		}
		return nil
	})
	if err != nil {
		return err
	}
	return size
}
