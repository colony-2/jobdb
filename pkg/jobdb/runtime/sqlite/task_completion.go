package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	"github.com/segmentio/ksuid"
)

// claimWaitingTask installs the recovery route before publishing any output.
// Failure after this commits leaves a job lease whose expiry enables replay.
func (r *Runtime) claimWaitingTask(ctx context.Context, expected jobRow, resumeJobType string, policy jobdb.RunPolicy) (*executionLease, error) {
	key := jobdb.JobKey{TenantId: expected.tenantID, JobId: expected.jobID}
	payload, err := encodeJobPayload(jobPayload{RunPolicy: policy})
	if err != nil {
		return nil, err
	}
	lease := &executionLease{runtime: r, jobKey: key, leaseID: ksuid.New().String(),
		workerID: r.workerID, route: jobdb.Route{JobType: resumeJobType}, payload: payload,
		duration: defaultRemoteLeaseDuration}
	err = r.withTx(ctx, func(tx *sql.Tx) error {
		current, err := r.loadJobRowTx(ctx, tx, key)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if current.archivedAtNS.Valid || current.cancelRequested ||
			(current.leaseExpiresAtNS.Valid && current.leaseExpiresAtNS.Int64 > timeToNS(now)) ||
			current.nextRoute != expected.nextRoute || !bytes.Equal(current.payload, expected.payload) {
			return fmt.Errorf("%w: waiting task changed", jobdb.ErrConflict)
		}
		// Claim only eligible work. Existing availability and dependencies can stay
		// unchanged: they have already been satisfied when the lease is installed.
		waitFor, err := decodeWaitFor(current.waitForRaw)
		if err != nil {
			return err
		}
		ready, err := dependenciesReady(ctx, tx, key.TenantId, waitFor)
		if err != nil {
			return err
		}
		if !ready || current.availableAtNS > timeToNS(now) {
			return fmt.Errorf("%w: waiting task is not available", jobdb.ErrConflict)
		}
		lease.expiresAt = now.Add(lease.duration)
		// Drop task coordinates to match the job-only route. Alternate routing must
		// not redirect recovery to another task after this lease expires.
		_, err = tx.ExecContext(ctx, `UPDATE jobdb_jobs SET route_job_type=?,route_task_type='',payload=?,lease_id=?,lease_worker_id=?,lease_expires_at_ns=?,alternate_job_type=NULL,alternate_task_type=NULL,alternate_at_ns=NULL,updated_at_ns=? WHERE tenant_id=? AND job_id=?`,
			resumeJobType, payload, lease.leaseID, lease.workerID, timeToNS(lease.expiresAt), timeToNS(now), key.TenantId, key.JobId)
		return err
	})
	if err != nil {
		return nil, err
	}
	return lease, nil
}

func (r *Runtime) appendTaskOutput(ctx context.Context, lease *executionLease, chapter story.Chapter) error {
	row, err := r.loadJobRow(ctx, lease.jobKey)
	if err != nil {
		return err
	}
	if row.cancelRequested {
		return jobdb.ErrExecutionLeaseLost
	}
	if err := validateLeaseRow(row, lease.leaseID, lease.workerID, time.Now().UTC()); err != nil {
		return err
	}
	// The chapter store commits independently. A lease can expire after this
	// check; the atomic ordinal constraint decides competing appends, and the
	// job-only recovery route lets a new worker replay the committed history.
	if err := r.chapterStore.SaveChapter(chapterContext(ctx), storyKeyForJob(lease.jobKey), chapter); err != nil {
		if errors.Is(err, core.ErrConflict) {
			return fmt.Errorf("%w: output chapter %d already exists or is not appendable", jobdb.ErrConflict, chapter.Ordinal())
		}
		return err
	}
	return nil
}
