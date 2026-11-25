package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
)

func (l *executionLease) RescheduleResourceBackoff(ctx context.Context, req jobdb.ResourceBackoffRequest) (jobdb.ResourceBackoffReceipt, error) {
	return l.runtime.RescheduleResourceBackoffByID(ctx, l.jobKey, l.leaseID, l.workerID, req)
}

func (r *Runtime) RescheduleResourceBackoffByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, req jobdb.ResourceBackoffRequest) (receipt jobdb.ResourceBackoffReceipt, err error) {
	return r.resourceBackoff(ctx, key, leaseID, workerID, req, r.now, r.sampleResource, r.resourceFault)
}

// Clock, RNG and fault boundaries run under the actual transaction. Tests use
// these private seams without replacing the storage operation.
func (r *Runtime) resourceBackoff(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, req jobdb.ResourceBackoffRequest, now func() time.Time, sample func() float64, fault func(string) error) (receipt jobdb.ResourceBackoffReceipt, err error) {
	req, err = jobdb.CloneResourceBackoffRequest(req)
	if err != nil {
		return receipt, err
	}
	body, err := jobdb.CanonicalResourceBackoffRequest(req)
	if err != nil {
		return receipt, err
	}
	if err = r.validate(); err != nil {
		return receipt, err
	}
	if leaseID == "" || workerID == "" {
		return receipt, jobdb.ErrExecutionLeaseLost
	}
	if err = jobdb.RequireProjectWrite(ctx, r.projects, key.TenantId); err != nil {
		return receipt, err
	}
	check := func(stage string) error {
		if fault != nil {
			return fault(stage)
		}
		return nil
	}
	ch, chErr := r.GetChapter(ctx, jobdb.ChapterRef{JobKey: key, Ordinal: req.Resume.CheckpointOrdinal})
	var inputChapter jobdb.Chapter
	var inputErr error
	if req.Resume.TaskWait != nil {
		inputChapter, inputErr = r.GetChapter(ctx, jobdb.ChapterRef{JobKey: key, Ordinal: req.Resume.TaskWait.InputOrdinal})
	}
	err = r.withTx(ctx, func(tx *sql.Tx) error {
		var version int
		if e := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); e != nil {
			return e
		}
		if version != currentFormat {
			return &jobdb.ResourceBackoffUnsupportedError{Feature: fmt.Sprintf("SQLite format %d", currentFormat)}
		}
		var savedBody, savedReceipt []byte
		var savedWorker string
		e := tx.QueryRowContext(ctx, `SELECT worker_id,request,receipt FROM jobdb_resource_receipts WHERE tenant_id=? AND job_id=? AND lease_id=? AND request_id=?`, key.TenantId, key.JobId, leaseID, req.RequestID).Scan(&savedWorker, &savedBody, &savedReceipt)
		if e == nil {
			if savedWorker != workerID || !bytes.Equal(savedBody, body) {
				return jobdb.ErrConflict
			}
			return json.Unmarshal(savedReceipt, &receipt)
		}
		if e != sql.ErrNoRows {
			return e
		}
		row, e := r.loadJobRowTx(ctx, tx, key)
		if e != nil {
			return e
		}
		at := now()
		if e = validateLeaseRow(row, leaseID, workerID, at); e != nil {
			return e
		}
		if row.dependencyWaitID != "" || len(row.waitForRaw) > 0 && string(row.waitForRaw) != "[]" {
			return fmt.Errorf("%w: incompatible resource wait", jobdb.ErrConflict)
		}
		if req.Resume.Route != row.nextRoute || req.Resume.PayloadRevision != row.clientPayloadRevision {
			return jobdb.ErrConflict
		}
		currentTask := jobExecutionState(row.payload).TaskWait
		if (currentTask == nil) != (req.Resume.TaskWait == nil) {
			return jobdb.ErrConflict
		}
		if currentTask != nil {
			if req.Resume.TaskWait == nil || currentTask.InputOrdinal != req.Resume.TaskWait.InputOrdinal || currentTask.InputHash != req.Resume.TaskWait.InputHash || currentTask.ResumeJobType != req.Resume.TaskWait.ResumeJobType || req.Resume.TaskWait.OutputOrdinal != req.Resume.CheckpointOrdinal+1 {
				return jobdb.ErrConflict
			}
		}
		if chErr != nil {
			return chErr
		}
		if e = jobdb.ValidateResourceCheckpoint(ch, req); e != nil {
			return e
		}
		if req.Resume.TaskWait != nil {
			if inputErr != nil {
				return inputErr
			}
			if e = jobdb.ValidateResourceTaskCheckpoint(ch, inputChapter, req); e != nil {
				return e
			}
			if b, ok := inputChapter.Body.(jobdb.ExecutionChapter); ok {
				declared, _ := jobdb.DecodeExecutionRecord(b.Record)
				hash, hashErr := runtimecodec.ExecutionIdentity(declared)
				if hashErr != nil || hash != req.Resume.TaskWait.InputHash {
					return jobdb.ErrConflict
				}
			}
		}
		if e = check("validated"); e != nil {
			return e
		}
		policy, _ := json.Marshal(req.Policy)
		count := int64(0)
		allowance := int64(5)
		var oldPolicy []byte
		e = tx.QueryRowContext(ctx, `SELECT count,policy,allowance FROM jobdb_resource_sequences WHERE tenant_id=? AND job_id=? AND invocation=? AND attempt=? AND reason=?`, key.TenantId, key.JobId, req.Invocation, req.Attempt, req.Reason).Scan(&count, &oldPolicy, &allowance)
		if e == nil {
			if !bytes.Equal(oldPolicy, policy) {
				return jobdb.ErrConflict
			}
		} else if e != sql.ErrNoRows {
			return e
		} else if req.Policy.CrashAllowance != nil {
			allowance = *req.Policy.CrashAllowance
		}
		if count < math.MaxInt64 {
			count++
		}
		delay, e := jobdb.ResourceBackoffDelay(req.Policy, count, sample())
		if e != nil {
			return e
		}
		wakeNS := timeToNS(at)
		if wakeNS > math.MaxInt64-int64(delay) {
			wakeNS = math.MaxInt64
		} else {
			wakeNS += int64(delay)
		}
		gateNS := wakeNS
		checkpoint, _ := jobdb.DecodeExecutionRecord(ch.Body.(jobdb.ExecutionChapter).Record)
		var alternateJob, alternateAt any
		// Keep the full resource gate. Only the job route may wake earlier to
		// process the original deadline; task completion and append cannot.
		if !checkpoint.Deadline.IsZero() {
			alternateJob = req.Resume.Route.JobType
			alternateAt = timeToNS(checkpoint.Deadline)
		}
		nextPayload, revision, e := clientpayload.Apply(row.clientPayload, row.clientPayloadRevision, req.ClientPayloadUpdate)
		if e != nil {
			return e
		}
		resume := req.Resume
		resume.PayloadRevision = revision
		resumeBytes, _ := json.Marshal(resume)
		payload, e := decodeJobPayload(row.payload)
		if e != nil {
			return e
		}
		if req.Resume.TaskWait != nil {
			w := req.Resume.TaskWait
			payload.TaskWait = &taskWait{InputStep: w.InputOrdinal, OutputStep: w.OutputOrdinal, InputHash: w.InputHash, Next: w.ResumeJobType}
		}
		payloadBytes, e := encodeJobPayload(payload)
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO jobdb_resource_sequences VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(tenant_id,job_id,invocation,attempt,reason) DO UPDATE SET count=excluded.count`, key.TenantId, key.JobId, req.Invocation, req.Attempt, req.Reason, count, policy, allowance)
		if e != nil {
			return e
		}
		if e = check("sequence"); e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `UPDATE jobdb_jobs SET crash_budget_remaining=?,resource_not_before_ns=?,resource_resume=?,payload=?,client_payload=?,client_payload_revision=?,lease_id=NULL,lease_worker_id=NULL,lease_expires_at_ns=NULL,alternate_job_type=?,alternate_task_type=NULL,alternate_at_ns=?,updated_at_ns=? WHERE tenant_id=? AND job_id=?`, allowance, gateNS, resumeBytes, payloadBytes, nullableJSON(nextPayload), revision, alternateJob, alternateAt, timeToNS(at), key.TenantId, key.JobId)
		if e != nil {
			return e
		}
		if e = check("job"); e != nil {
			return e
		}
		receipt = jobdb.ResourceBackoffReceipt{Version: 1, RequestID: req.RequestID, JobKey: key, LeaseID: leaseID, WorkerID: workerID, Count: count, WakeAt: timeFromNS(wakeNS), Reason: req.Reason, Policy: req.Policy, ResolvedCrashAllowance: allowance, Resume: resume}
		savedReceipt, e = json.Marshal(receipt)
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO jobdb_resource_receipts VALUES(?,?,?,?,?,?,?)`, key.TenantId, key.JobId, leaseID, workerID, req.RequestID, body, savedReceipt)
		if e != nil {
			return e
		}
		return check("receipt")
	})
	if err != nil {
		return jobdb.ResourceBackoffReceipt{}, err
	}
	return receipt, nil
}

func (r *Runtime) ReplayResourceBackoffReceiptByID(ctx context.Context, key jobdb.JobKey, lease, worker string, req jobdb.ResourceBackoffRequest) (result jobdb.ResourceBackoffReceipt, err error) {
	body, err := jobdb.CanonicalResourceBackoffRequest(req)
	if err != nil {
		return result, jobdb.ErrExecutionLeaseLost
	}
	var saved, receipt []byte
	var savedWorker string
	err = r.db.QueryRowContext(ctx, `SELECT worker_id,request,receipt FROM jobdb_resource_receipts WHERE tenant_id=? AND job_id=? AND lease_id=? AND request_id=?`, key.TenantId, key.JobId, lease, req.RequestID).Scan(&savedWorker, &saved, &receipt)
	if err != nil || savedWorker != worker || !bytes.Equal(saved, body) {
		return result, jobdb.ErrExecutionLeaseLost
	}
	err = json.Unmarshal(receipt, &result)
	return result, err
}

func (r *Runtime) now() time.Time {
	if r.clock != nil {
		return r.clock().UTC()
	}
	return time.Now().UTC()
}
func (r *Runtime) sampleResource() float64 {
	if r.resourceSample != nil {
		return r.resourceSample()
	}
	return rand.Float64()
}
