package toyimpl

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/rand/v2"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
)

type resourceSequence struct {
	Count     int64
	Policy    []byte
	Allowance int64
}
type resourceReceipt struct {
	Worker        string
	Body, Receipt []byte
}

func recordExecutionState(r *jobRecord) jobdb.ExecutionState {
	s := toyExecutionState(r.payload)
	b := r.crashBudgetRemaining
	s.CrashBudgetRemaining = &b
	s.ResourceBackoff = &jobdb.ResourceBackoffCapability{ProtocolVersion: 1}
	s.ResourceResume = r.resourceResume
	return jobdb.CloneExecutionState(s)
}
func (l *runtimeLease) RescheduleResourceBackoff(ctx context.Context, req jobdb.ResourceBackoffRequest) (jobdb.ResourceBackoffReceipt, error) {
	return l.runtime.RescheduleResourceBackoffByID(ctx, l.jobKey, l.leaseID, l.workerID, req)
}
func (r *Runtime) RescheduleResourceBackoffByID(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, req jobdb.ResourceBackoffRequest) (receipt jobdb.ResourceBackoffReceipt, err error) {
	return r.resourceBackoff(ctx, key, leaseID, workerID, req, r.now, r.sampleResource)
}
func (r *Runtime) resourceBackoff(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, req jobdb.ResourceBackoffRequest, now func() time.Time, sample func() float64) (receipt jobdb.ResourceBackoffReceipt, err error) {
	req, err = jobdb.CloneResourceBackoffRequest(req)
	if err != nil {
		return receipt, err
	}
	body, err := jobdb.CanonicalResourceBackoffRequest(req)
	if err != nil {
		return receipt, err
	}
	if err = ctx.Err(); err != nil {
		return receipt, err
	}
	if err = jobdb.RequireProjectWrite(ctx, r.projects, key.TenantId); err != nil {
		return receipt, err
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	rec := r.engine.jobRecords[key]
	if rec == nil {
		return receipt, jobdb.ErrJobNotFound
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	receiptKey, _ := json.Marshal([]string{leaseID, req.RequestID})
	if old, ok := rec.resourceReceipts[string(receiptKey)]; ok {
		if old.Worker != workerID || !bytes.Equal(old.Body, body) {
			return receipt, jobdb.ErrConflict
		}
		err = json.Unmarshal(old.Receipt, &receipt)
		return receipt, err
	}
	at := now()
	if leaseID == "" || workerID == "" || !rec.leased || rec.leaseID != leaseID || rec.leaseWorkerID != workerID || !rec.leaseExpiresAt.After(at) || rec.cancelled || rec.archived != nil || rec.hidden {
		return receipt, jobdb.ErrExecutionLeaseLost
	}
	if rec.dependencyWaitID != "" || len(rec.waitFor) > 0 || req.Resume.Route != rec.route || req.Resume.PayloadRevision != rec.clientPayloadRevision {
		return receipt, jobdb.ErrConflict
	}
	chapters := r.engine.runtimeChapters[key]
	ch, ok := chapters[req.Resume.CheckpointOrdinal]
	if !ok {
		return receipt, jobdb.ErrChapterNotFound
	}
	if err = jobdb.ValidateResourceCheckpoint(ch, req); err != nil {
		return receipt, err
	}
	oldTask := toyExecutionState(rec.payload).TaskWait
	if (oldTask == nil) != (req.Resume.TaskWait == nil) {
		return receipt, jobdb.ErrConflict
	}
	if oldTask != nil {
		w := req.Resume.TaskWait
		if w == nil || w.InputOrdinal != oldTask.InputOrdinal || w.InputHash != oldTask.InputHash || w.ResumeJobType != oldTask.ResumeJobType || w.OutputOrdinal != req.Resume.CheckpointOrdinal+1 {
			return receipt, jobdb.ErrConflict
		}
	}
	if w := req.Resume.TaskWait; w != nil {
		input, ok := chapters[w.InputOrdinal]
		if !ok {
			return receipt, jobdb.ErrChapterNotFound
		}
		if err = jobdb.ValidateResourceTaskCheckpoint(ch, input, req); err != nil {
			return receipt, err
		}
		if b, ok := input.Body.(jobdb.ExecutionChapter); ok {
			declared, _ := jobdb.DecodeExecutionRecord(b.Record)
			hash, hashErr := runtimecodec.ExecutionIdentity(declared)
			if hashErr != nil || hash != w.InputHash {
				return receipt, jobdb.ErrConflict
			}
		}
	}
	seqKey, _ := json.Marshal([]any{req.Invocation, req.Attempt, req.Reason})
	seq := rec.resourceSequences[string(seqKey)]
	policy, _ := json.Marshal(req.Policy)
	if seq.Count > 0 {
		if !bytes.Equal(seq.Policy, policy) {
			return receipt, jobdb.ErrConflict
		}
	} else {
		seq.Policy = policy
		seq.Allowance = 5
		if req.Policy.CrashAllowance != nil {
			seq.Allowance = *req.Policy.CrashAllowance
		}
	}
	if seq.Count < math.MaxInt64 {
		seq.Count++
	}
	delay, err := jobdb.ResourceBackoffDelay(req.Policy, seq.Count, sample())
	if err != nil {
		return receipt, err
	}
	wake := jobdb.ResourceBackoffWake(at, delay)
	client, revision, err := clientpayload.Apply(rec.clientPayload, rec.clientPayloadRevision, req.ClientPayloadUpdate)
	if err != nil {
		return receipt, err
	}
	payload := workerJobPayload{}
	err = json.Unmarshal(rec.payload, &payload)
	if err != nil {
		return receipt, err
	}
	if w := req.Resume.TaskWait; w != nil {
		payload.TaskWait = &workerTaskWait{InputStep: w.InputOrdinal, OutputStep: w.OutputOrdinal, InputHash: w.InputHash, Next: w.ResumeJobType}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return receipt, err
	}
	resume := req.Resume
	resume.PayloadRevision = revision
	receipt = jobdb.ResourceBackoffReceipt{Version: 1, RequestID: req.RequestID, JobKey: key, LeaseID: leaseID, WorkerID: workerID, Count: seq.Count, WakeAt: wake, Reason: req.Reason, Policy: req.Policy, ResolvedCrashAllowance: seq.Allowance, Resume: resume}
	raw, _ := json.Marshal(receipt)
	if rec.resourceSequences == nil {
		rec.resourceSequences = map[string]resourceSequence{}
		rec.resourceReceipts = map[string]resourceReceipt{}
	}
	rec.resourceSequences[string(seqKey)] = seq
	rec.resourceReceipts[string(receiptKey)] = resourceReceipt{workerID, append([]byte(nil), body...), raw}
	rec.clientPayload = client
	rec.clientPayloadRevision = revision
	rec.payload = encoded
	rec.crashBudgetRemaining = seq.Allowance
	rec.resourceNotBefore = wake
	checkpoint, _ := jobdb.DecodeExecutionRecord(ch.Body.(jobdb.ExecutionChapter).Record)
	rec.resourceResume = &resume
	rec.leased = false
	rec.leaseID = ""
	rec.leaseWorkerID = ""
	rec.leaseExpiresAt = time.Time{}
	rec.alternateRoute = nil
	// The full resource gate is independent of the earlier job-only timeout wake.
	if !checkpoint.Deadline.IsZero() {
		rec.alternateRoute = &jobdb.Route{JobType: req.Resume.Route.JobType}
		rec.alternateAt = checkpoint.Deadline
	}
	rec.status = jobdb.JobStatusReady
	receipt = jobdb.ResourceBackoffReceipt{}
	err = json.Unmarshal(raw, &receipt)
	return receipt, err
}

func (r *Runtime) ReplayResourceBackoffReceiptByID(ctx context.Context, key jobdb.JobKey, lease, worker string, req jobdb.ResourceBackoffRequest) (result jobdb.ResourceBackoffReceipt, err error) {
	body, err := jobdb.CanonicalResourceBackoffRequest(req)
	if err != nil {
		return result, jobdb.ErrExecutionLeaseLost
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	rec := r.engine.jobRecords[key]
	if rec == nil {
		return result, jobdb.ErrExecutionLeaseLost
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	id, _ := json.Marshal([]string{lease, req.RequestID})
	old, ok := rec.resourceReceipts[string(id)]
	if !ok || old.Worker != worker || !bytes.Equal(old.Body, body) {
		return result, jobdb.ErrExecutionLeaseLost
	}
	err = json.Unmarshal(old.Receipt, &result)
	return result, err
}

func (r *Runtime) now() time.Time {
	if r.engine.clock != nil {
		return r.engine.clock().UTC()
	}
	return time.Now().UTC()
}
func (r *Runtime) sampleResource() float64 {
	if r.engine.resourceSample != nil {
		return r.engine.resourceSample()
	}
	return rand.Float64()
}
