package runtimeadapter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
	"github.com/lib/pq"
)

func (s Scheduler) AcquireWork(ctx context.Context, req runtimecore.WorkRequest) ([]runtimecore.LeaseSnapshot, error) {
	predicates, err := metadataPredicatesToPgjobdb(req.MetadataEquals)
	if err != nil {
		return nil, err
	}
	selector := workSelectorToPgjobdb(req.Selector)
	limit := req.Limit
	if limit <= 0 {
		limit = 1
	}
	leasings := make([]runtimecore.LeaseSnapshot, 0, limit)
	for len(leasings) < limit {
		lease, err := pgjobdb.GetWork(ctx, s.DB, pgjobdb.WorkerID(req.WorkerID), selector,
			pgjobdb.GetWorkOptions{
				TenantIDs:          []pgjobdb.TenantID{pgjobdb.TenantID(req.TenantId)},
				MetadataPredicates: predicates, LeaseDuration: req.LeaseDuration,
			})
		if err != nil {
			return nil, err
		}
		if lease == nil {
			break
		}
		snapshot, err := leaseFromPgjobdb(*lease, req.LeaseDuration)
		if err != nil {
			return nil, err
		}
		leasings = append(leasings, snapshot)
	}
	return leasings, nil
}

func (s Scheduler) AcquireJobLease(ctx context.Context, req runtimecore.JobLeaseRequest) (*runtimecore.LeaseSnapshot, error) {
	lease, err := pgjobdb.GetJobLease(ctx, s.DB,
		pgjobdb.TenantID(req.JobKey.TenantId), pgjobdb.JobID(req.JobKey.JobId),
		pgjobdb.WorkerID(req.WorkerID), workSelectorToPgjobdb(req.Selector),
		pgjobdb.GetJobLeaseOptions{LeaseDuration: req.LeaseDuration})
	if err != nil || lease == nil {
		return nil, err
	}
	snapshot, err := leaseFromPgjobdb(*lease, req.LeaseDuration)
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (s Scheduler) ValidateLease(ctx context.Context, identity runtimecore.LeaseIdentity) (runtimecore.LeaseSnapshot, error) {
	lease, err := pgjobdb.ValidateLease(ctx, s.DB, identityToPgjobdb(identity))
	if err != nil {
		return runtimecore.LeaseSnapshot{}, translateLeaseError(err)
	}
	return leaseFromPgjobdb(*lease, 0)
}

func (s Scheduler) KeepAliveLease(ctx context.Context, mutation runtimecore.LeaseMutation) (runtimecore.LeaseSnapshot, error) {
	lease, err := pgjobdb.KeepAliveLease(ctx, s.DB,
		identityToPgjobdb(mutation.Identity), mutation.Duration)
	if err != nil {
		return runtimecore.LeaseSnapshot{}, translateLeaseError(err)
	}
	return leaseFromPgjobdb(*lease, mutation.Duration)
}

func translateLeaseError(err error) error {
	if errors.Is(err, pgjobdb.ErrLeaseLost) {
		return jobdb.ErrExecutionLeaseLost
	}
	var pgErr *pq.Error
	if errors.As(err, &pgErr) && pgErr.Code == "P0001" &&
		(strings.Contains(pgErr.Message, "lease not found") ||
			strings.Contains(pgErr.Message, "lease not active") ||
			strings.Contains(pgErr.Message, "lease owner mismatch") ||
			strings.Contains(pgErr.Message, "cannot extend the lease")) {
		return fmt.Errorf("%w: %s", jobdb.ErrExecutionLeaseLost, pgErr.Message)
	}
	return err
}

func workSelectorToPgjobdb(selector runtimecore.WorkSelector) pgjobdb.WorkSelector {
	out := pgjobdb.WorkSelector{JobTypes: make([]pgjobdb.JobType, 0, len(selector.JobTypes)),
		Tasks: make([]pgjobdb.TaskSelector, 0, len(selector.Tasks))}
	for _, jobType := range selector.JobTypes {
		out.JobTypes = append(out.JobTypes, pgjobdb.JobType(jobType))
	}
	for _, task := range selector.Tasks {
		out.Tasks = append(out.Tasks, pgjobdb.TaskSelector{
			JobType: pgjobdb.JobType(task.JobType), TaskType: pgjobdb.TaskType(task.TaskType),
		})
	}
	return out
}

func leaseFromPgjobdb(lease pgjobdb.JobLease, requestedDuration time.Duration) (runtimecore.LeaseSnapshot, error) {
	policy, err := runPolicyFromPgjobdb(lease.RunPolicy)
	if err != nil {
		return runtimecore.LeaseSnapshot{}, err
	}
	duration := requestedDuration
	if duration == 0 {
		duration = time.Minute
	}
	snapshot := runtimecore.LeaseSnapshot{
		Identity: runtimecore.LeaseIdentity{
			JobKey:  jobdb.JobKey{TenantId: string(lease.TenantID), JobId: string(lease.JobID)},
			LeaseID: lease.LeaseID, WorkerID: string(lease.WorkerID), ExpiresAt: lease.ExpiresAt,
		},
		JobType: string(lease.JobType), RouteJobType: string(lease.RouteJobType),
		WorkKind: runtimecore.WorkKind(lease.WorkKind), RunPolicy: policy,
		LeasePayload:        append([]byte(nil), lease.LeasePayload...),
		LeasePayloadVisible: lease.LeasePayloadVisible,
		SchemaHash:          lease.SchemaHash, Duration: duration,
	}
	if lease.WorkKind == pgjobdb.WorkKindTask && lease.Task != nil {
		snapshot.TaskWork = &runtimecore.TaskWork{
			TaskType:      string(lease.Task.TaskType),
			ResumeJobType: string(lease.Task.ResumeJobType),
			InputOrdinal:  lease.Task.InputOrdinal, OutputOrdinal: lease.Task.OutputOrdinal,
			InputHash: lease.Task.InputHash,
		}
	}
	return snapshot, nil
}
