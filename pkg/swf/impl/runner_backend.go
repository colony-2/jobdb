package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	swfinternal "github.com/colony-2/swf-go/pkg/swf/internal"
)

// defaultRunnerBackend preserves existing behavior for real runs.
type defaultRunnerBackend struct {
	engine     *swfEngineImpl
	lease      swfinternal.Lease
	pgwfLease  *pgwf.Lease
	capability pgwf.Capability
}

func (b *defaultRunnerBackend) GetChapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error) {
	return b.engine.strata.Chapter(ctx, key, ordinal)
}

func (b *defaultRunnerBackend) SaveChapter(ctx context.Context, key story.Key, chap story.Chapter) error {
	return b.engine.strata.SaveChapter(ctx, key, chap)
}

func (b *defaultRunnerBackend) GetJobAttemptOutcome(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error) {
	return b.GetChapter(ctx, key, ordinal)
}

func (b *defaultRunnerBackend) AwaitUntil(ctx context.Context, wakeAt time.Time, info swfinternal.AwaitInfo) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if b.engine == nil || b.pgwfLease == nil {
		return nil
	}
	capability := b.capability
	if capability == "" && b.lease != nil {
		capability = b.lease.NextNeed()
	}
	if capability == "" {
		return nil
	}
	jobID := pgwf.JobID(info.JobKey.JobId)
	ch := b.engine.AwaitUntil(jobID, capability, b.pgwfLease, info.Ordinal, info.Attempt, wakeAt)
	if ch == nil {
		prematureCloseOut()
		return nil
	}

	// Clear any stale signal before waiting.
	select {
	case <-ch:
	default:
	}

	select {
	case sig := <-ch:
		if sig.Kind == awaitSignalKindRecycle {
			prematureCloseOut()
		}
	case <-ctx.Done():
		prematureCloseOut()
		return ctx.Err()
	}
	return nil
}

func (b *defaultRunnerBackend) AwaitJobs(ctx context.Context, jobIds []string, info swfinternal.AwaitInfo) (bool, error) {
	if len(jobIds) == 0 {
		return false, fmt.Errorf("at least one jobId is required")
	}
	if b.engine == nil {
		return false, fmt.Errorf("engine is not available")
	}
	if b.lease == nil {
		return false, fmt.Errorf("lease is not available")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	capability := b.capability
	if capability == "" {
		capability = b.lease.NextNeed()
	}
	if capability == "" {
		return false, fmt.Errorf("capability is required")
	}

	completed, err := b.awaitJobsComplete(ctx, jobIds, info.JobKey.TenantId)
	if err != nil {
		return false, err
	}
	if completed {
		return false, nil
	}
	waitFor := make([]pgwf.JobID, 0, len(jobIds))
	for _, id := range jobIds {
		if id == "" {
			return false, fmt.Errorf("jobId cannot be empty")
		}
		waitFor = append(waitFor, pgwf.JobID(id))
	}
	payload := b.lease.Payload()
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	deps := pgwf.JobDependencies{
		NextNeed: capability,
		WaitFor:  waitFor,
	}
	if err := b.lease.Reschedule(context.TODO(), deps, payload); err != nil {
		return false, err
	}
	return true, nil
}

func (b *defaultRunnerBackend) awaitJobsComplete(ctx context.Context, jobIds []string, tenantId string) (bool, error) {
	if tenantId == "" {
		return false, fmt.Errorf("tenantId is required")
	}
	jobKeys := make([]swf.JobKey, 0, len(jobIds))
	jobIDSet := make(map[string]struct{}, len(jobIds))
	for _, id := range jobIds {
		if id == "" {
			return false, fmt.Errorf("jobId cannot be empty")
		}
		jobKeys = append(jobKeys, swf.JobKey{TenantId: tenantId, JobId: id})
		jobIDSet[id] = struct{}{}
	}
	pageToken := ""
	for {
		resp, err := b.engine.ListJobs(ctx, swf.ListJobsRequest{
			TenantIds: []string{tenantId},
			Stores:    []swf.JobStore{swf.JobStoreActive},
			JobKeys:   jobKeys,
			PageSize:  swf.MaxListJobsPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return false, err
		}
		for _, job := range resp.Jobs {
			if _, ok := jobIDSet[job.JobKey.JobId]; ok {
				return false, nil
			}
		}
		if resp.NextPageToken == "" {
			return true, nil
		}
		pageToken = resp.NextPageToken
	}
}

func (b *defaultRunnerBackend) AfterSaveTaskOutput(output swf.TaskData, dataBytes swf.Data, artifacts []swf.Artifact, digests []string, key story.Key, ordinal int64, logger *slog.Logger) (swf.TaskData, error) {
	return wrapOutputArtifactsWithFallback(output, dataBytes, artifacts, digests, key, ordinal, b.engine.strata, logger)
}
