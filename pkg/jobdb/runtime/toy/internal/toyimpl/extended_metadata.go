package toyimpl

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadataedit"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
)

type jobMetadataEvent struct {
	Key   string
	Entry jobdb.JobMetadataEntry
}

func (r *Runtime) EditJobMetadata(ctx context.Context, key jobdb.JobKey, edit jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, error) {
	return r.editJobMetadata(ctx, key, edit, "", "")
}

func (l *runtimeLease) EditJobMetadata(ctx context.Context, edit jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, error) {
	return l.runtime.editJobMetadata(ctx, l.jobKey, edit, l.leaseID, l.workerID)
}

func (r *Runtime) EditJobMetadataWithVerifiedLease(ctx context.Context, key jobdb.JobKey, edit jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, error) {
	claims, ok := leaseauth.ClaimsFromContext(ctx)
	if !ok || claims.LeaseID == "" || !leaseauth.Matches(claims, key, claims.LeaseID) {
		return jobdb.JobMetadataEntry{}, jobdb.ErrExecutionLeaseLost
	}
	return r.editJobMetadata(ctx, key, edit, claims.LeaseID, claims.WorkerID)
}

func (r *Runtime) editJobMetadata(ctx context.Context, key jobdb.JobKey, edit jobdb.JobMetadataEdit, leaseID, workerID string) (jobdb.JobMetadataEntry, error) {
	if err := key.Validate(); err != nil {
		return jobdb.JobMetadataEntry{}, jobdb.ErrInvalidJobMetadata
	}
	if err := edit.Validate(); err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if leaseID == "" {
		if _, bound := leaseauth.ClaimsFromContext(ctx); bound {
			return jobdb.JobMetadataEntry{}, jobdb.ErrLeasePermissionDenied
		}
		if err := jobmetadataedit.CheckScope(ctx, key, ""); err != nil {
			return jobdb.JobMetadataEntry{}, err
		}
		if err := r.preflightJobMetadataScope(ctx, key); err != nil {
			return jobdb.JobMetadataEntry{}, err
		}
		if err := jobdb.RequireProjectWrite(ctx, r.projects, key.TenantId); err != nil {
			return jobdb.JobMetadataEntry{}, err
		}
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	record := r.engine.jobRecords[key]
	if record == nil {
		return jobdb.JobMetadataEntry{}, jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.hidden || record.recordKind.IsTerminalRecord() {
		return jobdb.JobMetadataEntry{}, jobdb.ErrJobNotFound
	}
	if leaseID != "" {
		if _, err := leaseauth.Authorize(ctx, key, leaseID); err != nil {
			return jobdb.JobMetadataEntry{}, err
		}
		if workerID == "" || !record.leased || record.leaseID != leaseID || record.leaseWorkerID != workerID || !record.leaseExpiresAt.After(r.now()) || record.cancelled || record.archived != nil {
			return jobdb.JobMetadataEntry{}, jobdb.ErrExecutionLeaseLost
		}
	} else if err := jobmetadataedit.CheckScope(ctx, key, record.cell.CellID); err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	entry, changed, err := jobmetadataedit.Apply(record.extendedMetadata[edit.Key], edit)
	if err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	if err := ctx.Err(); err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	if changed {
		if record.extendedMetadata == nil {
			record.extendedMetadata = make(map[string]jobdb.JobMetadataEntry)
		}
		record.extendedMetadata[edit.Key] = entry
		record.jobMetadataEvents = append(record.jobMetadataEvents, jobMetadataEvent{Key: edit.Key, Entry: entry})
	}
	return jobmetadataedit.Clone(entry), nil
}

func (r *Runtime) preflightJobMetadataScope(ctx context.Context, key jobdb.JobKey) error {
	record := r.engine.getJobRecord(key)
	if record == nil {
		return jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.hidden || record.recordKind.IsTerminalRecord() {
		return jobdb.ErrJobNotFound
	}
	return jobmetadataedit.CheckScope(ctx, key, record.cell.CellID)
}

func (record *jobRecord) currentJobMetadata() map[string]jobdb.JobMetadataEntry {
	if record.hidden || record.recordKind.IsTerminalRecord() || len(record.extendedMetadata) == 0 {
		return nil
	}
	entries := make(map[string]jobdb.JobMetadataEntry, len(record.extendedMetadata))
	for key, entry := range record.extendedMetadata {
		entries[key] = jobmetadataedit.Clone(entry)
	}
	return entries
}
