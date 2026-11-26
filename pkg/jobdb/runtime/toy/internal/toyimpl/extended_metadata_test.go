package toyimpl

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadataaudittest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadataedit"
	"github.com/stretchr/testify/require"
)

func TestJobExtendedMetadataToyAuditAtomicity(t *testing.T) {
	ctx := celltest.Context(context.Background())
	runtime := New(WithProjects(celltest.UnrestrictedProjects{}))
	handle, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, Routes: []jobdb.Route{{JobType: "work"}}, WorkerID: "worker", LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
	writer := lease.(jobdb.JobMetadataLease)
	version := jobdb.MetadataVersion(0)
	resolved := json.RawMessage(`null`)
	edit := jobdb.JobMetadataEdit{Key: "key", ExpectedVersion: &version, Set: resolved}
	entry, err := writer.EditJobMetadata(ctx, edit)
	require.NoError(t, err)
	require.EqualValues(t, 1, entry.Version)
	entry.Value[0] = 'X'
	_, err = writer.EditJobMetadata(ctx, edit)
	require.ErrorIs(t, err, jobdb.ErrConflict)
	version = 1
	unchanged, err := runtime.EditJobMetadata(ctx, handle.JobKey, edit)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobMetadataEntry{Version: 1, Present: true, Value: resolved}, unchanged)
	removed, err := runtime.EditJobMetadata(ctx, handle.JobKey, jobdb.JobMetadataEdit{Key: edit.Key, ExpectedVersion: &version, Remove: true})
	require.NoError(t, err)
	require.EqualValues(t, 2, removed.Version)
	failure := jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "failed after edits"}}
	require.NoError(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: failure}}}))
	_, err = writer.EditJobMetadata(ctx, jobdb.JobMetadataEdit{Key: "stale", ExpectedVersion: &version, Remove: true})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	runtime.engine.mu.Lock()
	defer runtime.engine.mu.Unlock()
	record := runtime.engine.jobRecords[handle.JobKey]
	require.NotNil(t, record)
	record.mu.Lock()
	defer record.mu.Unlock()
	require.Equal(t, []jobMetadataEvent{
		{Key: edit.Key, Entry: jobdb.JobMetadataEntry{Version: 1, Present: true, Value: resolved}},
		{Key: edit.Key, Entry: removed},
	}, record.jobMetadataEvents)
}

func TestJobExtendedMetadataToyHTTPAudit(t *testing.T) {
	for _, surface := range []struct{ name, family string }{{"tenant-http", "tenants"}, {"project-http", "projects"}} {
		t.Run(surface.name, func(t *testing.T) {
			runtime := New(WithProjects(celltest.UnrestrictedProjects{}))
			jobmetadataaudittest.RunHTTP(t, surface.family, runtime, func(key jobdb.JobKey) []jobmetadataaudittest.Event {
				runtime.engine.mu.Lock()
				defer runtime.engine.mu.Unlock()
				record := runtime.engine.jobRecords[key]
				require.NotNil(t, record)
				record.mu.Lock()
				defer record.mu.Unlock()
				var events []jobmetadataaudittest.Event
				for _, event := range record.jobMetadataEvents {
					events = append(events, jobmetadataaudittest.Event{JobKey: key, Key: event.Key, Entry: jobmetadataedit.Clone(event.Entry)})
				}
				return events
			})
		})
	}
}
