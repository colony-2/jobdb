package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore"
	"github.com/stretchr/testify/require"
)

func TestCompletionSummaryExistingRowsWithoutHistory(t *testing.T) {
	ctx := context.Background()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "existing.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(ctx) })
	cases := []struct {
		id, status, detail string
	}{
		{"success", "success", ""},
		{"failure", "failed_system", "original error\n"},
		{"unknown", "future_category", "future detail"},
		{"missing", "", ""},
	}
	for _, tc := range cases {
		h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: tc.id, JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
		require.NoError(t, err)
		// Populate only pre-existing scheduler columns. No final chapter is
		// needed to expose a stored outcome, including one from an older writer.
		_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET archived_at_ns = ?, completion_status = ?, completion_detail = ? WHERE tenant_id = ? AND job_id = ?`, time.Now().UnixNano(), nullableString(tc.status), nullableString(tc.detail), h.JobKey.TenantId, h.JobKey.JobId)
		require.NoError(t, err)
	}
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	// Listing may validate the store exists, but must not invoke chapter or
	// artifact operations. An unconfigured store cannot service those calls.
	original := r.chapterStore
	r.chapterStore = &chapterstore.Store{}
	defer func() { r.chapterStore = original }()
	for _, tc := range cases {
		listed, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{{TenantId: "tenant", JobId: tc.id}}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived}})
		require.NoError(t, err)
		require.Len(t, listed.Jobs, 1)
		require.Equal(t, jobdb.JobStatusCompleted, listed.Jobs[0].Status)
		require.Equal(t, tc.status, listed.Jobs[0].CompletionStatus)
		require.Equal(t, tc.detail, listed.Jobs[0].CompletionDetail)
	}
}
