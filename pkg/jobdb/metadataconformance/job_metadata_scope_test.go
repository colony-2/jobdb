package metadataconformance_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestJobExtendedMetadataReadScope(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		ctx := celltest.Context(context.Background())
		store := metadataStore(t, ctx, backend)
		handle, err := celltest.Submit(store, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
			TenantId: "c2", JobID: "scoped", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1),
		}})
		require.NoError(t, err)
		key := handle.JobKey
		var zero jobdb.MetadataVersion
		entry, err := store.EditJobMetadata(ctx, key, jobdb.JobMetadataEdit{Key: "exact", ExpectedVersion: &zero, Set: json.RawMessage(`{"n":9007199254740993}`)})
		require.NoError(t, err)
		set, err := store.EditJobMetadata(ctx, key, jobdb.JobMetadataEdit{Key: "removed", ExpectedVersion: &zero, Set: json.RawMessage(`null`)})
		require.NoError(t, err)
		removed, err := store.EditJobMetadata(ctx, key, jobdb.JobMetadataEdit{Key: "removed", ExpectedVersion: &set.Version, Remove: true})
		require.NoError(t, err)
		legacy, err := store.GetJob(ctx, key)
		require.NoError(t, err)
		other, err := store.(jobdb.CellRuntime).CreateCell(ctx, jobdb.CreateCellRequest{TenantID: key.TenantId, Name: "other", Repo: "example.test/other"})
		require.NoError(t, err)
		permissions := map[string]bool{"jobs.get": true, "jobs.list": true}

		for _, surface := range []string{"native-go", "tenant-http", "project-http"} {
			t.Run(backend+"/"+surface, func(t *testing.T) {
				for _, test := range []struct {
					name      string
					authority jobdb.CellAuthority
					visible   bool
					listError error
				}{
					{"in-scope", jobdb.CellAuthority{Principal: "reader", TenantID: key.TenantId, Cells: map[string]bool{legacy.CellID: true}, Permissions: permissions}, true, nil},
					{"cross-tenant", jobdb.CellAuthority{Principal: "reader", TenantID: "0", Permissions: permissions}, false, jobdb.ErrCellForbidden},
					{"wrong-cell", jobdb.CellAuthority{Principal: "reader", TenantID: key.TenantId, Cells: map[string]bool{other.ID: true}, Permissions: permissions}, false, nil},
					{"legacy-detail-only", jobdb.CellAuthority{Principal: "reader", TenantID: key.TenantId, Permissions: map[string]bool{"jobs.get": true}}, false, jobdb.ErrCellForbidden},
				} {
					t.Run(test.name, func(t *testing.T) {
						client, _, _ := metadataClient(t, store, surface, &test.authority)
						caller := jobdb.WithCellAuthority(ctx, test.authority)
						info, err := client.GetJob(caller, key)
						require.NoError(t, err)
						require.Equal(t, legacy.CellID, info.CellID)
						require.Equal(t, legacy.Status, info.Status)
						page, err := client.ListJobs(caller, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}})
						if test.visible {
							require.True(t, info.ExtendedMetadataComplete)
							require.Equal(t, map[string]jobdb.JobMetadataEntry{"exact": entry, "removed": removed}, info.ExtendedMetadata)
							require.NoError(t, err)
							require.Len(t, page.Jobs, 1)
							require.True(t, page.Jobs[0].ExtendedMetadataComplete)
							require.Equal(t, info.ExtendedMetadata, page.Jobs[0].ExtendedMetadata)
							return
						}
						require.False(t, info.ExtendedMetadataComplete)
						require.Empty(t, info.ExtendedMetadata)
						require.ErrorIs(t, err, test.listError)
						require.Empty(t, page.Jobs)
					})
				}
			})
		}
	}
}
