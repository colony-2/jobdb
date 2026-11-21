package sqlite

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
)

// This exercises catalog installation beside real existing resource rows. It is
// deliberately narrower than the complete multi-store production upgrade gate.
func TestProjectCatalogLeavesExistingHistoryBytes(t *testing.T) {
	ctx := celltest.Context(context.Background())
	runtime, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "existing.db")})
	require.NoError(t, err)
	defer runtime.Close(ctx)
	data := &jobdb.SimpleTaskData{Data: json.RawMessage(`{"tenantId":"application-owned","number":9007199254740993123456789}`), Artifacts: []jobdb.Artifact{jobdb.NewArtifactFromBytes("signed.txt", []byte("c2\x00signed historical bytes\n"))}}
	handle, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", JobID: "preserved", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: data, Metadata: json.RawMessage(`{"tenantId":"opaque metadata","projectId":"also opaque"}`)}})
	require.NoError(t, err)
	snapshot := func() [][]byte {
		t.Helper()
		var body, payload, metadata, artifact []byte
		require.NoError(t, runtime.db.QueryRowContext(ctx, `SELECT body FROM jobdb_chapter_chapters WHERE anthology_id='c2' AND story_id='preserved' AND ordinal=0`).Scan(&body))
		require.NoError(t, runtime.db.QueryRowContext(ctx, `SELECT payload,metadata FROM jobdb_jobs WHERE tenant_id='c2' AND job_id='preserved'`).Scan(&payload, &metadata))
		require.NoError(t, runtime.db.QueryRowContext(ctx, `SELECT inline_data FROM jobdb_chapter_artifacts WHERE anthology_id='c2' AND story_id='preserved' AND ordinal=0`).Scan(&artifact))
		return [][]byte{body, payload, metadata, artifact}
	}
	before := snapshot()
	catalogStore, err := projectcatalog.NewSQLStore(runtime.db, projectcatalog.SQLite)
	require.NoError(t, err)
	require.NoError(t, catalogStore.CreateSchema(ctx))
	catalog := &projectcatalog.Service{Store: catalogStore}
	admin := jobdb.WithProjectAdministration(ctx, func(context.Context) error { return nil })
	install := projectcatalog.Installation{Manifest: projectcatalog.InstallationDigest([]byte("synthetic existing c2 inventory")), Seeds: []projectcatalog.Seed{{ID: "c2", Name: "c2"}}}
	require.NoError(t, catalog.Install(admin, install))
	require.NoError(t, catalog.Install(admin, install))
	require.NoError(t, catalog.AdmitWrite(ctx, "c2"))
	_, err = catalog.CreateProject(admin, jobdb.CreateProjectRequest{Name: "another", RequestID: "new-project"})
	require.NoError(t, err)
	require.Equal(t, before, snapshot())
	server := httptest.NewServer(celltest.Handler(remote.NewServer(runtime)))
	defer server.Close()
	retained, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	chapter, err := retained.GetChapter(ctx, jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 0})
	require.NoError(t, err)
	start, ok := chapter.Body.(jobdb.JobStartChapter)
	require.True(t, ok)
	require.JSONEq(t, string(data.Data), string(start.Input.Data))
	art, err := retained.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: handle.JobKey, Ordinal: 0, Name: "signed.txt"})
	require.NoError(t, err)
	reader, err := art.Open()
	require.NoError(t, err)
	defer reader.Close()
	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("c2\x00signed historical bytes\n"), body)
	require.Equal(t, before, snapshot())
}
