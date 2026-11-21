package sqlite

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
)

func projectAdminContext() context.Context {
	return jobdb.WithProjectAdministration(celltest.Context(context.Background()), func(context.Context) error { return nil })
}
func TestProjectRuntimeExplicitInstallation(t *testing.T) {
	ctx := projectAdminContext()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "runtime.db")}
	r, e := NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	q := jobdb.CreateCellRequest{TenantID: "c2", Name: "test", Repo: "example.test/test"}
	_, e = r.CreateCell(ctx, q)
	require.ErrorIs(t, e, jobdb.ErrProjectUnavailable)
	var n int
	require.NoError(t, r.db.QueryRow("SELECT count(*) FROM jobdb_cells").Scan(&n))
	require.Zero(t, n)
	participants := projectcatalog.InstallationDigest([]byte("isolated test: default secrets empty; blobfs empty; no external writers"))
	inv, e := InspectProjectInstallation(ctx, r.db, participants)
	require.NoError(t, e)
	require.NoError(t, InstallProjectCatalog(ctx, r.db, inv, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}))
	require.NoError(t, InstallProjectCatalog(ctx, r.db, inv, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}))
	after, e := InspectProjectInstallation(ctx, r.db, participants)
	require.NoError(t, e)
	require.Equal(t, inv, after)
	require.NoError(t, r.CheckProject(ctx, "c2"))
	_, e = r.CreateCell(ctx, q)
	require.NoError(t, e)
	_, e = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "invented", Name: "test", Repo: "example.test/test"})
	require.ErrorIs(t, e, jobdb.ErrProjectNotFound)
	p, e := r.CreateProject(ctx, jobdb.CreateProjectRequest{RequestID: "new-id", Name: "second"})
	require.NoError(t, e)
	require.NotEqual(t, "second", p.Project.ID)
	_, e = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: p.Project.ID, Name: "second", Repo: "example.test/second"})
	require.NoError(t, e)
	require.NoError(t, r.Close(ctx))
	r, e = NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	defer r.Close(ctx)
	require.NoError(t, r.CheckProject(ctx, p.Project.ID))
	_, e = r.GetCell(ctx, jobdb.CellKey{TenantID: "c2", CellID: "missing"})
	require.ErrorIs(t, e, jobdb.ErrCellNotFound)
}
func TestProjectInstallerRejectsChangedAndUnknownInventory(t *testing.T) {
	ctx := projectAdminContext()
	r, e := NewFromConfig(ctx, Config{DBPath: filepath.Join(t.TempDir(), "runtime.db"), Projects: celltest.UnrestrictedProjects{}})
	require.NoError(t, e)
	defer r.Close(ctx)
	participants := projectcatalog.InstallationDigest([]byte("synthetic participant inventory"))
	inv, e := InspectProjectInstallation(ctx, r.db, participants)
	require.NoError(t, e)
	_, e = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "c2", Name: "test", Repo: "example.test/test"})
	require.NoError(t, e)
	require.ErrorIs(t, InstallProjectCatalog(ctx, r.db, inv, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}), jobdb.ErrProjectConflict)
	var n int
	require.NoError(t, r.db.QueryRow("SELECT count(*) FROM sqlite_schema WHERE name='jobdb_project_catalog'").Scan(&n))
	require.Zero(t, n)
	inv, e = InspectProjectInstallation(ctx, r.db, participants)
	require.NoError(t, e)
	require.Equal(t, []string{"c2"}, inv.Scopes)
	require.ErrorIs(t, InstallProjectCatalog(ctx, r.db, inv, nil), jobdb.ErrProjectConflict)
	_, e = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "unexpected", Name: "test", Repo: "example.test/test"})
	require.NoError(t, e)
	_, e = InspectProjectInstallation(ctx, r.db, participants)
	require.ErrorContains(t, e, "unapproved legacy project")
}
func TestProjectHTTPHistoryOnReopenedInstalledSQLite(t *testing.T) {
	ctx := projectAdminContext()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "history.db"), Projects: celltest.UnrestrictedProjects{}}
	old, e := NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	raw := json.RawMessage(`{"tenantId":"opaque", "projectId":"also opaque", "n":9007199254740993123456789}`)
	h, e := celltest.Submit(old, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", JobID: "old-link", Cell: "test", CellRepo: "example.test/test", JobType: "job", Metadata: raw, Data: &jobdb.SimpleTaskData{Data: raw}}})
	require.NoError(t, e)
	var before []byte
	require.NoError(t, old.db.QueryRow("SELECT body FROM jobdb_chapter_chapters WHERE story_id='old-link' AND ordinal=0").Scan(&before))
	inv, e := InspectProjectInstallation(ctx, old.db, projectcatalog.InstallationDigest([]byte("synthetic history participants")))
	require.NoError(t, e)
	require.NoError(t, InstallProjectCatalog(ctx, old.db, inv, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}))
	require.NoError(t, old.Close(ctx))
	cfg.Projects = nil
	r, e := NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	defer r.Close(ctx)
	server := httptest.NewServer(localProjectTestHandler(remote.NewServer(r)))
	defer server.Close()
	legacy, e := remote.New(server.URL, server.Client())
	require.NoError(t, e)
	project, e := remote.NewProjectRuntime(server.URL, server.Client())
	require.NoError(t, e)
	for _, client := range []*remote.Runtime{legacy, project} {
		ch, e := client.GetChapter(ctx, jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 0})
		require.NoError(t, e)
		require.JSONEq(t, string(raw), string(ch.Body.(jobdb.JobStartChapter).Input.Data))
		jobs, e := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}})
		require.NoError(t, e)
		require.Len(t, jobs.Jobs, 1)
		require.Equal(t, h.JobKey, jobs.Jobs[0].JobKey)
	}
	var after []byte
	require.NoError(t, r.db.QueryRow("SELECT body FROM jobdb_chapter_chapters WHERE story_id='old-link' AND ordinal=0").Scan(&after))
	require.Equal(t, before, after)
	_, e = project.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"missing"}})
	require.Error(t, e)
}

func localProjectTestHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(jobdb.WithProjectAdministration(celltest.Context(r.Context()), func(context.Context) error { return nil })))
	})
}

// Goal conversion and project installation are separate, explicit operations.
// Project installation must preserve every row produced by the current converter.
func TestProjectInstallationAfterExplicitGoalMigration(t *testing.T) {
	ctx := projectAdminContext()
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open(sqliteDriverName, path)
	require.NoError(t, err)
	defer db.Close()
	raw, err := os.ReadFile("testdata/goal-upstream-11.sql")
	require.NoError(t, err)
	// Materialize the synthetic fixture in the approved c2 scope, including
	// its serialized goal/preparation identities, before taking any snapshots.
	fixture := strings.ReplaceAll(strings.ReplaceAll(string(raw), "'tenant'", "'c2'"), `"tenant"`, `"c2"`)
	fixture = regexp.MustCompile(`X'([0-9A-Fa-f]+)'`).ReplaceAllStringFunc(fixture, func(literal string) string {
		decoded, decodeErr := hex.DecodeString(literal[2 : len(literal)-1])
		require.NoError(t, decodeErr)
		decoded = []byte(strings.ReplaceAll(string(decoded), `"tenant"`, `"c2"`))
		return "X'" + hex.EncodeToString(decoded) + "'"
	})
	_, err = db.Exec(fixture)
	require.NoError(t, err)
	participants := projectcatalog.InstallationDigest([]byte("synthetic fenced goal migration participants"))
	for _, format := range []int{11, 12} {
		_, err = db.Exec(fmt.Sprintf("PRAGMA user_version=%d", format))
		require.NoError(t, err)
		before, schema := compatibilitySnapshot(t, db), schemaSnapshot(t, db)
		inv, err := InspectProjectInstallation(ctx, db, participants)
		require.ErrorContains(t, err, "requires current SQLite format 13")
		require.ErrorContains(t, InstallProjectCatalog(ctx, db, inv, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}), "requires current SQLite format 13")
		require.Equal(t, before, compatibilitySnapshot(t, db))
		require.Equal(t, schema, schemaSnapshot(t, db))
	}
	_, err = db.Exec("PRAGMA user_version=11")
	require.NoError(t, err)
	report, err := MigrateGoals(ctx, path)
	require.NoError(t, err, "%+v", report.Issues)
	before := compatibilitySnapshot(t, db)
	inv, err := InspectProjectInstallation(ctx, db, participants)
	require.NoError(t, err)
	require.Equal(t, 13, inv.Format)
	require.Len(t, inv.Tables, 15)
	require.NoError(t, InstallProjectCatalog(ctx, db, inv, []projectcatalog.Seed{{ID: "c2", Name: "c2"}}))
	after := compatibilitySnapshot(t, db)
	for table, rows := range before {
		require.Equal(t, rows, after[table], table)
	}
	r, err := NewFromConfig(ctx, Config{DBPath: path})
	require.NoError(t, err)
	defer r.Close(ctx)
	key := jobdb.JobKey{TenantId: "c2", JobId: "saved"}
	server := httptest.NewServer(localProjectTestHandler(remote.NewServer(r)))
	defer server.Close()
	legacy, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	project, err := remote.NewProjectRuntime(server.URL, server.Client())
	require.NoError(t, err)
	for _, client := range []*remote.Runtime{legacy, project} {
		result, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}, JobKeys: []jobdb.JobKey{key}})
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		require.Equal(t, key, result.Jobs[0].JobKey)
		assertStartArtifact(t, ctx, client, key, "saved.bin", []byte("saved upstream artifact"))
	}
}
