package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestProjectSelectorsAndPublicCellView(t *testing.T) {
	backend := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
	calls := 0
	h := localCellAdministration(remote.NewServer(backend))
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Contains(t, r.URL.Path, "/v1/projects/c2/cells")
		h.ServeHTTP(w, r)
	}))
	defer s.Close()
	for _, selectors := range [][]string{{"--project", "c2"}, {"--tenant", "c2", "--project", "c2"}, {"--project", "c2", "--tenant", "c2"}} {
		cmd := newCellCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs(append([]string{"create", "--name", "test", "--repo", "example.test/test", "--url", s.URL}, selectors...))
		require.NoError(t, cmd.Execute())
		require.Contains(t, out.String(), `"projectId":"c2"`)
		require.NotContains(t, out.String(), `"tenantId"`)
	}
	before := calls
	for _, selectors := range [][]string{{"--project", "c2", "--tenant", "other"}, {"--tenant", "other", "--project", "c2"}} {
		cmd := newCellCmd()
		cmd.SetArgs(append([]string{"create", "--name", "test", "--repo", "example.test/test", "--url", s.URL}, selectors...))
		require.ErrorContains(t, cmd.Execute(), "different IDs")
	}
	require.Equal(t, before, calls)
}

func TestProjectCreateCommandReceiptRetry(t *testing.T) {
	ctx := jobdb.WithProjectAdministration(context.Background(), func(context.Context) error { return nil })
	c := &projectcatalog.Service{Store: &projectcatalog.MemoryStore{}}
	require.NoError(t, c.Install(ctx, projectcatalog.Installation{Manifest: projectcatalog.InstallationDigest([]byte("isolated CLI fixture"))}))
	s := httptest.NewServer(localCellAdministration(remote.NewServer(toy.New(toy.WithProjects(c)))))
	defer s.Close()
	run := func() string {
		cmd := newProjectsCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs([]string{"create", "work", "--request-id", "retry-id", "--url", s.URL})
		require.NoError(t, cmd.Execute())
		return out.String()
	}
	require.Equal(t, run(), run())
}

func TestProjectPublicGoalAndCandidateViews(t *testing.T) {
	for _, v := range []any{jobdb.GoalInfo{GoalKey: jobdb.GoalKey{TenantId: "c2", GoalID: "goal"}}, jobdb.ListJobsResponse{Jobs: []jobdb.JobSummary{{JobKey: jobdb.JobKey{TenantId: "c2", JobId: "job"}, Metadata: json.RawMessage(`{"tenantId":"opaque"}`)}}}} {
		var out bytes.Buffer
		require.NoError(t, encodeProjectCLI(&out, v))
		require.Contains(t, out.String(), `"projectId":"c2"`)
		require.NotContains(t, out.String(), `"tenantId":"c2"`)
	}
}

func TestProjectInitializeAndInspectCLI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fresh.db")
	initializeCLIProjectFixture(t, path)
	cmd := newProjectsCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"installation", "inspect", "--database", path, "--participants-sha256", projectcatalog.InstallationDigest([]byte("explicit CLI test fixture participant inventory"))})
	require.NoError(t, cmd.Execute())
	var inventory struct {
		Format int `json:"format"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &inventory))
	require.Equal(t, 15, inventory.Format)
	require.Contains(t, out.String(), `"scopes":[]`)
	cmd = newProjectsCmd()
	cmd.SetArgs([]string{"installation", "initialize", "--database", path, "--participants-sha256", projectcatalog.InstallationDigest([]byte("fixture"))})
	require.ErrorContains(t, cmd.Execute(), "new database path")
}
