package cli

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func initializeCLIProjectFixture(t *testing.T, path string) {
	t.Helper()
	cmd := newRootCmd()
	cmd.SetArgs([]string{"project", "installation", "initialize", "--database", path, "--participants-sha256", projectcatalog.InstallationDigest([]byte("explicit CLI test fixture participant inventory"))})
	require.NoError(t, cmd.ExecuteContext(context.Background()))
}
func createServingProject(t *testing.T, handler http.Handler) string {
	t.Helper()
	s := httptest.NewServer(handler)
	defer s.Close()
	rt, e := remote.New(s.URL, s.Client())
	require.NoError(t, e)
	p, e := rt.CreateProject(context.Background(), jobdb.CreateProjectRequest{Name: "test", RequestID: "serving-project-fixture"})
	require.NoError(t, e)
	return p.Project.ID
}
