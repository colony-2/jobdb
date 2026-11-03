package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestCreateRootCellCommand(t *testing.T) {
	backend := toy.New()
	server := httptest.NewServer(localCellAdministration(remote.NewServer(backend)))
	defer server.Close()
	run := func(repo string) (jobdb.Cell, error) {
		cmd := newCellCmd()
		var output bytes.Buffer
		cmd.SetOut(&output)
		cmd.SetArgs([]string{"create-root", "--tenant", "tenant", "--repo", repo, "--url", server.URL})
		err := cmd.Execute()
		var c jobdb.Cell
		if err == nil {
			err = json.Unmarshal(output.Bytes(), &c)
		}
		return c, err
	}
	first, err := run("git@github.com:org/root.git")
	require.NoError(t, err)
	require.True(t, first.IsRoot)
	again, err := run("https://github.com/org/root")
	require.NoError(t, err)
	require.Equal(t, first, again)
	_, err = run("github.com/org/other")
	require.ErrorContains(t, err, "update it explicitly with PATCH")
}

func TestCreateRootCellCommandAuthentication(t *testing.T) {
	t.Setenv("JOBDB_BEARER_TOKEN", "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"forbidden","message":"forbidden"}`))
	}))
	defer server.Close()
	cmd := newCellCmd()
	cmd.SetArgs([]string{"create-root", "--tenant", "tenant", "--repo", "github.com/org/root", "--url", server.URL})
	require.ErrorIs(t, cmd.Execute(), jobdb.ErrCellForbidden)
}
