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

func TestCreateCellCommand(t *testing.T) {
	backend := toy.New()
	server := httptest.NewServer(localCellAdministration(remote.NewServer(backend)))
	defer server.Close()
	run := func(repo string) (jobdb.Cell, error) {
		cmd := newCellCmd()
		var output bytes.Buffer
		cmd.SetOut(&output)
		cmd.SetArgs([]string{"create", "--name", "root", "--tenant", "tenant", "--repo", repo, "--url", server.URL})
		err := cmd.Execute()
		var c jobdb.Cell
		if err == nil {
			err = json.Unmarshal(output.Bytes(), &c)
		}
		return c, err
	}
	first, err := run("git@github.com:org/root.git")
	require.NoError(t, err)
	require.Equal(t, "root", first.Name)
	again, err := run("https://github.com/org/root")
	require.NoError(t, err)
	require.Equal(t, first, again)
	_, err = run("github.com/org/other")
	require.ErrorContains(t, err, "update it explicitly with PATCH")
}

func TestCreateCellCommandAuthentication(t *testing.T) {
	t.Setenv("JOBDB_BEARER_TOKEN", "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"forbidden","message":"forbidden"}`))
	}))
	defer server.Close()
	cmd := newCellCmd()
	cmd.SetArgs([]string{"create", "--name", "root", "--tenant", "tenant", "--repo", "github.com/org/root", "--url", server.URL})
	require.ErrorIs(t, cmd.Execute(), jobdb.ErrCellForbidden)
}

func TestCreateOrdinaryCellCommand(t *testing.T) {
	backend := toy.New()
	server := httptest.NewServer(localCellAdministration(remote.NewServer(backend)))
	defer server.Close()
	cmd := newCellCmd()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetArgs([]string{"create", "--name", "project", "--tenant", "tenant", "--repo", "github.com/org/project", "--url", server.URL})
	require.NoError(t, cmd.Execute())
	var cell jobdb.Cell
	require.NoError(t, json.Unmarshal(output.Bytes(), &cell))
	require.Equal(t, "project", cell.Name)
}

func TestCellCreateRequiresNameAndRemovesRootOnlyCommand(t *testing.T) {
	for _, args := range [][]string{{"create", "--tenant", "tenant", "--repo", "example.test/repo"}, {"create-root"}} {
		cmd := newCellCmd()
		cmd.SetArgs(args)
		require.Error(t, cmd.Execute())
	}
}
