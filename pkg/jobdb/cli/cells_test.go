package cli

import "github.com/colony-2/jobdb/pkg/jobdb/celltest"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestCreateCellCommand(t *testing.T) {
	backend := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
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
	backend := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
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

func TestCellArchiveCommandRevisions(t *testing.T) {
	t.Setenv("JOBDB_BEARER_TOKEN", "archive-token")
	for _, action := range []string{"archive", "unarchive"} {
		for _, mode := range []string{"explicit", "implicit", "stale", "edited"} {
			t.Run(action+"/"+mode, func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				backend := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
				initial, err := backend.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "example", Repo: "example.test/org/example"})
				require.NoError(t, err)
				key := jobdb.CellKey{TenantID: "tenant", CellID: initial.ID}
				if action == "unarchive" {
					initial, err = backend.ArchiveCell(ctx, jobdb.ArchiveCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", initial.Revision)})
					require.NoError(t, err)
				}
				observed := initial.Revision
				edit := func() (jobdb.Cell, error) {
					description := "concurrent edit"
					return backend.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", observed), Description: &description})
				}
				beforeMutation := initial
				if mode == "stale" {
					beforeMutation, err = edit()
					require.NoError(t, err)
				}
				type requestRecord struct{ method, path, auth, etag string }
				var lock sync.Mutex
				var requests []requestRecord
				var editErr error
				path := "/v1/projects/tenant/cells/" + key.CellID
				handler := localCellAdministration(remote.NewServer(backend))
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					lock.Lock()
					defer lock.Unlock()
					requests = append(requests, requestRecord{r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("If-Match")})
					if mode == "edited" && r.Method == http.MethodGet {
						response := httptest.NewRecorder()
						handler.ServeHTTP(response, r)
						beforeMutation, editErr = edit()
						for name, values := range response.Header() {
							w.Header()[name] = values
						}
						w.WriteHeader(response.Code)
						_, _ = w.Write(response.Body.Bytes())
						return
					}
					handler.ServeHTTP(w, r)
				}))
				defer server.Close()
				cmd := newCellCmd()
				var output bytes.Buffer
				cmd.SetOut(&output)
				cmd.SetErr(&output)
				args := []string{action, key.CellID, "--project", "tenant", "--url", server.URL}
				expected := []requestRecord{}
				if mode == "explicit" || mode == "stale" {
					args = append(args, "--revision", observed)
				} else {
					expected = append(expected, requestRecord{http.MethodGet, path, "Bearer archive-token", ""})
				}
				expected = append(expected, requestRecord{http.MethodPost, path + "/" + action, "Bearer archive-token", fmt.Sprintf("%q", observed)})
				cmd.SetArgs(args)
				err = cmd.Execute()
				lock.Lock()
				defer lock.Unlock()
				require.NoError(t, editErr)
				require.Equal(t, expected, requests)
				stored, getErr := backend.GetCell(ctx, key)
				require.NoError(t, getErr)
				if mode == "stale" || mode == "edited" {
					require.ErrorIs(t, err, jobdb.ErrCellPrecondition)
					require.ErrorContains(t, err, "stale revision")
					require.Equal(t, beforeMutation, stored)
				} else {
					require.NoError(t, err)
					var returned struct {
						jobdb.Cell
						ProjectID string `json:"projectId"`
					}
					require.NoError(t, json.Unmarshal(output.Bytes(), &returned))
					require.Equal(t, key.TenantID, returned.ProjectID)
					returned.TenantID = returned.ProjectID
					require.Equal(t, stored, returned.Cell)
					require.Equal(t, action == "archive", stored.ArchivedAt != nil)
				}
			})
		}
	}
}
