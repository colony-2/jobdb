package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
)

func TestCellRestoreThroughBundledCommands(test *testing.T) {
	original := serveHTTPFunc
	test.Cleanup(func() { serveHTTPFunc = original })
	for _, backend := range []string{"toy", "sqlite"} {
		test.Run(backend, func(test *testing.T) {
			args := []string{backend}
			if backend == "sqlite" {
				dir := test.TempDir()
				path := filepath.Join(dir, "jobdb.db")
				initializeCLIProjectFixture(test, path)
				args = append(args, "--db", path, "--blob-dir", filepath.Join(dir, "blobs"))
			}
			served := false
			serveHTTPFunc = func(ctx context.Context, _ string, handler http.Handler, _ func(context.Context) error) error {
				served = true
				server := httptest.NewServer(handler)
				defer server.Close()
				client, err := remote.NewProjectRuntime(server.URL, server.Client())
				require.NoError(test, err)
				project, err := client.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "test", RequestID: "restore-command"})
				require.NoError(test, err)
				path := "/v1/projects/" + project.Project.ID + "/cells"
				request := func(method, path, etag string, body io.Reader, status int) ([]byte, string) {
					test.Helper()
					req, err := http.NewRequestWithContext(ctx, method, server.URL+path, body)
					require.NoError(test, err)
					if etag != "" {
						req.Header.Set("If-Match", etag)
					}
					if body != nil {
						req.Header.Set("Content-Type", "application/json")
					}
					resp, err := server.Client().Do(req)
					require.NoError(test, err)
					defer resp.Body.Close()
					data, err := io.ReadAll(resp.Body)
					require.NoError(test, err)
					require.Equal(test, status, resp.StatusCode, string(data))
					return data, resp.Header.Get("ETag")
				}
				data, etag := request("POST", path, "", strings.NewReader(`{"name":"restore-demo","repo":"github.com/org/restore-demo"}`), 201)
				var cell jobdb.Cell
				require.NoError(test, json.Unmarshal(data, &cell))
				require.NotEmpty(test, cell.ID)
				_, retiredETag := request("POST", path+"/"+cell.ID+"/retire", etag, nil, 200)
				require.NotEqual(test, etag, retiredETag)
				listed, err := client.ListCells(ctx, jobdb.ListCellsRequest{TenantID: project.Project.ID, State: "ACTIVE"})
				require.NoError(test, err)
				require.Empty(test, listed.Cells)
				data, restoredETag := request("POST", path+"/"+cell.ID+"/restore", retiredETag, nil, 200)
				var restored jobdb.Cell
				require.NoError(test, json.Unmarshal(data, &restored))
				require.Equal(test, cell.ID, restored.ID)
				require.Equal(test, jobdb.CellActive, restored.State)
				require.NotContains(test, string(data), `"retiredAt"`)
				require.Equal(test, fmt.Sprintf("%q", restored.Revision), restoredETag)
				require.NotEqual(test, retiredETag, restoredETag)
				listed, err = client.ListCells(ctx, jobdb.ListCellsRequest{TenantID: project.Project.ID, State: "ACTIVE"})
				require.NoError(test, err)
				require.Len(test, listed.Cells, 1)
				require.Equal(test, cell.ID, listed.Cells[0].ID)
				require.Equal(test, restored.Revision, listed.Cells[0].Revision)
				return nil
			}
			cmd := newRootCmd()
			cmd.SetArgs(args)
			require.NoError(test, cmd.ExecuteContext(context.Background()))
			require.True(test, served)
		})
	}
}
