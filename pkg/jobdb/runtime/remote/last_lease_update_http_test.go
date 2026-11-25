package remote_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
)

func TestPublishedLastLeaseUpdateLists(t *testing.T) {
	ctx := celltest.Context(context.Background())
	runtime, err := sqlite.NewFromConfig(ctx, sqlite.Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	listCheck(t, err)
	defer runtime.Close(ctx)
	makeJob := func(id, kind string) jobdb.JobKey {
		handle, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: id, Cell: "test", CellRepo: "example.test/test", JobType: kind, Data: jobdb.NewTaskDataOrPanic(1)}})
		listCheck(t, err)
		return handle.JobKey
	}
	never := makeJob("never", "listed")
	active := makeJob("active", "listed")
	makeJob("excluded", "filtered")
	hidden := makeJob("hidden", "listed")
	deleted, err := runtime.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "tenant", JobIDs: []string{hidden.JobId}})
	listCheck(t, err)
	if deleted.DeletedCount != 1 {
		t.Fatalf("hidden job was not deleted: %+v", deleted)
	}
	lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: active, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "listed"}}, LeaseDuration: time.Hour})
	listCheck(t, err)
	renewed, err := runtime.RenewExecutionLeaseByID(ctx, active, lease.LeaseID(), "worker", time.Minute)
	listCheck(t, err)
	wantExpiry := renewed.LeaseExpiry()
	wantUpdate := wantExpiry.Add(-time.Minute)
	request := jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{"listed"}, CellNames: []string{"test"}, PageSize: 1}
	first, err := runtime.ListJobs(ctx, request)
	listCheck(t, err)
	if len(first.Jobs) != 1 || first.Jobs[0].JobKey != active || first.NextPageToken == "" {
		t.Fatalf("unexpected direct first page: %+v", first)
	}
	for _, scope := range []string{"tenants", "projects"} {
		t.Run(scope, func(t *testing.T) {
			var capture []byte
			var mu sync.Mutex
			wire := func() []byte {
				mu.Lock()
				defer mu.Unlock()
				return capture
			}
			var oldServer atomic.Bool
			handler := celltest.Handler(remote.NewServer(runtime))
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, req *http.Request) {
				if req.URL.Path != "/v1/"+scope+"/tenant/jobs/query" || req.Method != http.MethodPost {
					t.Errorf("unexpected list route: %s %s", req.Method, req.URL.Path)
				}
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, req)
				raw := bytes.Clone(recorder.Body.Bytes())
				if oldServer.Load() && recorder.Code == http.StatusOK {
					var envelope map[string]json.RawMessage
					var jobs []map[string]json.RawMessage
					if json.Unmarshal(raw, &envelope) != nil || json.Unmarshal(envelope["jobs"], &jobs) != nil {
						http.Error(response, "invalid list response", http.StatusInternalServerError)
						return
					}
					for _, row := range jobs {
						delete(row, "lastLeaseUpdatedAt")
					}
					envelope["jobs"], _ = json.Marshal(jobs)
					raw, _ = json.Marshal(envelope)
				}
				mu.Lock()
				capture = raw
				mu.Unlock()
				response.Header().Set("Content-Type", "application/json")
				response.WriteHeader(recorder.Code)
				_, _ = response.Write(raw)
			}))
			defer server.Close()
			var client *remote.Runtime
			if scope == "tenants" {
				client, err = remote.New(server.URL, server.Client())
			} else {
				client, err = remote.NewProjectRuntime(server.URL, server.Client())
			}
			listCheck(t, err)
			page, err := client.ListJobs(ctx, request)
			listCheck(t, err)
			if len(page.Jobs) != 1 || page.Jobs[0].JobKey != active || page.NextPageToken != first.NextPageToken {
				t.Fatalf("unexpected first page: %+v", page)
			}
			row := page.Jobs[0]
			raw := wire()
			if row.LastLeaseUpdatedAt == nil || !row.LastLeaseUpdatedAt.Equal(wantUpdate) || row.LeaseExpiresAt == nil || !row.LeaseExpiresAt.Equal(wantExpiry) {
				t.Fatalf("unexpected lease time or expiry: %+v", row)
			}
			if !bytes.Contains(raw, []byte(`"lastLeaseUpdatedAt":"`+wantUpdate.Format(time.RFC3339Nano)+`"`)) {
				t.Fatalf("missing exact wire time in %s", raw)
			}
			var legacy struct {
				Jobs          []struct{ LeaseExpiresAt *time.Time }
				NextPageToken string
			}
			listCheck(t, json.NewDecoder(bytes.NewReader(raw)).Decode(&legacy))
			if len(legacy.Jobs) != 1 || legacy.Jobs[0].LeaseExpiresAt == nil || !legacy.Jobs[0].LeaseExpiresAt.Equal(wantExpiry) || legacy.NextPageToken != page.NextPageToken {
				t.Fatalf("legacy fields changed: %+v", legacy)
			}
			if baseline := os.Getenv("JOBDB_LAST_LEASE_BASELINE"); baseline != "" {
				t.Run("old-client", func(t *testing.T) {
					timeout, cancel := context.WithTimeout(ctx, 10*time.Second)
					defer cancel()
					output, err := exec.CommandContext(timeout, baseline, "list", scope, server.URL).CombinedOutput()
					if err != nil {
						t.Fatalf("pinned old client: %v: %s", err, output)
					}
					var old jobdb.ListJobsResponse
					listCheck(t, json.Unmarshal(output, &old))
					if len(old.Jobs) != 1 || old.Jobs[0].JobKey != active || old.Jobs[0].LeaseExpiresAt == nil || !old.Jobs[0].LeaseExpiresAt.Equal(wantExpiry) || old.NextPageToken != page.NextPageToken {
						t.Fatalf("pinned old client got %s", output)
					}
				})
			}
			continued := request
			continued.PageToken = page.NextPageToken
			page, err = client.ListJobs(ctx, continued)
			listCheck(t, err)
			raw = wire()
			if len(page.Jobs) != 1 || page.Jobs[0].JobKey != never || page.Jobs[0].LastLeaseUpdatedAt != nil || page.Jobs[0].LeaseExpiresAt != nil || page.NextPageToken != "" || bytes.Contains(raw, []byte(`"lastLeaseUpdatedAt"`)) {
				t.Fatalf("unknown time not omitted or pagination changed: %s", raw)
			}
			oldServer.Store(true)
			page, err = client.ListJobs(ctx, request)
			listCheck(t, err)
			if len(page.Jobs) != 1 || page.Jobs[0].JobKey != active || page.Jobs[0].LastLeaseUpdatedAt != nil || page.Jobs[0].LeaseExpiresAt == nil || !page.Jobs[0].LeaseExpiresAt.Equal(wantExpiry) {
				t.Fatalf("could not read legacy server response: %s", wire())
			}
		})
	}
}

func TestPinnedLastLeaseUpdateOldServer(t *testing.T) {
	baseline := os.Getenv("JOBDB_LAST_LEASE_BASELINE")
	if baseline == "" {
		t.Skip("set JOBDB_LAST_LEASE_BASELINE to the separately built pre-feature binary")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(deadline, baseline, "serve", filepath.Join(t.TempDir(), "old.db"))
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	listCheck(t, err)
	listCheck(t, cmd.Start())
	defer func() { cancel(); _ = cmd.Wait() }()
	address, err := bufio.NewReader(stdout).ReadString('\n')
	listCheck(t, err)
	for _, scope := range []string{"tenants", "projects"} {
		t.Run(scope, func(t *testing.T) {
			var client *remote.Runtime
			var err error
			if scope == "tenants" {
				client, err = remote.New(strings.TrimSpace(address), nil)
			} else {
				client, err = remote.NewProjectRuntime(strings.TrimSpace(address), nil)
			}
			listCheck(t, err)
			page, err := client.ListJobs(celltest.Context(deadline), jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{"listed"}})
			listCheck(t, err)
			if len(page.Jobs) != 1 || page.Jobs[0].JobKey.JobId != "active" || page.Jobs[0].LastLeaseUpdatedAt != nil || page.Jobs[0].LeaseExpiresAt == nil {
				t.Fatalf("new client could not read old server page: %+v", page)
			}
		})
	}
}

func listCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
