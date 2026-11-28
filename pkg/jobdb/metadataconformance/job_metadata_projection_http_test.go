package metadataconformance_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
)

type httpMetadataProjection struct {
	Entries  map[string]jobdb.JobMetadataEntry `json:"extendedMetadata"`
	Complete *bool                             `json:"extendedMetadataComplete"`
}

func (projection httpMetadataProjection) firstVersion(key string) *jobdb.MetadataVersion {
	complete := projection.Entries != nil && projection.Complete != nil && *projection.Complete
	return firstMetadataVersion(projection.Entries, complete, key)
}

func firstMetadataVersion(entries map[string]jobdb.JobMetadataEntry, complete bool, key string) *jobdb.MetadataVersion {
	if _, found := entries[key]; found || !complete {
		return nil
	}
	var zero jobdb.MetadataVersion
	return &zero
}

func metadataJSONRequest(t *testing.T, server *httptest.Server, method, path, body string, status int) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, status, resp.StatusCode, "%s %s: %s", method, path, raw)
	return raw
}

func readMetadataProjections(t *testing.T, server *httptest.Server, jobs, jobID string) map[string]httpMetadataProjection {
	t.Helper()
	exactRaw := metadataJSONRequest(t, server, http.MethodGet, jobs+"/"+jobID, "", http.StatusOK)
	compactRaw := metadataJSONRequest(t, server, http.MethodPost, jobs+"/query", `{}`, http.StatusOK)
	var exact httpMetadataProjection
	var compact struct {
		Jobs []httpMetadataProjection `json:"jobs"`
	}
	require.NoError(t, json.Unmarshal(exactRaw, &exact))
	require.NoError(t, json.Unmarshal(compactRaw, &compact))
	require.Len(t, compact.Jobs, 1)
	return map[string]httpMetadataProjection{"exact": exact, "compact": compact.Jobs[0]}
}

func submitMetadataProjectionJob(t *testing.T, ctx context.Context, store jobdb.WorkflowRuntime) jobdb.JobKey {
	t.Helper()
	handle, err := celltest.Submit(store, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "c2", JobID: "projection-job", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1),
	}})
	require.NoError(t, err)
	return handle.JobKey
}

func TestJobExtendedMetadataHTTPAttestedProjection(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, surface := range []string{"tenant-http", "project-http"} {
			t.Run(backend+"/"+surface, func(t *testing.T) {
				ctx := celltest.Context(t.Context())
				store := metadataStore(t, ctx, backend)
				job := submitMetadataProjectionJob(t, ctx, store)
				server, prefix := metadataServer(t, store, surface, nil)
				jobs := prefix + job.TenantId + "/jobs"
				patch := jobs + "/" + job.JobId + "/extended-metadata"
				assertProjections := func(state string, entries map[string]jobdb.JobMetadataEntry) map[string]*jobdb.MetadataVersion {
					versions := map[string]*jobdb.MetadataVersion{}
					for shape, projection := range readMetadataProjections(t, server, jobs, job.JobId) {
						require.NotNil(t, projection.Complete, "%s %s", shape, state)
						require.True(t, *projection.Complete, "%s %s", shape, state)
						require.Equal(t, entries, projection.Entries, "%s %s", shape, state)
						versions[shape] = projection.firstVersion("label")
					}
					return versions
				}
				initial := assertProjections("never-written", map[string]jobdb.JobMetadataEntry{})
				zero := jobdb.MetadataVersion(0)
				require.Equal(t, &zero, initial["exact"])
				require.Equal(t, &zero, initial["compact"])
				first := initial["exact"]
				if surface == "project-http" {
					first = initial["compact"]
				}
				value := json.RawMessage(`{"tenantId":"opaque","n":9007199254740993,"null":null}`)
				body, err := json.Marshal(jobdb.JobMetadataEdit{Key: "label", ExpectedVersion: first, Set: value})
				require.NoError(t, err)
				var patched jobdb.JobMetadataEntry
				raw := metadataJSONRequest(t, server, http.MethodPatch, patch, string(body), http.StatusOK)
				require.NoError(t, json.Unmarshal(raw, &patched))
				labeled := jobdb.JobMetadataEntry{Version: 1, Present: true, Value: value}
				require.Equal(t, labeled, patched)
				versions := assertProjections("labeled", map[string]jobdb.JobMetadataEntry{"label": labeled})
				require.Nil(t, versions["exact"])
				require.Nil(t, versions["compact"])
				metadataJSONRequest(t, server, http.MethodPatch, patch, string(body), http.StatusConflict)
				assertProjections("after stale write", map[string]jobdb.JobMetadataEntry{"label": labeled})

				body, err = json.Marshal(jobdb.JobMetadataEdit{Key: "label", ExpectedVersion: &labeled.Version, Remove: true})
				require.NoError(t, err)
				metadataJSONRequest(t, server, http.MethodPatch, patch, string(body), http.StatusOK)
				versions = assertProjections("tombstoned", map[string]jobdb.JobMetadataEntry{"label": {Version: 2, Present: false}})
				require.Nil(t, versions["exact"])
				require.Nil(t, versions["compact"])
			})
		}
	}
}

type unattestedReadBackend struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
	jobdb.ProjectExistence
}

func (b unattestedReadBackend) GetJob(ctx context.Context, key jobdb.JobKey) (jobdb.JobInfo, error) {
	job, err := b.WorkflowRuntime.GetJob(ctx, key)
	job.ExtendedMetadataComplete = false
	return job, err
}

func (b unattestedReadBackend) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	page, err := b.WorkflowRuntime.ListJobs(ctx, req)
	for i := range page.Jobs {
		page.Jobs[i].ExtendedMetadataComplete = false
	}
	return page, err
}

func TestJobExtendedMetadataHTTPUnsupportedBackend(t *testing.T) {
	for _, surface := range []string{"tenant-http", "project-http"} {
		t.Run(surface, func(t *testing.T) {
			ctx := celltest.Context(t.Context())
			store := metadataStore(t, ctx, "toy")
			job := submitMetadataProjectionJob(t, ctx, store)
			backend := unattestedReadBackend{
				WorkflowRuntime:    store,
				ConfigurationAdmin: store.(jobdb.ConfigurationAdmin),
				ProjectExistence:   store.(jobdb.ProjectExistence),
			}
			require.NotImplements(t, (*jobdb.JobMetadataRuntime)(nil), backend)
			server, prefix := metadataServer(t, backend, surface, nil)
			jobs := prefix + job.TenantId + "/jobs"
			for shape, projection := range readMetadataProjections(t, server, jobs, job.JobId) {
				require.Nil(t, projection.Entries, shape)
				require.Nil(t, projection.Complete, shape)
				require.Nil(t, projection.firstVersion("label"), shape)
			}
			var zero jobdb.MetadataVersion
			entry, err := store.EditJobMetadata(ctx, job, jobdb.JobMetadataEdit{Key: "existing", ExpectedVersion: &zero, Set: json.RawMessage(`null`)})
			require.NoError(t, err)
			for shape, projection := range readMetadataProjections(t, server, jobs, job.JobId) {
				require.Equal(t, map[string]jobdb.JobMetadataEntry{"existing": entry}, projection.Entries, shape)
				require.Nil(t, projection.Complete, shape)
				require.Nil(t, projection.firstVersion("label"), shape)
			}
		})
	}
}

func TestJobExtendedMetadataHTTPLegacyPeer(t *testing.T) {
	job := jobdb.JobKey{TenantId: "c2", JobId: "old-job"}
	const fields = `"goalId":"legacy-goal","otherGoalIds":[],"clientPayloadRevision":"0","executionState":{},"status":"READY","cell":"test","cellId":"old-cell","cellRepo":"example.test/test"`
	for _, family := range []string{"tenants", "projects"} {
		for _, fixture := range []struct{ name, fields string }{
			{"omitted", ""},
			{"unattested-empty-map", `,"extendedMetadata":{}`},
			{"attestation-without-map", `,"extendedMetadataComplete":true`},
			{"attestation-with-null-map", `,"extendedMetadataComplete":true,"extendedMetadata":null`},
		} {
			t.Run(family+"/"+fixture.name, func(t *testing.T) {
				jobs := "/v1/" + family + "/" + job.TenantId + "/jobs"
				scopeKey := "tenantId"
				if family == "projects" {
					scopeKey = "projectId"
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.Method == http.MethodGet && r.URL.Path == jobs+"/"+job.JobId:
						_, _ = io.WriteString(w, `{`+fields+fixture.fields+`}`)
					case r.Method == http.MethodPost && r.URL.Path == jobs+"/query":
						_, _ = io.WriteString(w, `{"jobs":[{`+fields+fixture.fields+`,"jobKey":{"`+scopeKey+`":"c2","jobId":"old-job"}}]}`)
					default:
						http.NotFound(w, r)
					}
				}))
				t.Cleanup(server.Close)
				newClient := remote.New
				if family == "projects" {
					newClient = remote.NewProjectRuntime
				}
				client, err := newClient(server.URL, server.Client())
				require.NoError(t, err)
				info, err := client.GetJob(t.Context(), job)
				require.NoError(t, err)
				require.Equal(t, "legacy-goal", info.GoalID)
				require.False(t, info.ExtendedMetadataComplete)
				require.Nil(t, firstMetadataVersion(info.ExtendedMetadata, info.ExtendedMetadataComplete, "label"))

				page, err := client.ListJobs(t.Context(), jobdb.ListJobsRequest{TenantIds: []string{job.TenantId}})
				require.NoError(t, err)
				require.Len(t, page.Jobs, 1)
				item := page.Jobs[0]
				require.Equal(t, job, item.JobKey)
				require.False(t, item.ExtendedMetadataComplete)
				require.Nil(t, firstMetadataVersion(item.ExtendedMetadata, item.ExtendedMetadataComplete, "label"))
			})
		}
	}
}
