package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestProvenanceHTTPNativeSourcesAndAuthorization(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			f := newDelegationFixture(t, kind, nil)
			key := f.owner.Job().JobKey
			record := jobdb.ExecutionRecord{Version: jobdb.ExecutionPatternVersion, Kind: "header", At: time.Now(), Versions: &jobdb.ExecutionVersions{ExecutionPatternVersion: jobdb.ExecutionPatternVersion, JobDBVersion: "stored-jobdb", ConsumerVersions: map[string]string{"c2j": "stored-consumer", "c2j.commit": "abc"}}}
			raw, err := json.Marshal(record)
			require.NoError(t, err)
			require.NoError(t, f.client.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: f.owner.LeaseID(), LeaseToken: f.owner.(*remoteExecutionLease).LeaseToken(), Ref: jobdb.ChapterRef{JobKey: key, Ordinal: 1}, Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "__execution__", CreatedAt: record.At, Body: jobdb.ExecutionChapter{Record: raw}}}))
			q := jobdb.ReadExecutionProvenanceRequest{JobKey: key, Consumer: "c2j"}
			local, err := jobdb.ReadExecutionProvenance(ctx, f.backend, q)
			require.NoError(t, err)
			require.Equal(t, jobdb.ExecutionProvenanceComplete, local.Status)
			require.Len(t, local.Pairs, 1)
			out, err := f.client.ReadExecutionProvenance(ctx, q)
			require.NoError(t, err)
			require.Equal(t, local.Pairs, out.Pairs)
			require.Equal(t, local.ThroughOrdinal, out.ThroughOrdinal)
			path := fmt.Sprintf("/v1/tenants/%s/jobs/%s/execution-provenance?consumer=c2j", key.TenantId, key.JobId)
			resp, err := f.server.Client().Get(f.server.URL + path)
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			resp.Body.Close()
			require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
			require.Equal(t, 200, resp.StatusCode)
			require.NoError(t, json.Unmarshal(body, &out))
			require.Equal(t, len(body), out.Usage.ResponseBytes)
			subset := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
			status, body := delegationHTTP(t, f, "GET", path, subset.LeaseToken(), nil)
			require.Equal(t, 403, status, string(body))
			require.NotContains(t, string(body), "stored-consumer")
			all := f.create(t, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Read: &jobdb.DelegationChapters{Any: true}}})
			status, body = delegationHTTP(t, f, "GET", path, all.LeaseToken(), nil)
			require.Equal(t, 200, status, string(body))
			status, body = delegationHTTP(t, f, "GET", strings.Replace(path, "/tenants/tenant/", "/tenants/other/", 1), all.LeaseToken(), nil)
			require.Contains(t, []int{401, 403}, status, string(body))
			require.NotContains(t, string(body), "stored-consumer")
			status, body = delegationHTTP(t, f, "GET", strings.Replace(path, "/tenants/tenant/", "/tenants/other/", 1), f.owner.(*remoteExecutionLease).LeaseToken(), nil)
			require.Equal(t, 409, status, string(body))
			require.NotContains(t, string(body), "stored-consumer")
			status, body = delegationHTTP(t, f, "GET", path, delegationTokenPrefix+"invalid", nil)
			require.Contains(t, []int{401, 409}, status, string(body))
			require.NotContains(t, string(body), "stored-consumer")
			// Expiry is enforced before reading provenance.
			past := time.Now().Add(100 * time.Millisecond)
			token, _, err := attenuateGrant(all.LeaseToken(), jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Read: &jobdb.DelegationChapters{Any: true}}, NotAfter: &past})
			require.NoError(t, err)
			time.Sleep(time.Until(past) + time.Millisecond)
			status, body = delegationHTTP(t, f, "GET", path, token, nil)
			require.Contains(t, []int{401, 403}, status, string(body))
			require.NotContains(t, string(body), "stored-consumer")
			_, err = f.backend.(jobdb.JobDeletionRuntime).DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: key.TenantId, JobIDs: []string{key.JobId}})
			require.NoError(t, err)
			status, body = delegationHTTP(t, f, "GET", path, "", nil)
			require.Equal(t, 404, status, string(body))
			require.NotContains(t, string(body), "stored-consumer")
		})
	}
}

type noProvenanceRuntime struct {
	jobdb.WorkflowRuntime
	jobdb.ConfigurationAdmin
}

func TestProvenanceHTTPUnsupportedAndOldServers(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	server := httptest.NewServer(celltest.Handler(NewServer(noProvenanceRuntime{f.backend, f.backend.(jobdb.ConfigurationAdmin)})))
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	_, err = client.ReadExecutionProvenance(context.Background(), jobdb.ReadExecutionProvenanceRequest{JobKey: f.owner.Job().JobKey, Consumer: "c2j"})
	require.ErrorIs(t, err, jobdb.ErrExecutionProvenanceUnsupported)
	for _, tc := range []struct {
		status int
		body   string
	}{{404, "old route"}, {405, "old route"}, {200, `{"capability":"execution_provenance_v1","status":"complete"}`}} {
		calls := 0
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			w.WriteHeader(tc.status)
			_, _ = io.WriteString(w, tc.body)
		}))
		c, err := New(s.URL, s.Client())
		require.NoError(t, err)
		out, err := c.ReadExecutionProvenance(context.Background(), jobdb.ReadExecutionProvenanceRequest{JobKey: f.owner.Job().JobKey, Consumer: "c2j"})
		require.Error(t, err)
		require.Empty(t, out.Pairs)
		require.Equal(t, 1, calls)
		s.Close()
	}
}
