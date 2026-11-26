package jobmetadataaudittest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
)

type Event struct {
	JobKey jobdb.JobKey
	Key    string
	Entry  jobdb.JobMetadataEntry
}

func RunHTTP(t *testing.T, family string, store jobdb.WorkflowRuntime, inspect func(jobdb.JobKey) []Event) {
	t.Helper()
	ctx, cancel := context.WithCancel(celltest.Context(context.Background()))
	defer cancel()
	handle, err := celltest.Submit(store, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
		TenantId: "c2", JobID: "audit", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1),
	}})
	require.NoError(t, err)
	key := handle.JobKey
	path := "/v1/" + family + "/c2/"
	handler := remote.NewServer(store)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !strings.HasPrefix(req.URL.Path, path) {
			t.Errorf("expected %s API route, got %s", family, req.URL.Path)
			http.NotFound(w, req)
			return
		}
		callerCtx := celltest.Context(req.Context())
		if req.Header.Get("Test-Caller") == "cross-tenant" {
			authority, _ := jobdb.CellAuthorityFromContext(callerCtx)
			authority.TenantID = "0"
			callerCtx = jobdb.WithCellAuthority(callerCtx, authority)
		}
		handler.ServeHTTP(w, req.WithContext(callerCtx))
	}))
	defer server.Close()
	var client *remote.Runtime
	if family == "projects" {
		client, err = remote.NewProjectRuntime(server.URL, server.Client())
	} else {
		client, err = remote.New(server.URL, server.Client())
	}
	require.NoError(t, err)
	lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute, WorkerID: "worker"})
	require.NoError(t, err)
	require.NotNil(t, lease)
	writer := lease.(jobdb.JobMetadataLease)
	token := lease.(interface{ LeaseToken() string }).LeaseToken()
	release := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-release:
		}
		failure := jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "failed after HTTP edits"}}
		if err := client.PutChapter(ctx, jobdb.PutChapterRequest{
			Ref: jobdb.ChapterRef{JobKey: key, Ordinal: 1}, LeaseID: lease.LeaseID(), LeaseToken: token,
			Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "task", CreatedAt: time.Now(), Body: jobdb.TaskAttemptOutcomeChapter{Outcome: failure}},
		}); err != nil {
			finished <- err
			return
		}
		finished <- lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed", Chapter: &jobdb.Chapter{
			Ordinal: 2, TaskType: "work", CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: failure},
		}})
	}()

	const exact = `{"n":9007199254740993123456789,"scale":1.00e-999}`
	set := func(key string, version jobdb.MetadataVersion, value string) jobdb.JobMetadataEdit {
		return jobdb.JobMetadataEdit{Key: key, ExpectedVersion: &version, Set: json.RawMessage(value)}
	}
	leased, err := writer.EditJobMetadata(ctx, set("removed", 0, `null`))
	require.NoError(t, err)
	require.Equal(t, jobdb.JobMetadataEntry{Version: 1, Present: true, Value: json.RawMessage(`null`)}, leased)
	_, err = writer.EditJobMetadata(ctx, set("removed", 0, `false`))
	require.ErrorIs(t, err, jobdb.ErrConflict)
	removed, err := client.EditJobMetadata(ctx, key, jobdb.JobMetadataEdit{Key: "removed", ExpectedVersion: &leased.Version, Remove: true})
	require.NoError(t, err)
	require.Equal(t, jobdb.JobMetadataEntry{Version: 2}, removed)
	request, err := http.NewRequestWithContext(ctx, http.MethodPatch, server.URL+path+"jobs/audit/extended-metadata", strings.NewReader(`{"key":"denied","expectedVersion":"0","set":true}`))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Test-Caller", "cross-tenant")
	response, err := server.Client().Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusForbidden, response.StatusCode)
	_, err = client.EditJobMetadata(ctx, key, set("", 0, `true`))
	require.ErrorIs(t, err, jobdb.ErrInvalidJobMetadata)

	results := make(chan error, 2)
	for attempt := 0; attempt < 2; attempt++ {
		go func() {
			_, err := client.EditJobMetadata(ctx, key, set("survives", 0, exact))
			results <- err
		}()
	}
	first, second := <-results, <-results
	if first != nil {
		first, second = second, first
	}
	require.NoError(t, first)
	require.ErrorIs(t, second, jobdb.ErrConflict)
	retained := jobdb.JobMetadataEntry{Version: 1, Present: true, Value: json.RawMessage(exact)}
	unchanged, err := client.EditJobMetadata(ctx, key, set("survives", 1, exact))
	require.NoError(t, err)
	require.Equal(t, retained, unchanged)
	expected := []Event{{JobKey: key, Key: "removed", Entry: leased}, {JobKey: key, Key: "removed", Entry: removed}, {JobKey: key, Key: "survives", Entry: retained}}
	require.ElementsMatch(t, expected, inspect(key), "before releasing task failure")
	close(release)
	require.NoError(t, <-finished)
	_, err = writer.EditJobMetadata(ctx, set("stale-lease", 0, `true`))
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	info, err := client.GetJob(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "failed", info.CompletionStatus)
	require.Equal(t, map[string]jobdb.JobMetadataEntry{"survives": retained, "removed": removed}, info.ExtendedMetadata)
	page, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived}})
	require.NoError(t, err)
	require.Len(t, page.Jobs, 1)
	require.Equal(t, key, page.Jobs[0].JobKey)
	require.Equal(t, info.ExtendedMetadata, page.Jobs[0].ExtendedMetadata)
	require.ElementsMatch(t, expected, inspect(key), "after task and job failure")
}
