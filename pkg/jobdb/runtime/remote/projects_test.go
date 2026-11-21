package remote

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func projectTestCatalog(t *testing.T) *projectcatalog.Service {
	t.Helper()
	c := &projectcatalog.Service{Store: &projectcatalog.MemoryStore{}, CanDelete: func(context.Context, string) error { return nil }}
	require.NoError(t, c.Install(projectTestAdmin(context.Background()), projectcatalog.Installation{Manifest: projectcatalog.InstallationDigest([]byte("isolated no external participants"))}))
	return c
}
func projectTestAdmin(ctx context.Context) context.Context {
	return jobdb.WithProjectAdministration(celltest.Context(ctx), func(context.Context) error { return nil })
}
func projectTestServer(t *testing.T, c *projectcatalog.Service) *httptest.Server {
	t.Helper()
	h := NewServer(toy.New(toy.WithProjects(c)))
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r.WithContext(projectTestAdmin(r.Context())))
	}))
	t.Cleanup(s.Close)
	return s
}
func TestProjectCatalogHTTPReceiptsAndDenials(t *testing.T) {
	c := projectTestCatalog(t)
	s := projectTestServer(t, c)
	rt, e := NewProjectRuntime(s.URL, s.Client())
	require.NoError(t, e)
	ctx := context.Background()
	q := jobdb.CreateProjectRequest{Name: "work", RequestID: "recorded-request", Metadata: json.RawMessage(`{"tenantId":"opaque","n":9007199254740993123456789}`)}
	first, e := rt.CreateProject(ctx, q)
	require.NoError(t, e)
	require.Equal(t, 201, first.Status)
	require.Equal(t, "/v1/projects/"+first.Project.ID, first.Location)
	second, e := rt.CreateProject(ctx, q)
	require.NoError(t, e)
	require.Equal(t, first, second)
	q.Description = "changed"
	_, e = rt.CreateProject(ctx, q)
	require.ErrorIs(t, e, jobdb.ErrProjectConflict)
	require.ErrorIs(t, rt.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: first.Project.ID}), jobdb.ErrProjectPreconditionRequired)
	require.ErrorIs(t, rt.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: first.Project.ID, IfMatch: projectcatalog.ETag(ksuid.New().String())}), jobdb.ErrProjectRevisionMismatch)
	require.NoError(t, rt.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: first.Project.ID, IfMatch: first.ETag}))
	q.Description = ""
	retry, e := rt.CreateProject(ctx, q)
	require.NoError(t, e)
	require.Equal(t, first, retry)
	_, e = rt.GetProject(ctx, first.Project.ID)
	require.ErrorIs(t, e, jobdb.ErrProjectNotFound)
	replacement, e := rt.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "work", RequestID: "new-creation"})
	require.NoError(t, e)
	require.NotEqual(t, first.Project.ID, replacement.Project.ID)
	for _, path := range []string{"/v1/projects", "/v1/projects/" + replacement.Project.ID} {
		req, e := http.NewRequest("GET", s.URL+path, nil)
		require.NoError(t, e)
		req.Header.Set("X-JobDB-Lease-Token", "lease-does-not-administer")
		res, e := s.Client().Do(req)
		require.NoError(t, e)
		res.Body.Close()
		require.Equal(t, 403, res.StatusCode)
		require.Equal(t, "no-store", res.Header.Get("Cache-Control"))
	}
	h := NewServer(toy.New(toy.WithProjects(c)))
	denied := httptest.NewRecorder()
	h.ServeHTTP(denied, httptest.NewRequest("GET", "/v1/projects", nil))
	require.Equal(t, 403, denied.Code)
}
func TestProjectAndTenantContractsShareHistoryAndLiveLeases(t *testing.T) {
	c := projectTestCatalog(t)
	s := projectTestServer(t, c)
	project, e := NewProjectRuntime(s.URL, s.Client())
	require.NoError(t, e)
	legacy, e := New(s.URL, s.Client())
	require.NoError(t, e)
	ctx := context.Background()
	p, e := project.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "work", RequestID: "project"})
	require.NoError(t, e)
	cell, e := project.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: p.Project.ID, Name: "test", Repo: "example.test/test", Metadata: map[string]json.RawMessage{"identity": json.RawMessage(`{"tenantId":"opaque"}`)}})
	require.NoError(t, e)
	raw := json.RawMessage(`{"tenantId":"user","projectId":"also-user","n":9007199254740993123456789}`)
	h, e := legacy.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: p.Project.ID, JobID: "saved", Cell: cell.Name, JobType: "job", Data: &jobdb.SimpleTaskData{Data: raw}, Metadata: raw}})
	require.NoError(t, e)
	for _, family := range []string{"tenants", "projects"} {
		res, e := s.Client().Get(s.URL + "/v1/" + family + "/" + p.Project.ID + "/cells/" + cell.ID)
		require.NoError(t, e)
		body, e := io.ReadAll(res.Body)
		res.Body.Close()
		require.NoError(t, e)
		if family == "projects" {
			require.Contains(t, string(body), `"projectId":"`+p.Project.ID+`"`)
			require.NotContains(t, string(body), `"tenantId":"`+p.Project.ID+`"`)
		} else {
			require.Contains(t, string(body), `"tenantId":"`+p.Project.ID+`"`)
		}
		require.Contains(t, string(body), `"tenantId":"opaque"`)
	}
	for _, rt := range []*Runtime{legacy, project} {
		ch, e := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 0})
		require.NoError(t, e)
		require.JSONEq(t, string(raw), string(ch.Body.(jobdb.JobStartChapter).Input.Data))
		jobs, e := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{p.Project.ID}})
		require.NoError(t, e)
		require.Equal(t, h.JobKey, jobs.Jobs[0].JobKey)
	}
	leases, e := project.PollWork(ctx, jobdb.PollWorkRequest{TenantId: p.Project.ID, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, Limit: 1, LeaseDuration: time.Minute})
	require.NoError(t, e)
	require.Len(t, leases, 1)
	require.Equal(t, h.JobKey, leases[0].Job().JobKey)
	require.NoError(t, leases[0].KeepAlive(ctx))
	require.NoError(t, legacy.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: h.JobKey}))
	require.ErrorIs(t, leases[0].KeepAlive(ctx), jobdb.ErrExecutionLeaseLost)
	require.ErrorIs(t, project.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}), jobdb.ErrProjectConflict)
}
func TestProjectTransportNeverFallsBackAfterUncertainMutation(t *testing.T) {
	var calls atomic.Int32
	lost := errors.New("lost after commit")
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, "/v1/projects/c2/jobs", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jobKey":{"projectId":"c2","jobId":"saved"},"cell":"test","cellId":"id","cellRepo":"example.test/test","goalId":"goal"}`))
	}))
	defer s.Close()
	client := s.Client()
	next := client.Transport
	client.Transport = projectTestTransport(func(r *http.Request) (*http.Response, error) {
		res, e := next.RoundTrip(r)
		if e == nil {
			res.Body.Close()
			return nil, lost
		}
		return res, e
	})
	rt, e := NewProjectRuntime(s.URL, client)
	require.NoError(t, e)
	_, e = rt.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", Cell: "test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.ErrorIs(t, e, lost)
	require.Equal(t, int32(1), calls.Load())
}

type projectTestTransport func(*http.Request) (*http.Response, error)

func (f projectTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestProjectScopeBodyCannotOverridePath(t *testing.T) {
	c := projectTestCatalog(t)
	s := projectTestServer(t, c)
	rt, e := NewProjectRuntime(s.URL, s.Client())
	require.NoError(t, e)
	p, e := rt.CreateProject(context.Background(), jobdb.CreateProjectRequest{Name: "work", RequestID: "project"})
	require.NoError(t, e)
	for _, body := range []string{`{"tenantId":"c2","workerId":"w","routes":[],"limit":1}`, `{"projectId":"c2","workerId":"w","routes":[],"limit":1}`} {
		res, e := s.Client().Post(s.URL+"/v1/projects/"+p.Project.ID+"/jobs/poll", "application/json", strings.NewReader(body))
		require.NoError(t, e)
		res.Body.Close()
		require.Equal(t, 400, res.StatusCode)
	}
}

func TestProjectUnknownScopePreservesProtectedDenial(t *testing.T) {
	c := projectTestCatalog(t)
	// No cell authority: the retained permission check must win over catalog lookup.
	s := httptest.NewServer(NewServer(toy.New(toy.WithProjects(c))))
	defer s.Close()
	req, e := http.NewRequest(http.MethodPost, s.URL+"/v1/projects/unknown/cells", strings.NewReader(`{"name":"test","repo":"example.test/test"}`))
	require.NoError(t, e)
	req.Header.Set("Content-Type", "application/json")
	res, e := s.Client().Do(req)
	require.NoError(t, e)
	defer res.Body.Close()
	require.Equal(t, http.StatusForbidden, res.StatusCode)
}

func TestProjectContractsPreserveImmutableGoalFailureRecords(t *testing.T) {
	ctx := context.Background()
	server := projectTestServer(t, projectTestCatalog(t))
	project, err := NewProjectRuntime(server.URL, server.Client())
	require.NoError(t, err)
	legacy, err := New(server.URL, server.Client())
	require.NoError(t, err)
	created, err := project.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "work", RequestID: "goal-records"})
	require.NoError(t, err)
	cell, err := project.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: created.Project.ID, Name: "work", Repo: "example.test/work"})
	require.NoError(t, err)
	origin, err := project.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: created.Project.ID, JobID: "origin", Cell: cell.Name, JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.NoError(t, err)
	key := jobdb.GoalKey{TenantId: created.Project.ID, GoalID: origin.GoalID}
	failure, err := project.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: key, Message: "exact\nreason 保存"})
	require.NoError(t, err)
	require.NotEmpty(t, failure.FailureJobID)
	marker := jobdb.JobKey{TenantId: created.Project.ID, JobId: failure.FailureJobID}
	for _, client := range []*Runtime{legacy, project} {
		got, err := client.GetGoal(ctx, key)
		require.NoError(t, err)
		require.Equal(t, failure, got)
		info, err := client.GetJob(ctx, marker)
		require.NoError(t, err)
		require.Equal(t, jobdb.JobRecordGoalFailure, info.RecordKind)
		require.Equal(t, failure.Failure, info.TerminalFailure)
		require.Empty(t, info.OtherGoalIDs)
		page, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{created.Project.ID}, JobKeys: []jobdb.JobKey{marker}})
		require.NoError(t, err)
		require.Len(t, page.Jobs, 1)
		require.Equal(t, info.RecordKind, page.Jobs[0].RecordKind)
		require.Equal(t, info.TerminalFailure, page.Jobs[0].TerminalFailure)
	}
	// Neither wire family revives the removed mutable membership endpoint.
	for _, family := range []string{"tenants", "projects"} {
		req, err := http.NewRequest(http.MethodPut, server.URL+"/v1/"+family+"/"+created.Project.ID+"/goals/"+origin.GoalID+"/candidates/origin", nil)
		require.NoError(t, err)
		response, err := server.Client().Do(req)
		require.NoError(t, err)
		response.Body.Close()
		require.Equal(t, http.StatusNotFound, response.StatusCode)
	}
}
