package metadataconformance_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type metadataRuntime interface {
	jobdb.WorkflowRuntime
	jobdb.JobMetadataRuntime
	jobdb.JobDeletionRuntime
}

func TestJobExtendedMetadataConformance(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		for _, surface := range []string{"native-go", "tenant-http", "project-http"} {
			t.Run(backend+"/"+surface, func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				store := metadataStore(t, ctx, backend)
				client, server, prefix := metadataClient(t, store, surface, nil)
				runMetadataChecks(t, ctx, client, store, server, prefix)
			})
		}
	}
}

func metadataStore(t *testing.T, ctx context.Context, backend string) metadataRuntime {
	t.Helper()
	admin := jobdb.WithProjectAdministration(ctx, func(context.Context) error { return nil })
	manifest := projectcatalog.InstallationDigest([]byte("isolated metadata conformance"))
	seeds := []projectcatalog.Seed{{ID: "c2", Name: "c2"}, {ID: "0", Name: "0"}}
	if backend == "toy" {
		catalog := &projectcatalog.Service{Store: &projectcatalog.MemoryStore{}}
		require.NoError(t, catalog.Install(admin, projectcatalog.Installation{Manifest: manifest, Seeds: seeds}))
		return toy.New(toy.WithProjects(catalog))
	}
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer db.Close()
	inv, err := sqlite.InspectProjectInstallation(ctx, db, manifest)
	require.NoError(t, err)
	require.NoError(t, sqlite.InstallProjectCatalog(admin, db, inv, seeds))
	return store
}

func metadataClient(t *testing.T, store metadataRuntime, surface string, defaultAuthority *jobdb.CellAuthority) (metadataRuntime, *httptest.Server, string) {
	t.Helper()
	if surface == "native-go" {
		return store, nil, ""
	}
	family := "tenants"
	if surface == "project-http" {
		family = "projects"
	}
	prefix := "/v1/" + family + "/"
	handler := remote.NewServer(store)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, prefix) && !(surface == "tenant-http" && r.URL.Path == "/v1/jobs/poll") {
			t.Errorf("unexpected %s route: %s", surface, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		ctx := celltest.Context(r.Context())
		if defaultAuthority != nil {
			ctx = jobdb.WithCellAuthority(ctx, *defaultAuthority)
		}
		if restriction := r.Header.Get("Test-Caller"); restriction != "" {
			authority, _ := jobdb.CellAuthorityFromContext(ctx)
			switch restriction {
			case "different-tenant":
				authority.TenantID = "0"
			case "wrong-cell":
				authority.Cells = map[string]bool{"other": true}
			case "no-principal":
				authority.Principal = ""
			}
			ctx = jobdb.WithCellAuthority(ctx, authority)
		}
		handler.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(server.Close)
	if surface == "project-http" {
		client, err := remote.NewProjectRuntime(server.URL, server.Client())
		require.NoError(t, err)
		return client, server, prefix
	}
	client, err := remote.New(server.URL, server.Client())
	require.NoError(t, err)
	return client, server, prefix
}

func runMetadataChecks(t *testing.T, ctx context.Context, client, store metadataRuntime, server *httptest.Server, prefix string) {
	t.Helper()
	const ordinary = `{"ordinary":"unchanged"}`
	const exact = `{"tenantId":"inside","projectId":"also-inside","nested":[null,9007199254740993123456789,1.00e-999]}`
	for _, tenant := range []string{"c2", "0"} {
		_, err := celltest.Submit(store, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: "job", Cell: "test", CellRepo: "example.test/test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1), Metadata: []byte(ordinary)}})
		require.NoError(t, err)
	}
	key := jobdb.JobKey{TenantId: "c2", JobId: "job"}
	version := func(number jobdb.MetadataVersion) *jobdb.MetadataVersion { return &number }
	write := func(name string, expected jobdb.MetadataVersion, value string) jobdb.JobMetadataEdit {
		return jobdb.JobMetadataEdit{Key: name, ExpectedVersion: version(expected), Set: json.RawMessage(value)}
	}
	remove := func(name string, expected jobdb.MetadataVersion) jobdb.JobMetadataEdit {
		return jobdb.JobMetadataEdit{Key: name, ExpectedVersion: version(expected), Remove: true}
	}
	expiring, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: 500 * time.Millisecond, WorkerID: "expiring"})
	require.NoError(t, err)
	require.NotNil(t, expiring)
	expiresAt := expiring.(interface{ LeaseExpiry() time.Time }).LeaseExpiry()
	require.False(t, expiresAt.IsZero())
	time.Sleep(time.Until(expiresAt) + time.Millisecond)
	_, err = expiring.(jobdb.JobMetadataLease).EditJobMetadata(ctx, write("denied", 0, `true`))
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	var lease jobdb.ExecutionLease
	deadline := time.Now().Add(3 * time.Second)
	for lease == nil && time.Now().Before(deadline) {
		lease, err = client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute, WorkerID: "worker"})
		require.NoError(t, err)
		if lease == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	require.NotNil(t, lease)
	leaseWriter := lease.(jobdb.JobMetadataLease)
	entry, err := leaseWriter.EditJobMetadata(ctx, write("lease", 0, `null`))
	require.NoError(t, err)
	require.Equal(t, jobdb.JobMetadataEntry{Version: 1, Present: true, Value: json.RawMessage(`null`)}, entry)
	entry, err = leaseWriter.EditJobMetadata(ctx, remove("lease", 1))
	require.NoError(t, err)
	require.Equal(t, jobdb.JobMetadataEntry{Version: 2}, entry)
	entry, err = client.EditJobMetadata(ctx, key, write("exact", 0, exact))
	require.NoError(t, err)
	require.Equal(t, jobdb.JobMetadataEntry{Version: 1, Present: true, Value: json.RawMessage(exact)}, entry)
	_, err = client.EditJobMetadata(ctx, key, write("lease", 0, `1`))
	require.ErrorIs(t, err, jobdb.ErrConflict)
	_, err = client.EditJobMetadata(ctx, key, remove("exact", 0))
	require.ErrorIs(t, err, jobdb.ErrConflict)
	start := make(chan struct{})
	writes := make(chan error, 4)
	for _, name := range []string{"same", "same", "independent-a", "independent-b"} {
		go func() {
			<-start
			_, err := client.EditJobMetadata(ctx, key, write(name, 0, `42`))
			writes <- err
		}()
	}
	close(start)
	conflicts := 0
	for range 4 {
		if err := <-writes; err != nil {
			require.ErrorIs(t, err, jobdb.ErrConflict)
			conflicts++
		}
	}
	require.Equal(t, 1, conflicts)
	requireDenied := func(tenant, jobID, path, credential, caller string, status int) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, server.URL+prefix+tenant+"/jobs/"+jobID+path+"/extended-metadata", strings.NewReader(`{"key":"denied","expectedVersion":"0","set":1}`))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if credential != "" {
			req.Header.Set("X-JobDB-Lease-Token", credential)
		}
		req.Header.Set("Test-Caller", caller)
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, status, resp.StatusCode, tenant+path+caller)
	}
	if server != nil {
		path := "/leases/" + lease.LeaseID()
		token := lease.(interface{ LeaseToken() string }).LeaseToken()
		requireDenied("c2", "job", "", token, "", 403)
		requireDenied("c2", "job", path, "", "", 400)
		requireDenied("c2", "other-job", path, token, "", 409)
		requireDenied("0", "job", path, token, "", 409)
		requireDenied("c2", "job", "", "", "different-tenant", 403)
		requireDenied("c2", "job", "", "", "wrong-cell", 404)
		requireDenied("c2", "job", "", "", "no-principal", 403)
		for _, permissions := range []jobdb.DelegationPermissions{{}, {Read: &jobdb.DelegationChapters{Any: true}}} {
			delegated, err := client.(jobdb.LeaseDelegator).CreateDelegation(ctx, lease, jobdb.CreateDelegationRequest{Permissions: permissions})
			require.NoError(t, err)
			requireDenied("c2", "job", path, delegated.(interface{ LeaseToken() string }).LeaseToken(), "", 403)
		}
	} else {
		denied := jobdb.WithCellAuthority(ctx, jobdb.CellAuthority{Principal: "outside", TenantID: "0"})
		_, err := client.EditJobMetadata(denied, key, write("denied", 0, `1`))
		require.ErrorIs(t, err, jobdb.ErrCellForbidden)
	}
	foreign, err := store.GetJob(ctx, jobdb.JobKey{TenantId: "0", JobId: "job"})
	require.NoError(t, err)
	require.Empty(t, foreign.ExtendedMetadata)
	previousWriter := leaseWriter
	require.NoError(t, lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "work"}}))
	previousID := lease.LeaseID()
	lease, err = client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute, WorkerID: "replacement"})
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.NotEqual(t, previousID, lease.LeaseID())
	leaseWriter = lease.(jobdb.JobMetadataLease)
	_, err = previousWriter.EditJobMetadata(ctx, write("denied", 0, `true`))
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)

	fail := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		<-fail
		failed := jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Message: "task failed after edit"}}
		request := jobdb.PutChapterRequest{Ref: jobdb.ChapterRef{JobKey: key, Ordinal: 1}, LeaseID: lease.LeaseID(), Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "task", CreatedAt: time.Now(), Body: jobdb.TaskAttemptOutcomeChapter{Outcome: failed}}}
		if credentials, ok := lease.(interface{ LeaseToken() string }); ok {
			request.LeaseToken = credentials.LeaseToken()
		}
		if err := client.PutChapter(ctx, request); err != nil {
			finished <- err
			return
		}
		finished <- lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed", Detail: "task failed after edit", Chapter: &jobdb.Chapter{Ordinal: 2, TaskType: "work", CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: failed}}})
	}()
	entry, editErr := client.EditJobMetadata(ctx, key, write("before-failure", 0, `true`))
	close(fail)
	require.NoError(t, <-finished)
	require.NoError(t, editErr)
	require.Equal(t, jobdb.MetadataVersion(1), entry.Version)
	_, err = leaseWriter.EditJobMetadata(ctx, write("denied", 0, `true`))
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	_, err = client.EditJobMetadata(ctx, key, write("lease", 2, `false`))
	require.NoError(t, err)
	info, err := client.GetJob(ctx, key)
	require.NoError(t, err)
	require.Equal(t, "failed", info.CompletionStatus)
	require.Equal(t, exact, string(info.ExtendedMetadata["exact"].Value))
	require.Equal(t, jobdb.MetadataVersion(3), info.ExtendedMetadata["lease"].Version)
	require.Equal(t, entry, info.ExtendedMetadata["before-failure"])
	for _, key := range []string{"same", "independent-a", "independent-b"} {
		require.Equal(t, jobdb.JobMetadataEntry{Version: 1, Present: true, Value: json.RawMessage(`42`)}, info.ExtendedMetadata[key])
	}
	require.NotContains(t, info.ExtendedMetadata, "denied")
	page, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}, Stores: []jobdb.JobStore{jobdb.JobStoreArchived}, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, page.Jobs, 1)
	require.Equal(t, info.ExtendedMetadata, page.Jobs[0].ExtendedMetadata)
	require.JSONEq(t, ordinary, string(page.Jobs[0].Metadata))
	_, err = client.EditJobMetadata(ctx, jobdb.JobKey{TenantId: "c2", JobId: "missing"}, write("exact", 0, `true`))
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	dryRun, err := client.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "c2", JobIDs: []string{"job"}, DryRun: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, dryRun.MatchedCount)
	observed, err := client.GetJob(ctx, key)
	require.NoError(t, err)
	require.Equal(t, info.ExtendedMetadata, observed.ExtendedMetadata)
	deleteRace := make(chan error, 1)
	go func() {
		_, err := client.EditJobMetadata(ctx, key, write("deletion-race", 0, `true`))
		deleteRace <- err
	}()
	_, err = client.DeleteJobs(ctx, jobdb.DeleteJobsRequest{TenantID: "c2", JobIDs: []string{"job"}})
	require.NoError(t, err)
	if err := <-deleteRace; err != nil {
		require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	}
	_, err = client.EditJobMetadata(ctx, key, write("exact", 1, `true`))
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
	info, err = store.GetJob(ctx, key)
	require.NoError(t, err)
	require.Empty(t, info.ExtendedMetadata)
	page, err = client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}})
	require.NoError(t, err)
	require.Empty(t, page.Jobs)
}
