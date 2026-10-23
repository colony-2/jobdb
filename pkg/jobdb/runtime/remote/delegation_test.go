package remote

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type delegationFixture struct {
	client  *Runtime
	backend jobdb.WorkflowRuntime
	server  *httptest.Server
	signer  *leaseTokenSigner
	owner   jobdb.ExecutionLease
	renews  atomic.Int32
}

func newDelegationFixture(t *testing.T, kind string, provider jobdb.SecretProvider) *delegationFixture {
	t.Helper()
	f := &delegationFixture{backend: providerRuntime(t, kind, provider), signer: newLeaseTokenSigner()}
	handler, err := newServer(f.backend, f.signer)
	require.NoError(t, err)
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/keepalive") && isDelegationToken(r.Header.Get("X-JobDB-Lease-Token")) {
			f.renews.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(f.server.Close)
	f.client, err = New(f.server.URL, f.server.Client())
	require.NoError(t, err)
	f.owner = secretsRemoteLease(t, f.client, "tenant")
	return f
}
func delegationPolicy() jobdb.DelegationPermissions {
	return jobdb.DelegationPermissions{
		Submit:  &jobdb.DelegationSubmitPermission{JobTypes: jobdb.DelegationNames{Names: []string{"child"}}, JobIDs: jobdb.DelegationNames{Any: true}},
		Read:    &jobdb.DelegationChapters{Ordinals: []int64{0, 1}},
		Write:   &jobdb.DelegationWritePermission{Ordinals: []int64{1}, TaskTypes: jobdb.DelegationNames{Names: []string{"task"}}, ArtifactNames: jobdb.DelegationNames{Names: []string{"result"}}},
		Secrets: []string{"allowed", "unknown"},
	}
}
func (f *delegationFixture) create(t *testing.T, req jobdb.CreateDelegationRequest) *remoteExecutionLease {
	t.Helper()
	l, err := f.client.CreateDelegation(context.Background(), f.owner, req)
	require.NoError(t, err)
	return l.(*remoteExecutionLease)
}
func (f *delegationFixture) validity(t *testing.T) jobdb.LeaseValidity {
	t.Helper()
	v, err := f.backend.(interface {
		ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
	}).ValidateExecutionLeaseByID(context.Background(), f.owner.Job().JobKey, f.owner.LeaseID(), f.owner.(*remoteExecutionLease).workerID)
	require.NoError(t, err)
	return v
}
func (f *delegationFixture) staleSnapshot(t *testing.T, l *remoteExecutionLease) {
	t.Helper()
	l.mu.Lock()
	l.expiresAt = time.Now().Add(-time.Minute)
	l.mu.Unlock()
}
func delegationBase(l jobdb.ExecutionLease) string {
	return fmt.Sprintf("/v1/tenants/%s/jobs/%s/leases/%s", l.Job().JobKey.TenantId, l.Job().JobKey.JobId, l.LeaseID())
}
func delegationHTTP(t *testing.T, f *delegationFixture, method, path, token string, body any) (int, []byte) {
	t.Helper()
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		require.NoError(t, err)
	}
	req, err := http.NewRequest(method, f.server.URL+path, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("X-JobDB-Lease-Token", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, raw
}
func delegationChapter(ordinal int64) jobdb.Chapter {
	return jobdb.Chapter{Ordinal: ordinal, TaskType: "task", CreatedAt: time.Now().UTC(), Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)}}}}
}
func TestDelegationLeaseCompatibilityAndPermissions(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			var providerCalls atomic.Int32
			f := newDelegationFixture(t, kind, testSecretProvider(func(_ context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
				providerCalls.Add(1)
				require.Equal(t, []string{"allowed", "unknown"}, req.Names)
				return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"allowed": {Value: "value"}}}, nil
			}))
			before := f.validity(t)
			l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
			require.Equal(t, f.owner.LeaseID(), l.LeaseID())
			require.Equal(t, f.owner.Route(), l.Route())
			require.Empty(t, l.ClientPayload())
			require.Equal(t, jobdb.ExecutionState{}, l.ExecutionState())
			cap, err := ExportLease(l)
			require.NoError(t, err)
			raw, err := cap.Encode()
			require.NoError(t, err)
			importedCap, err := DecodeLeaseCapability(raw)
			require.NoError(t, err)
			isolated, err := New(f.server.URL, f.server.Client())
			require.NoError(t, err)
			imported, err := isolated.ImportLease(ctx, importedCap)
			require.NoError(t, err)
			require.Equal(t, int32(0), f.renews.Load(), "import must not require renewal")
			_, err = imported.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "child", Data: jobdb.NewTaskDataOrPanic(42)}})
			require.NoError(t, err)
			_, err = imported.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "other", Data: jobdb.NewTaskDataOrPanic(42)}})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			_, err = imported.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "other", JobID: "specific", Data: jobdb.NewTaskDataOrPanic(42)}})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			secrets, err := imported.ResolveSecrets(ctx, []string{"allowed", "denied", "unknown"})
			require.NoError(t, err)
			require.Len(t, secrets.Secrets, 1)
			secrets, err = imported.ResolveSecrets(ctx, []string{"denied"})
			require.NoError(t, err)
			require.Empty(t, secrets.Secrets)
			require.Equal(t, int32(1), providerCalls.Load())
			_, err = isolated.GetChapter(ctx, jobdb.ChapterRef{JobKey: l.Job().JobKey, LeaseID: l.LeaseID(), LeaseToken: l.LeaseToken(), Ordinal: 1})
			require.ErrorIs(t, err, jobdb.ErrChapterNotFound)
			put := jobdb.PutChapterRequest{LeaseID: l.LeaseID(), LeaseToken: l.LeaseToken(), Ref: jobdb.ChapterRef{JobKey: l.Job().JobKey, Ordinal: 1}, Chapter: delegationChapter(1)}
			require.NoError(t, isolated.PutChapter(ctx, put))
			ref := jobdb.ChapterRef{JobKey: l.Job().JobKey, Ordinal: 1, LeaseID: l.LeaseID(), LeaseToken: l.LeaseToken()}
			_, err = isolated.GetChapter(ctx, ref)
			require.NoError(t, err)
			ref.Ordinal = 99
			_, err = isolated.GetChapter(ctx, ref)
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			chapters, err := isolated.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: l.Job().JobKey, LeaseID: l.LeaseID(), LeaseToken: l.LeaseToken()})
			require.NoError(t, err)
			require.Len(t, chapters, 2)
			for _, mutation := range []func(*jobdb.PutChapterRequest){
				func(p *jobdb.PutChapterRequest) { p.Chapter.Ordinal = 2; p.Ref.Ordinal = 2 },
				func(p *jobdb.PutChapterRequest) { p.Chapter.TaskType = "other" },
				func(p *jobdb.PutChapterRequest) { p.Chapter.Body = jobdb.ExecutionChapter{Record: []byte(`{}`)} },
				func(p *jobdb.PutChapterRequest) {
					p.Chapter.Body = jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}}
				},
				func(p *jobdb.PutChapterRequest) {
					p.Chapter.Artifacts = []jobdb.StoredArtifact{{Name: "result", Digest: "some-existing-digest", Size: 1}}
				},
			} {
				copy := put
				mutation(&copy)
				require.ErrorIs(t, isolated.PutChapter(ctx, copy), jobdb.ErrLeasePermissionDenied)
			}
			// HTTP denial matrix also exercises routes whose client methods reject locally.
			for _, test := range []struct{ method, path string }{
				{"POST", delegationBase(l) + "/complete"}, {"POST", delegationBase(l) + "/reschedule"},
				{"POST", delegationBase(l) + "/delegations"}, {"POST", delegationBase(l) + "/jobs/restarts"},
				{"POST", delegationBase(l) + "/jobs/id/restart"}, {"GET", delegationBase(l) + "/configuration"},
				{"POST", "/v1/jobs/poll"}, {"POST", "/v1/tenants/tenant/jobs"},
				{"GET", "/v1/tenants/tenant/jobs/" + l.Job().JobKey.JobId + "/chapters?startOrdinal=0"},
				{"POST", "/dev/v1/tenants/tenant/secrets/set"},
			} {
				status, raw := delegationHTTP(t, f, test.method, test.path, l.LeaseToken(), map[string]any{})
				require.Equal(t, 403, status, "%s: %s", test.path, raw)
			}
			require.Equal(t, before, f.validity(t), "child operations must not extend the parent")
			require.NoError(t, l.KeepAlive(ctx))
			require.Equal(t, before, f.validity(t))
			next, err := l.Renew(ctx)
			require.NoError(t, err)
			require.Equal(t, l.Delegation(), next.(*remoteExecutionLease).Delegation())
			require.NoError(t, f.backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: l.Job().JobKey}))
			require.ErrorIs(t, isolated.PutChapter(ctx, put), jobdb.ErrExecutionLeaseLost)
			require.ErrorIs(t, l.KeepAlive(ctx), jobdb.ErrExecutionLeaseLost)
			ref.Ordinal = 0
			_, err = isolated.GetChapter(ctx, ref)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
		})
	}
}

func TestDelegationUsableUntilDeadline(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deadline=%v", kind, deadline), func(t *testing.T) {
				f := newDelegationFixture(t, kind, nil)
				req := jobdb.CreateDelegationRequest{Permissions: delegationPolicy()}
				if deadline {
					d := time.Now().UTC().Add(time.Hour)
					req.NotAfter = &d
				}
				l := f.create(t, req)
				before := f.validity(t)
				cap, err := ExportLease(l)
				require.NoError(t, err)
				_, err = f.client.ImportLease(context.Background(), cap)
				require.NoError(t, err)
				require.Equal(t, int32(0), f.renews.Load(), "valid import needs no refresh")
				require.NoError(t, l.KeepAlive(context.Background()))
				f.staleSnapshot(t, l)
				_, err = l.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
				require.NoError(t, err)
				require.Equal(t, int32(1), f.renews.Load(), "only explicit keepalive; operations need no refresh")
				require.Equal(t, before, f.validity(t))
				require.Equal(t, req.NotAfter, l.Delegation().NotAfter)
			})
		}
	}
}

func TestDelegationConcurrentSubmissionNeedsNoRefresh(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	f.staleSnapshot(t, l)
	before := f.validity(t)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := l.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "child", JobID: fmt.Sprint("child-", i), Data: jobdb.NewTaskDataOrPanic(i)}})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(0), f.renews.Load())
	require.Equal(t, before, f.validity(t))
	require.True(t, l.LeaseExpiry().Before(time.Now()), "stale snapshots do not block operations")
}

func TestDelegationSignedClaimsCannotBeChanged(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	parts := strings.Split(l.LeaseToken(), ".")
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	raw = bytes.Replace(raw, []byte(`"ordinals":[0,1]`), []byte(`"ordinals":[0,2]`), 1)
	parts[1] = base64.RawURLEncoding.EncodeToString(raw)
	status, _ := delegationHTTP(t, f, "GET", delegationBase(l)+"/chapters/0", strings.Join(parts, "."), nil)
	require.Equal(t, 401, status)
	_, err = f.signer.parse(l.LeaseToken())
	require.Error(t, err, "owner parser must never accept a delegation")
	for _, path := range []string{strings.Replace(delegationBase(l), "/tenant/", "/other/", 1), strings.Replace(delegationBase(l), l.LeaseID(), "other", 1)} {
		status, _ := delegationHTTP(t, f, "GET", path, l.LeaseToken(), nil)
		require.Equal(t, 401, status)
	}
	cap, err := ExportLease(l)
	require.NoError(t, err)
	for _, value := range []any{cap, &cap, l, l.Delegation()} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
			require.NotContains(t, fmt.Sprintf(format, value), l.LeaseToken())
		}
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		require.NotContains(t, string(raw), l.LeaseToken())
	}
}

func TestDelegationIsolatedProcess(t *testing.T) {
	if os.Getenv("JOBDB_DELEGATION_CHILD") == "1" {
		var handoff struct {
			URL        string
			Capability json.RawMessage
		}
		require.NoError(t, json.NewDecoder(os.Stdin).Decode(&handoff))
		c, err := New(handoff.URL, nil)
		require.NoError(t, err)
		cap, err := DecodeLeaseCapability(handoff.Capability)
		require.NoError(t, err)
		lease, err := c.ImportLease(context.Background(), cap)
		require.NoError(t, err)
		ctx := context.Background()
		token := lease.(interface{ LeaseToken() string }).LeaseToken()
		_, err = c.GetChapter(ctx, jobdb.ChapterRef{JobKey: lease.Job().JobKey, LeaseID: lease.LeaseID(), LeaseToken: token, Ordinal: 0})
		require.NoError(t, err)
		values, err := lease.ResolveSecrets(ctx, []string{"allowed"})
		require.NoError(t, err)
		require.Equal(t, "isolated-secret", values.Secrets["allowed"].Value)
		require.NoError(t, c.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: token, Ref: jobdb.ChapterRef{JobKey: lease.Job().JobKey, Ordinal: 1}, Chapter: delegationChapter(1)}))
		narrow, err := c.CreateDelegation(ctx, lease, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Submit: delegationPolicy().Submit}})
		require.NoError(t, err)
		_, err = narrow.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "child", Data: jobdb.NewTaskDataOrPanic("isolated")}})
		require.NoError(t, err)
		require.NoError(t, narrow.KeepAlive(ctx))
		return
	}
	f := newDelegationFixture(t, "sqlite", testSecretProvider(func(context.Context, jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		return jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"allowed": {Value: "isolated-secret"}}}, nil
	}))
	handler, err := NewDelegationServerWithOptions(f.backend, ServerOptions{LeaseTokenSigningKey: f.signer.key})
	require.NoError(t, err)
	ingress := httptest.NewServer(handler)
	defer ingress.Close()
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	f.staleSnapshot(t, l)
	before := f.validity(t)
	cap, err := ExportLease(l)
	require.NoError(t, err)
	raw, err := cap.Encode()
	require.NoError(t, err)
	handoff, err := json.Marshal(struct {
		URL        string
		Capability json.RawMessage
	}{ingress.URL, raw})
	require.NoError(t, err)
	binary, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestDelegationIsolatedProcess$")
	cmd.Env = []string{"JOBDB_DELEGATION_CHILD=1"}
	cmd.Dir = t.TempDir()
	cmd.Stdin = bytes.NewReader(handoff)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Equal(t, int32(0), f.renews.Load())
	require.Equal(t, before, f.validity(t))
}

func TestDelegationDiesWithExpiredOrReplacedParent(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			f := newDelegationFixture(t, kind, nil)
			l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
			require.NoError(t, f.backend.(interface {
				KeepAliveLeaseByID(context.Context, jobdb.JobKey, string, string, time.Duration) error
			}).KeepAliveLeaseByID(ctx, l.Job().JobKey, l.LeaseID(), l.workerID, time.Nanosecond))
			require.ErrorIs(t, l.KeepAlive(ctx), jobdb.ErrExecutionLeaseLost)
			_, err := f.client.GetChapter(ctx, jobdb.ChapterRef{JobKey: l.Job().JobKey, Ordinal: 0, LeaseID: l.LeaseID(), LeaseToken: l.LeaseToken()})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			owner, err := f.client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: l.Job().JobKey, WorkerID: "next", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, owner)
			require.NotEqual(t, l.LeaseID(), owner.LeaseID())
			require.ErrorIs(t, l.KeepAlive(ctx), jobdb.ErrExecutionLeaseLost)
		})
	}
}

func TestDelegationReadFiltersAndExactSubmissionIDs(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	ctx := context.Background()
	for _, ordinal := range []int64{1, 2, 3} {
		c := delegationChapter(ordinal)
		require.NoError(t, f.client.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: f.owner.LeaseID(), LeaseToken: f.owner.(*remoteExecutionLease).LeaseToken(), Ref: jobdb.ChapterRef{JobKey: f.owner.Job().JobKey, Ordinal: ordinal}, Chapter: c}))
	}
	p := delegationPolicy()
	p.Read = &jobdb.DelegationChapters{Ranges: []jobdb.DelegationOrdinalRange{{Start: 1, End: 2}}}
	p.Submit.JobIDs = jobdb.DelegationNames{Names: []string{"exact"}}
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: p})
	chapters, err := f.client.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: l.Job().JobKey, LeaseID: l.LeaseID(), LeaseToken: l.LeaseToken()})
	require.NoError(t, err)
	require.Len(t, chapters, 2)
	require.Equal(t, int64(1), chapters[0].Ordinal)
	require.Equal(t, int64(2), chapters[1].Ordinal)
	for _, id := range []string{"", "wrong", "exact"} {
		_, err := l.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobID: id, JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
		if id == "exact" {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
		}
	}
	empty := f.create(t, jobdb.CreateDelegationRequest{})
	_, err = empty.ResolveSecrets(ctx, []string{"anything"})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	_, err = f.client.GetChapter(ctx, jobdb.ChapterRef{JobKey: l.Job().JobKey, LeaseID: l.LeaseID(), LeaseToken: empty.LeaseToken(), Ordinal: 0})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
}

func TestDelegationFreshArtifactsAndNoSchemaRegistration(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	ctx := context.Background()
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	upload := jobdb.ArtifactUpload{Name: "result", Size: 2, Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("ok")), nil }}
	req := jobdb.PutChapterRequest{LeaseID: l.LeaseID(), LeaseToken: l.LeaseToken(), Ref: jobdb.ChapterRef{JobKey: l.Job().JobKey, Ordinal: 1}, Chapter: delegationChapter(1), ArtifactUploads: []jobdb.ArtifactUpload{upload}}
	req.ArtifactUploads[0].Name = "denied"
	require.ErrorIs(t, f.client.PutChapter(ctx, req), jobdb.ErrLeasePermissionDenied)
	req.ArtifactUploads[0].Name = "result"
	require.NoError(t, f.client.PutChapter(ctx, req))
	_, err := l.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "child", Data: jobdb.NewTaskDataOrPanic(1), Schema: &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{}`)}}})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
}

func TestDelegationMalformedPolicyAndHardDeadline(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	for _, body := range []string{
		`{"permissions":{"unknown.action":{}}}`,
		`{"permissions":{"chapters.read":{"any":true,"ordinals":[0]}}}`,
		`{"permissions":{"chapters.write":{"ordinals":[-1]}}}`,
		`{"permissions":{"jobs.submit":{"jobTypes":{"any":true,"names":["x"]},"jobIds":{"any":true}}}}`,
		`{"permissions":{"chapters.read":{"ranges":[{"start":2,"end":1}]}}}`,
		`{"ttlSeconds":30,"permissions":{}}`,
		`{"permissions":{"delegation.renew":{"maxTtlSeconds":30}}}`,
		`{"notAfter":"2000-01-01T00:00:00Z","permissions":{}}`,
	} {
		req, err := http.NewRequest("POST", f.server.URL+delegationBase(f.owner)+"/delegations", strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-JobDB-Lease-Token", f.owner.(*remoteExecutionLease).LeaseToken())
		resp, err := f.server.Client().Do(req)
		require.NoError(t, err)
		require.Equal(t, 400, resp.StatusCode, body)
		resp.Body.Close()
	}
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	past := time.Now().UTC().Add(-time.Second)
	token := appendTestRestriction(t, l.LeaseToken(), delegationPolicy(), &past)
	for _, suffix := range []string{"/keepalive", "/secrets/resolve"} {
		status, raw := delegationHTTP(t, f, "POST", delegationBase(l)+suffix, token, map[string]any{"names": []string{"allowed"}})
		require.Equal(t, 403, status)
		require.Contains(t, string(raw), "delegation_deadline_reached")
	}
}

func TestDelegationTransportDoesNotReplayOtherFailuresOrFollowRedirects(t *testing.T) {
	for _, status := range []int{401, 403, 409, 500, 503, 307} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			var destinationCalls atomic.Int32
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destinationCalls.Add(1) }))
			defer destination.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", destination.URL)
				w.WriteHeader(status)
				fmt.Fprint(w, `{"code":"some_other_failure"}`)
			}))
			defer server.Close()
			client, err := New(server.URL, server.Client())
			require.NoError(t, err)
			req, err := http.NewRequest("POST", server.URL+"/v1/tenants/t/jobs/j/leases/l/jobs", strings.NewReader(`{"job":{}}`))
			require.NoError(t, err)
			req.Header.Set("X-JobDB-Lease-Token", delegationTokenPrefix+"opaque.signature")
			resp, err := client.raw.Client.Do(req)
			require.NoError(t, err)
			resp.Body.Close()
			require.Equal(t, int32(1), calls.Load())
			require.Equal(t, int32(0), destinationCalls.Load())
		})
	}
}

func TestDelegationDeadlineDuringOperationIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	f := newDelegationFixture(t, "toy", testSecretProvider(func(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
		calls.Add(1)
		<-ctx.Done()
		return jobdb.SecretResolveResult{}, ctx.Err()
	}))
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	deadline := time.Now().UTC().Add(100 * time.Millisecond)
	l.leaseToken = appendTestRestriction(t, l.LeaseToken(), delegationPolicy(), &deadline)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := l.ResolveSecrets(ctx, []string{"allowed"})
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, int32(0), f.renews.Load())
}

func TestDelegationKeepAlivePreservesGrant(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	original := l.LeaseToken()
	before := f.validity(t)
	for i := 0; i < 2; i++ {
		f.staleSnapshot(t, l)
		require.NoError(t, l.KeepAlive(context.Background()))
		require.Equal(t, original, l.LeaseToken())
		require.Equal(t, before.ExpiresAt, l.LeaseExpiry())
	}
	require.Equal(t, before, f.validity(t))
}

func TestDelegationWorksAcrossReplicasWithSharedKey(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	f.staleSnapshot(t, l)
	for _, sameKey := range []bool{true, false} {
		key := f.signer.key
		if !sameKey {
			key = bytes.Repeat([]byte{0}, 32)
		}
		handler, err := NewServerWithOptions(f.backend, ServerOptions{LeaseTokenSigningKey: key})
		require.NoError(t, err)
		replica := httptest.NewServer(handler)
		t.Cleanup(replica.Close)
		client, err := New(replica.URL, replica.Client())
		require.NoError(t, err)
		cap, err := ExportLease(l)
		require.NoError(t, err)
		_, err = client.ImportLease(context.Background(), cap)
		if sameKey {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func TestDelegationOnlyIngressRejectsMissingAndOwnerCredentials(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	l := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	f.staleSnapshot(t, l)
	handler, err := NewDelegationServerWithOptions(f.backend, ServerOptions{LeaseTokenSigningKey: f.signer.key})
	require.NoError(t, err)
	ingress := httptest.NewServer(handler)
	defer ingress.Close()
	for _, token := range []string{"", f.owner.(*remoteExecutionLease).LeaseToken(), "invalid"} {
		for _, path := range []string{"/v1/tenants/tenant/jobs", delegationBase(l), "/dev/v1/tenants/tenant/secrets/set"} {
			req, err := http.NewRequest("GET", ingress.URL+path, nil)
			require.NoError(t, err)
			req.Header.Set("X-JobDB-Lease-Token", token)
			resp, err := ingress.Client().Do(req)
			require.NoError(t, err)
			resp.Body.Close()
			require.Equal(t, 401, resp.StatusCode)
		}
	}
	client, err := New(ingress.URL, ingress.Client())
	require.NoError(t, err)
	cap, err := ExportLease(l)
	require.NoError(t, err)
	imported, err := client.ImportLease(context.Background(), cap)
	require.NoError(t, err)
	_, err = imported.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
}

func TestDelegationRenewalIsCappedAtHardDeadline(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			f := newDelegationFixture(t, kind, nil)
			deadline := time.Now().UTC().Add(10 * time.Second)
			child := f.create(t, jobdb.CreateDelegationRequest{NotAfter: &deadline, Permissions: delegationPolicy()})
			before := f.validity(t)
			require.Equal(t, deadline, child.LeaseExpiry())
			f.staleSnapshot(t, child)
			next, err := child.Renew(context.Background())
			require.NoError(t, err)
			require.Equal(t, deadline, next.LeaseExpiry())
			require.Equal(t, &deadline, next.(*remoteExecutionLease).Delegation().NotAfter)
			require.Equal(t, before, f.validity(t), "renewal must not extend the parent")
		})
	}
}
