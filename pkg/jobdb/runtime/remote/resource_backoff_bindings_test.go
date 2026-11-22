package remote

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

type resourceLostReply struct {
	base   http.RoundTripper
	mu     sync.Mutex
	bodies [][]byte
}

func (r *resourceLostReply) RoundTrip(q *http.Request) (*http.Response, error) {
	if !strings.HasSuffix(q.URL.Path, "/resource-backoff") {
		return r.base.RoundTrip(q)
	}
	raw, err := io.ReadAll(q.Body)
	if err != nil {
		return nil, err
	}
	q.Body = io.NopCloser(bytes.NewReader(raw))
	r.mu.Lock()
	r.bodies = append(r.bodies, raw)
	first := len(r.bodies) == 1
	r.mu.Unlock()
	response, err := r.base.RoundTrip(q)
	if err == nil && first {
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return nil, errors.New("lost post-commit response")
	}
	return response, err
}
func TestResourceBackoffHTTPBindings(t *testing.T) {
	for _, project := range []bool{false, true} {
		t.Run(map[bool]string{false: "tenant", true: "project"}[project], func(t *testing.T) {
			f := newDelegationFixture(t, "sqlite", nil)
			transport := &resourceLostReply{base: f.server.Client().Transport}
			httpClient := &http.Client{Transport: transport}
			var client *Runtime
			var err error
			if project {
				client, err = NewProjectRuntime(f.server.URL, httpClient)
			} else {
				client, err = New(f.server.URL, httpClient)
			}
			require.NoError(t, err)
			lease := secretsRemoteLease(t, client, "bindings")
			req, err := runtimetest.PrepareResourceBackoffCheckpoint(celltest.Context(context.Background()), f.backend, lease, "binding/attempt:1", 1, jobdb.DefaultResourceBackoffPolicy())
			require.NoError(t, err)
			t.Run("B14_C02_lost_reply", func(t *testing.T) {
				_, err := lease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(context.Background(), req)
				require.True(t, jobdb.IsExecutionInfrastructureError(err), "%v", err)
				receipt, err := lease.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(context.Background(), req)
				require.NoError(t, err)
				require.EqualValues(t, 1, receipt.Count)
				transport.mu.Lock()
				defer transport.mu.Unlock()
				require.Len(t, transport.bodies, 2)
				require.Equal(t, transport.bodies[0], transport.bodies[1])
				require.Equal(t, lease.Job().JobKey, receipt.JobKey)
			})
		})
	}
}
func TestResourceBackoffHTTPReopenAndInt64(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := sqlite.Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "http.db")}
	backend, err := sqlite.NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, backend.Close(ctx)) }()
	signingKey := bytes.Repeat([]byte{37}, 32)
	options := ServerOptions{LeaseTokenSigningKey: signingKey, LeaseTokenAudience: "resource-test"}
	handler, err := NewServerWithOptions(backend, options)
	require.NoError(t, err)
	server := httptest.NewServer(celltest.Handler(handler))
	client, err := NewProjectRuntime(server.URL, server.Client())
	require.NoError(t, err)
	lease := secretsRemoteLease(t, client, "int64")
	revision := int64(1<<53) + 7
	db, err := sql.Open("sqlite", cfg.DBPath)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE jobdb_jobs SET client_payload_revision=? WHERE tenant_id=? AND job_id=?`, revision, lease.Job().JobKey.TenantId, lease.Job().JobKey.JobId)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	renewed, err := lease.(jobdb.RenewableExecutionLease).Renew(ctx)
	require.NoError(t, err)
	require.Equal(t, revision, renewed.ClientPayloadRevision())
	policy := jobdb.DefaultResourceBackoffPolicy()
	allowance := int64(math.MaxInt64)
	policy.CrashAllowance = &allowance
	req, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, backend, renewed, "large/attempt:1", 1, policy)
	require.NoError(t, err)
	receipt, err := renewed.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
	require.NoError(t, err)
	t.Run("B15_B15c_int64", func(t *testing.T) {
		require.Equal(t, allowance, receipt.ResolvedCrashAllowance)
		require.Equal(t, revision, receipt.Resume.PayloadRevision)
		info, err := client.GetJob(ctx, lease.Job().JobKey)
		require.NoError(t, err)
		require.Equal(t, allowance, *info.ExecutionState.CrashBudgetRemaining)
		require.Equal(t, revision, info.ExecutionState.ResourceResume.PayloadRevision)
	})
	// Preserve both storage and verification authority across a genuine reopen.
	server.Close()
	require.NoError(t, backend.Close(ctx))
	backend, err = sqlite.NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	handler, err = NewServerWithOptions(backend, options)
	require.NoError(t, err)
	server = httptest.NewServer(celltest.Handler(handler))
	defer server.Close()
	signer, err := newLeaseTokenSignerWithKey(signingKey)
	require.NoError(t, err)
	signer.audience = options.LeaseTokenAudience
	owner := renewed.(*remoteExecutionLease)
	h := owner.Job()
	token, err := signer.mintWithPrincipal(h.JobKey, owner.LeaseID(), owner.workerID, "", time.Now().Add(-time.Second), time.Minute, nil, jobdb.ResolvedCell{Cell: h.Cell, CellID: h.CellID, CellRepo: h.CellRepo})
	require.NoError(t, err)
	raw, _ := json.Marshal(req)
	path := strings.Replace(delegationBase(owner), "/v1/tenants/", "/v1/projects/", 1) + "/resource-backoff"
	t.Run("B16b_storage_and_authority_reopen", func(t *testing.T) {
		status, body := resourceHTTP(t, &delegationFixture{server: server}, path, token, raw)
		require.Equal(t, 200, status, string(body))
		// Project DTOs rewrite the identity envelope only, preserving exact integers.
		wire, err := projectWireJSON(body, "ResourceBackoffReceipt", false)
		require.NoError(t, err)
		var replay jobdb.ResourceBackoffReceipt
		require.NoError(t, json.Unmarshal(wire, &replay))
		require.Equal(t, receipt, replay)
		retired, err := NewServerWithOptions(backend, ServerOptions{LeaseTokenSigningKey: bytes.Repeat([]byte{38}, 32), LeaseTokenAudience: options.LeaseTokenAudience})
		require.NoError(t, err)
		other := httptest.NewServer(celltest.Handler(retired))
		defer other.Close()
		status, body = resourceHTTP(t, &delegationFixture{server: other}, path, token, raw)
		require.NotEqual(t, 200, status, string(body))
		require.NotContains(t, string(body), "wakeAt")
	})
}
func TestResourceBackoffHTTPDelegatedSnapshots(t *testing.T) {
	f := newDelegationFixture(t, "sqlite", nil)
	ctx := celltest.Context(context.Background())
	req := resourceRequest(t, f)
	req.Policy.Initial = 20 * time.Millisecond
	req.Policy.Maximum = 20 * time.Millisecond
	req.Policy.Jitter = 0
	// Policy is authenticated by the prepared checkpoint, so prepare a matching one.
	req, err := runtimetest.PrepareResourceBackoffCheckpoint(ctx, f.backend, f.owner, req.Invocation, 1, req.Policy)
	require.NoError(t, err)
	_, err = f.owner.(jobdb.ResourceBackoffLease).RescheduleResourceBackoff(ctx, req)
	require.NoError(t, err)
	time.Sleep(25 * time.Millisecond)
	owner, err := f.client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: f.owner.Job().JobKey, WorkerID: "snapshot", Routes: []jobdb.Route{f.owner.Route()}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, owner)
	f.owner = owner
	t.Run("B15d_supplied_renewed_delegated", func(t *testing.T) {
		capability, err := ExportLease(owner)
		require.NoError(t, err)
		supplied, err := f.client.ImportLease(ctx, capability)
		require.NoError(t, err)
		require.Equal(t, owner.ExecutionState(), supplied.ExecutionState())
		narrow := f.create(t, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{ResourceBackoff: true}})
		require.NotNil(t, narrow.ExecutionState().ResourceBackoff)
		require.NotNil(t, narrow.ExecutionState().CrashBudgetRemaining)
		require.Nil(t, narrow.ExecutionState().ResourceResume)
		require.Nil(t, narrow.ExecutionState().TaskWait)
		full := f.create(t, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{ResourceBackoff: true, Read: &jobdb.DelegationChapters{Any: true}}})
		require.Equal(t, owner.ExecutionState().ResourceResume, full.ExecutionState().ResourceResume)
		next, err := full.Renew(ctx)
		require.NoError(t, err)
		require.Equal(t, full.ExecutionState(), next.ExecutionState())
		_, err = f.client.CreateDelegation(ctx, narrow, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{ResourceBackoff: true, Read: &jobdb.DelegationChapters{Any: true}}})
		require.Error(t, err)
	})
}
