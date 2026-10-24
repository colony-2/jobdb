package remote

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"gopkg.in/macaroon.v2"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

// Models a malicious holder: add a correctly chained but potentially invalid
// caveat without using the SDK's subset/deadline validation.
func appendTestRestriction(t *testing.T, token string, p jobdb.DelegationPermissions, d *time.Time) string {
	t.Helper()
	m, _, _, err := decodeGrant(token, true)
	require.NoError(t, err)
	raw, err := json.Marshal(grantStep{"restrict", 1, "test-step", p, d})
	require.NoError(t, err)
	require.NoError(t, m.AddFirstPartyCaveat(raw))
	next, err := encodeGrant(m)
	require.NoError(t, err)
	return next
}

func TestAttenuationLocalDerivationAndHandoff(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			f := newDelegationFixture(t, kind, nil)
			var requests atomic.Int32
			offline, err := New("http://offline.invalid", &http.Client{Transport: diagnosticTransport(func(*http.Request) (*http.Response, error) {
				requests.Add(1)
				return nil, errors.New("network disabled")
			})})
			require.NoError(t, err)
			deadline := time.Now().UTC().Add(time.Minute)
			policy := delegationPolicy()
			child, err := offline.CreateDelegation(context.Background(), f.owner, jobdb.CreateDelegationRequest{Permissions: policy, NotAfter: &deadline})
			require.NoError(t, err)
			policy.Read.Ordinals[0] = 999 // caller mutation must not alter returned metadata
			require.Equal(t, []int64{0, 1}, child.(*remoteExecutionLease).Delegation().Permissions.Read.Ordinals)
			narrow := jobdb.DelegationPermissions{Read: &jobdb.DelegationChapters{Ordinals: []int64{0}}, Secrets: []string{"allowed"}}
			grandchild, err := offline.CreateDelegation(context.Background(), child, jobdb.CreateDelegationRequest{Permissions: narrow})
			require.NoError(t, err)
			require.Equal(t, int32(0), requests.Load())
			require.Equal(t, &deadline, grandchild.(*remoteExecutionLease).Delegation().NotAfter)
			require.Empty(t, grandchild.ClientPayload())
			require.Equal(t, jobdb.ExecutionState{}, grandchild.ExecutionState())
			cap, err := ExportLease(grandchild)
			require.NoError(t, err)
			raw, err := cap.Encode()
			require.NoError(t, err)
			require.NotContains(t, string(raw), f.owner.(*remoteExecutionLease).LeaseToken())
			require.NotContains(t, string(raw), child.(*remoteExecutionLease).LeaseToken())
			owner, err := decodeOwner(f.owner.(*remoteExecutionLease).LeaseToken())
			require.NoError(t, err)
			root, _, _, err := decodeGrant(owner.Grant, true)
			require.NoError(t, err)
			childMac, _, _, err := decodeGrant(child.(*remoteExecutionLease).LeaseToken(), false)
			require.NoError(t, err)
			grandMac, _, _, err := decodeGrant(grandchild.(*remoteExecutionLease).LeaseToken(), false)
			require.NoError(t, err)
			binary, err := grandMac.MarshalBinary()
			require.NoError(t, err)
			require.NotContains(t, binary, root.Signature())
			require.NotContains(t, binary, childMac.Signature())
			before := f.validity(t)
			imported, err := f.client.ImportLease(context.Background(), cap)
			require.NoError(t, err)
			_, err = f.client.GetChapter(context.Background(), jobdb.ChapterRef{JobKey: imported.Job().JobKey, LeaseID: imported.LeaseID(), LeaseToken: cap.wire.Token, Ordinal: 0})
			require.NoError(t, err)
			_, err = imported.SubmitJob(context.Background(), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{JobType: "child", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			require.NoError(t, imported.KeepAlive(context.Background()))
			require.Equal(t, before, f.validity(t))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = offline.CreateDelegation(ctx, child, jobdb.CreateDelegationRequest{})
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, int32(0), requests.Load())
		})
	}
}

func TestAttenuationRejectsWideningLocallyAndOnServer(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	deadline := time.Now().UTC().Add(time.Minute)
	base := delegationPolicy()
	base.Submit.JobIDs = jobdb.DelegationNames{Names: []string{"one"}}
	parent := f.create(t, jobdb.CreateDelegationRequest{Permissions: base, NotAfter: &deadline})
	for _, tc := range []struct {
		name   string
		mutate func(*jobdb.CreateDelegationRequest)
	}{
		{"secret", func(r *jobdb.CreateDelegationRequest) { r.Permissions.Secrets = []string{"denied"} }},
		{"job type", func(r *jobdb.CreateDelegationRequest) {
			r.Permissions.Submit.JobTypes.Any = true
			r.Permissions.Submit.JobTypes.Names = nil
		}},
		{"job id", func(r *jobdb.CreateDelegationRequest) { r.Permissions.Submit.JobIDs = jobdb.DelegationNames{Any: true} }},
		{"read ordinal", func(r *jobdb.CreateDelegationRequest) { r.Permissions.Read.Ordinals = []int64{2} }},
		{"read any", func(r *jobdb.CreateDelegationRequest) { r.Permissions.Read = &jobdb.DelegationChapters{Any: true} }},
		{"write ordinal", func(r *jobdb.CreateDelegationRequest) { r.Permissions.Write.Ordinals = []int64{2} }},
		{"task", func(r *jobdb.CreateDelegationRequest) { r.Permissions.Write.TaskTypes.Names = []string{"other"} }},
		{"artifact", func(r *jobdb.CreateDelegationRequest) {
			r.Permissions.Write.ArtifactNames = jobdb.DelegationNames{Any: true}
		}},
		{"deadline", func(r *jobdb.CreateDelegationRequest) { d := deadline.Add(time.Second); r.NotAfter = &d }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := jobdb.CreateDelegationRequest{Permissions: parent.Delegation().Permissions}
			tc.mutate(&req)
			_, err := f.client.CreateDelegation(context.Background(), parent, req)
			require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
			forged := appendTestRestriction(t, parent.LeaseToken(), req.Permissions, req.NotAfter)
			_, err = f.signer.parseDelegation(forged)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			status, _ := delegationHTTP(t, f, "GET", delegationBase(parent), forged, nil)
			require.Equal(t, 401, status)
		})
	}
	// The SDK must not restore omitted actions, even through empty selectors.
	empty := f.create(t, jobdb.CreateDelegationRequest{})
	_, err := f.client.CreateDelegation(context.Background(), empty, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	require.ErrorIs(t, err, jobdb.ErrLeasePermissionDenied)
	require.NoError(t, empty.KeepAlive(context.Background()))
}

func TestAttenuationDeadlineInheritanceAndParentRotation(t *testing.T) {
	f := newDelegationFixture(t, "sqlite", nil)
	owner := f.owner.(*remoteExecutionLease)
	// An old owner access credential cannot renew the parent, but its root grant
	// remains usable while the backend lease lives (including for local issuance).
	expired, err := f.signer.mintWithPrincipal(owner.jobKey, owner.leaseID, owner.workerID, owner.schemaHash, time.Now().Add(-time.Second), time.Minute, nil)
	require.NoError(t, err)
	old := &remoteExecutionLease{goalID: owner.goalID, runtime: f.client, jobKey: owner.jobKey, leaseID: owner.leaseID, workerID: owner.workerID, leaseToken: expired, expiresAt: time.Now().Add(-time.Minute), route: owner.route}
	require.ErrorIs(t, old.KeepAlive(context.Background()), jobdb.ErrExecutionLeaseLost)
	deadline := time.Now().UTC().Add(10 * time.Second)
	child, err := f.client.CreateDelegation(context.Background(), old, jobdb.CreateDelegationRequest{Permissions: delegationPolicy(), NotAfter: &deadline})
	require.NoError(t, err)
	require.True(t, child.LeaseExpiry().Before(time.Now()), "local snapshot may be stale")
	cap, err := ExportLease(child)
	require.NoError(t, err)
	require.NoError(t, owner.KeepAlive(context.Background()))
	imported, err := f.client.ImportLease(context.Background(), cap)
	require.NoError(t, err)
	require.Equal(t, deadline, imported.LeaseExpiry())
	earlier := deadline.Add(-time.Second)
	grandchild, err := f.client.CreateDelegation(context.Background(), imported, jobdb.CreateDelegationRequest{Permissions: delegationPolicy(), NotAfter: &earlier})
	require.NoError(t, err)
	_, _, claims, err := decodeGrant(grandchild.(*remoteExecutionLease).LeaseToken(), false)
	require.NoError(t, err)
	require.Equal(t, &earlier, claims.Grant.NotAfter)
	// A holder can append a deadline in the past, but cannot use that grant.
	past := time.Now().UTC()
	dead := appendTestRestriction(t, cap.wire.Token, delegationPolicy(), &past)
	for _, op := range []struct{ method, suffix string }{{"GET", ""}, {"POST", "/keepalive"}, {"POST", "/secrets/resolve"}} {
		status, raw := delegationHTTP(t, f, op.method, delegationBase(child)+op.suffix, dead, map[string]any{"names": []string{"allowed"}})
		require.Equal(t, 403, status)
		require.Contains(t, string(raw), "delegation_deadline_reached")
	}
	require.NoError(t, f.backend.CancelJob(context.Background(), jobdb.CancelJobRequest{JobKey: owner.jobKey}))
	require.ErrorIs(t, grandchild.KeepAlive(context.Background()), jobdb.ErrExecutionLeaseLost)
}

func TestAttenuationIntervalSubsets(t *testing.T) {
	parent := &jobdb.DelegationChapters{Ordinals: []int64{2, 3}, Ranges: []jobdb.DelegationOrdinalRange{{Start: 0, End: 1}, {Start: 4, End: math.MaxInt64}}}
	require.True(t, chaptersSubset(&jobdb.DelegationChapters{Ranges: []jobdb.DelegationOrdinalRange{{Start: 0, End: math.MaxInt64}}}, parent))
	parent.Ordinals = []int64{2}
	require.False(t, chaptersSubset(&jobdb.DelegationChapters{Ranges: []jobdb.DelegationOrdinalRange{{Start: 0, End: math.MaxInt64}}}, parent))
	require.True(t, chaptersSubset(&jobdb.DelegationChapters{Ordinals: []int64{0, 2, math.MaxInt64}}, parent))
}

func TestAttenuationRejectsMalformedAndSplicedProofs(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	child := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	m, root, _, err := decodeGrant(child.LeaseToken(), false)
	require.NoError(t, err)
	for _, raw := range [][]byte{
		[]byte(`{"kind":"restrict","kind":"restrict","version":1,"id":"x","permissions":{}}`),
		[]byte(`{"kind":"restrict","version":2,"id":"x","permissions":{}}`),
		[]byte(`{"kind":"restrict","version":1,"id":"x","permissions":{"unknown":{}}}`),
		[]byte(`{"kind":"restrict","version":1,"id":"x","permissions":{},"extra":true}`),
		[]byte(` {"kind":"restrict","version":1,"id":"x","permissions":{}}`),
	} {
		bad := m.Clone()
		require.NoError(t, bad.AddFirstPartyCaveat(raw))
		token, err := encodeGrant(bad)
		require.NoError(t, err)
		_, err = f.signer.parseDelegation(token)
		require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
	}
	third := m.Clone()
	require.NoError(t, third.AddThirdPartyCaveat([]byte("third-party-key"), []byte("id"), "https://not-followed.invalid"))
	token, err := encodeGrant(third)
	require.NoError(t, err)
	_, err = f.signer.parseDelegation(token)
	require.Error(t, err)
	// Rebuild a chain with a removed caveat, a different identity or a sibling's
	// caveat while retaining the original signature. All must fail authentication.
	for _, mutate := range []func(map[string]json.RawMessage){
		func(w map[string]json.RawMessage) { w["c"] = json.RawMessage(`[]`) },
		func(w map[string]json.RawMessage) { w["i"] = json.RawMessage(`"changed root"`) },
	} {
		raw, err := m.MarshalJSON()
		require.NoError(t, err)
		var wire map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &wire))
		mutate(wire)
		raw, err = json.Marshal(wire)
		require.NoError(t, err)
		var bad macaroon.Macaroon
		if bad.UnmarshalJSON(raw) == nil {
			token, err := encodeGrant(&bad)
			require.NoError(t, err)
			_, err = f.signer.parseDelegation(token)
			require.Error(t, err)
		}
	}
	sibling, err := f.client.CreateDelegation(context.Background(), child, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Secrets: []string{"allowed"}}})
	require.NoError(t, err)
	siblingMac, _, _, err := decodeGrant(sibling.(*remoteExecutionLease).LeaseToken(), false)
	require.NoError(t, err)
	// Binary tampering preserves structure but invalidates the final MAC.
	raw, err := siblingMac.MarshalBinary()
	require.NoError(t, err)
	raw = bytes.Replace(raw, []byte(`"allowed"`), []byte(`"unknown"`), 1)
	_, err = f.signer.parseDelegation(delegationTokenPrefix + base64.RawURLEncoding.EncodeToString(raw))
	require.Error(t, err)
	owner, err := decodeOwner(f.owner.(*remoteExecutionLease).LeaseToken())
	require.NoError(t, err)
	_, err = f.signer.parseDelegation(owner.Grant)
	require.Error(t, err, "bare root grants cannot be used as children")
	raw, err = m.MarshalBinary()
	require.NoError(t, err)
	_, err = f.signer.parseDelegation(delegationTokenPrefix + base64.RawURLEncoding.EncodeToString(append(raw, 0)))
	require.Error(t, err, "trailing binary data is rejected")
	_, err = f.signer.parseDelegation(child.LeaseToken() + "\n")
	require.Error(t, err, "base64 whitespace is noncanonical")
	require.NotEmpty(t, root.ID)
}

func TestAttenuationLimitsAndAudience(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	var lease jobdb.ExecutionLease = f.owner
	for i := 0; i < maxDelegationDepth; i++ {
		next, err := f.client.CreateDelegation(context.Background(), lease, jobdb.CreateDelegationRequest{})
		require.NoError(t, err)
		lease = next
	}
	_, err := f.client.CreateDelegation(context.Background(), lease, jobdb.CreateDelegationRequest{})
	require.ErrorContains(t, err, "depth")
	over := appendTestRestriction(t, lease.(*remoteExecutionLease).LeaseToken(), jobdb.DelegationPermissions{}, nil)
	_, err = f.signer.parseDelegation(over)
	require.Error(t, err)
	_, err = f.signer.parseDelegation(delegationTokenPrefix + strings.Repeat("A", maxDelegationTokenBytes))
	require.Error(t, err)
	huge := make([]string, 256)
	for i := range huge {
		huge[i] = strings.Repeat("s", 1000)
	}
	_, err = f.client.CreateDelegation(context.Background(), f.owner, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Secrets: huge}})
	require.ErrorContains(t, err, "size limit")
	child := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	other := &leaseTokenSigner{key: f.signer.key, audience: "other-service"}
	_, err = other.parseDelegation(child.LeaseToken())
	require.Error(t, err)
	_, err = other.parse(f.owner.(*remoteExecutionLease).LeaseToken())
	require.Error(t, err)
	rotated := &leaseTokenSigner{key: bytes.Repeat([]byte{33}, 32)}
	_, err = rotated.parseDelegation(child.LeaseToken())
	require.Error(t, err)
	// Wrong audience, unknown key ID and changed parent are rejected even if a
	// privileged issuer accidentally signs them with the current root key.
	for _, change := range []func(*grantRoot){
		func(r *grantRoot) { r.Audience = "other" }, func(r *grantRoot) { r.KeyID = "unknown" }, func(r *grantRoot) { r.Purpose = "owner" },
	} {
		m, root, _, err := decodeGrant(child.LeaseToken(), false)
		require.NoError(t, err)
		change(&root)
		raw, err := json.Marshal(root)
		require.NoError(t, err)
		bad, err := macaroon.New(f.signer.grantKey(), raw, "", macaroon.V2)
		require.NoError(t, err)
		for _, cv := range m.Caveats() {
			require.NoError(t, bad.AddFirstPartyCaveat(cv.Id))
		}
		token, err := encodeGrant(bad)
		require.NoError(t, err)
		_, err = f.signer.parseDelegation(token)
		require.Error(t, err)
	}
}

func TestAttenuationRESTCreationUsesSameGrant(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	status, raw := delegationHTTP(t, f, "POST", delegationBase(f.owner)+"/delegations", f.owner.(*remoteExecutionLease).LeaseToken(), jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	require.Equal(t, 200, status, string(raw))
	var snapshot runtimeapi.ExecutionLease
	require.NoError(t, json.Unmarshal(raw, &snapshot))
	c, err := f.signer.parseDelegation(snapshot.LeaseToken)
	require.NoError(t, err)
	require.Equal(t, delegationPolicy(), c.Grant.Permissions)
	handle, err := f.client.executionLeaseFromAPI(snapshot)
	require.NoError(t, err)
	_, err = f.client.CreateDelegation(context.Background(), handle, jobdb.CreateDelegationRequest{Permissions: jobdb.DelegationPermissions{Secrets: []string{"allowed"}}})
	require.NoError(t, err)
}

func TestAttenuationConcurrentDerivationAndOwnerRenewal(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%4 == 0 {
				errs <- f.owner.KeepAlive(context.Background())
				return
			}
			child, err := f.client.CreateDelegation(context.Background(), f.owner, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
			if err == nil {
				cap, e := ExportLease(child)
				err = e
				if err == nil {
					_, err = f.client.ImportLease(context.Background(), cap)
				}
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestAttenuationChainCannotBeStrippedOrReordered(t *testing.T) {
	f := newDelegationFixture(t, "toy", nil)
	child := f.create(t, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	token := appendTestRestriction(t, child.LeaseToken(), delegationPolicy(), nil)
	m, _, _, err := decodeGrant(token, false)
	require.NoError(t, err)
	for _, reorder := range []bool{false, true} {
		raw, err := m.MarshalJSON()
		require.NoError(t, err)
		var wire map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &wire))
		var cs []json.RawMessage
		require.NoError(t, json.Unmarshal(wire["c"], &cs))
		require.Len(t, cs, 2)
		if reorder {
			cs[0], cs[1] = cs[1], cs[0]
		} else {
			cs = cs[1:]
		}
		wire["c"], err = json.Marshal(cs)
		require.NoError(t, err)
		raw, err = json.Marshal(wire)
		require.NoError(t, err)
		var bad macaroon.Macaroon
		require.NoError(t, bad.UnmarshalJSON(raw))
		forged, err := encodeGrant(&bad)
		require.NoError(t, err)
		_, _, _, err = decodeGrant(forged, false)
		require.NoError(t, err, "syntactically valid policy; MAC must detect the change")
		_, err = f.signer.parseDelegation(forged)
		require.Error(t, err)
	}
}

func TestAttenuationWireVector(t *testing.T) {
	signer := &leaseTokenSigner{key: bytes.Repeat([]byte{7}, 32), audience: "vector-service"}
	root := grantRoot{Version: 1, Purpose: grantPurpose, Audience: signer.audienceID(), KeyID: signer.grantKeyID(), ID: "root-vector", Parent: grantParent{TenantID: "tenant", JobID: "job", LeaseID: "lease", WorkerID: "worker"}, Ceiling: grantCeiling}
	raw, err := json.Marshal(root)
	require.NoError(t, err)
	m, err := macaroon.New(signer.grantKey(), raw, "", macaroon.V2)
	require.NoError(t, err)
	deadline := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	step := grantStep{Kind: "restrict", Version: 1, ID: "step-vector", Permissions: jobdb.DelegationPermissions{Secrets: []string{"secret"}}, NotAfter: &deadline}
	caveat, err := json.Marshal(step)
	require.NoError(t, err)
	require.NoError(t, m.AddFirstPartyCaveat(caveat))
	token, err := encodeGrant(m)
	require.NoError(t, err)
	const expected = "jdb-m1.AgL2AXsidmVyc2lvbiI6MSwicHVycG9zZSI6ImpvYmRiLmRlbGVnYXRpb24uZ3JhbnQudjEiLCJhdWRpZW5jZSI6InZlY3Rvci1zZXJ2aWNlIiwia2V5SWQiOiJkMDg0MTEzNmFiZjQ2OGZmIiwiaWQiOiJyb290LXZlY3RvciIsInBhcmVudCI6eyJ0ZW5hbnRJZCI6InRlbmFudCIsImpvYklkIjoiam9iIiwibGVhc2VJZCI6ImxlYXNlIiwid29ya2VySWQiOiJ3b3JrZXIifSwiY2VpbGluZyI6ImpvYmRiLnJlc291cmNlLWFjdGlvbnMudjEifQACgQF7ImtpbmQiOiJyZXN0cmljdCIsInZlcnNpb24iOjEsImlkIjoic3RlcC12ZWN0b3IiLCJwZXJtaXNzaW9ucyI6eyJzZWNyZXRzLnJlc29sdmUiOlsic2VjcmV0Il19LCJub3RBZnRlciI6IjIwMzAtMDEtMDJUMDM6MDQ6MDVaIn0AAAYgj03yUfHnqbYb_uh4Xmv9cblihruHG30O3amXYDTmPsA"
	require.Equal(t, expected, token)
	c, err := signer.parseDelegation(expected)
	require.NoError(t, err)
	require.Equal(t, &deadline, c.Grant.NotAfter)
}

func FuzzAttenuationParsing(f *testing.F) {
	signer := &leaseTokenSigner{key: bytes.Repeat([]byte{7}, 32)}
	root, err := signer.newGrant(leaseTokenClaims{TenantID: "t", JobID: "j", LeaseID: "l", WorkerID: "w"})
	if err != nil {
		f.Fatal(err)
	}
	token, _, err := attenuateGrant(root, jobdb.CreateDelegationRequest{Permissions: delegationPolicy()})
	if err != nil {
		f.Fatal(err)
	}
	f.Add("")
	f.Add(root)
	f.Add(token)
	f.Add("jdb-m1.invalid")
	f.Fuzz(func(t *testing.T, token string) {
		_, _ = decodeOwner(token)
		c, err := signer.parseDelegation(token)
		if err == nil {
			require.NotEmpty(t, c.Grant.ID)
			m, _, _, err := decodeGrant(token, false)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(m.Caveats()), 1)
			require.LessOrEqual(t, len(m.Caveats()), maxDelegationDepth)
			encoded, err := encodeGrant(m)
			require.NoError(t, err)
			require.Equal(t, token, encoded)
		}
	})
}
