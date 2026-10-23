package remote

import (
	"bytes"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/google/uuid"
	"gopkg.in/macaroon.v2"
)

const (
	ownerTokenPrefix        = "jdb-o1."
	delegationTokenPrefix   = "jdb-m1."
	maxDelegationTokenBytes = 16 << 10
	maxOwnerTokenBytes      = 32 << 10
	maxDelegationDepth      = 8
	grantPurpose            = "jobdb.delegation.grant.v1"
	grantCeiling            = "jobdb.resource-actions.v1"
)

type grantParent struct {
	Principal  *string `json:"principal,omitempty"`
	TenantID   string  `json:"tenantId"`
	JobID      string  `json:"jobId"`
	LeaseID    string  `json:"leaseId"`
	WorkerID   string  `json:"workerId"`
	SchemaHash string  `json:"schemaHash,omitempty"`
}

func parentOf(p leaseTokenClaims) grantParent {
	return grantParent{p.Principal, p.TenantID, p.JobID, p.LeaseID, p.WorkerID, p.SchemaHash}
}
func (p grantParent) claims() leaseTokenClaims {
	return leaseTokenClaims{Principal: p.Principal, TenantID: p.TenantID, JobID: p.JobID, LeaseID: p.LeaseID, WorkerID: p.WorkerID, SchemaHash: p.SchemaHash}
}

type grantRoot struct {
	Version  int         `json:"version"`
	Purpose  string      `json:"purpose"`
	Audience string      `json:"audience"`
	KeyID    string      `json:"keyId"`
	ID       string      `json:"id"`
	Parent   grantParent `json:"parent"`
	Ceiling  string      `json:"ceiling"`
}
type grantStep struct {
	Kind        string                      `json:"kind"`
	Version     int                         `json:"version"`
	ID          string                      `json:"id"`
	Permissions jobdb.DelegationPermissions `json:"permissions"`
	NotAfter    *time.Time                  `json:"notAfter,omitempty"`
}

// Expiry and route are backend/local snapshots, never authenticated grant bounds.
type delegationClaims struct {
	Parent    leaseTokenClaims
	Grant     jobdb.DelegationInfo
	Route     jobdb.Route
	ExpiresAt time.Time
}

func (c delegationClaims) key() jobdb.JobKey {
	return jobdb.JobKey{TenantId: c.Parent.TenantID, JobId: c.Parent.JobID}
}
func (c delegationClaims) accessClaims() leaseTokenClaims {
	p := c.Parent
	p.ExpiresAt = c.ExpiresAt.Unix()
	p.ExpiresAtNS = c.ExpiresAt.UnixNano()
	return p
}
func (c *delegationClaims) setExpiry(parentExpiry time.Time) {
	c.ExpiresAt = parentExpiry
	if c.Grant.NotAfter != nil && (c.ExpiresAt.IsZero() || c.ExpiresAt.After(*c.Grant.NotAfter)) {
		c.ExpiresAt = *c.Grant.NotAfter
	}
}
func isDelegationToken(t string) bool { return strings.HasPrefix(t, delegationTokenPrefix) }
func (s *leaseTokenSigner) audienceID() string {
	if s.audience != "" {
		return s.audience
	}
	return "jobdb"
}
func (s *leaseTokenSigner) grantKey() []byte {
	key, err := hkdf.Key(sha256.New, s.key, nil, grantPurpose+"\x00"+s.audienceID(), 32)
	if err != nil {
		panic(err)
	} // fixed output length, within HKDF bounds
	return key
}
func (s *leaseTokenSigner) grantKeyID() string {
	sum := sha256.Sum256(s.grantKey())
	return hex.EncodeToString(sum[:8])
}

// Canonical JSON rejects alternate encodings, duplicate/unknown fields and trailing
// data. No untrusted text is included in errors (it may contain credentials).
func decodeCanonical(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return jobdb.ErrExecutionLeaseLost
	}
	canonical, err := json.Marshal(out)
	if err != nil || !bytes.Equal(raw, canonical) {
		return jobdb.ErrExecutionLeaseLost
	}
	return nil
}

// Even Strict base64 decoding accepts CR/LF. Require a canonical envelope too.
func decodeTokenBase64(encoded string) ([]byte, error) {
	raw, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != encoded {
		return nil, jobdb.ErrExecutionLeaseLost
	}
	return raw, nil
}
func encodeGrant(m *macaroon.Macaroon) (string, error) {
	raw, err := m.MarshalBinary()
	if err != nil {
		return "", jobdb.ErrExecutionLeaseLost
	}
	token := delegationTokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	if len(token) > maxDelegationTokenBytes {
		return "", fmt.Errorf("delegation exceeds token size limit")
	}
	return token, nil
}
func (s *leaseTokenSigner) newGrant(p leaseTokenClaims) (string, error) {
	root := grantRoot{1, grantPurpose, s.audienceID(), s.grantKeyID(), uuid.NewString(), parentOf(p), grantCeiling}
	raw, err := json.Marshal(root)
	if err != nil {
		return "", err
	}
	m, err := macaroon.New(s.grantKey(), raw, "", macaroon.V2)
	if err != nil {
		return "", err
	}
	return encodeGrant(m)
}

// decodeGrant performs bounded syntax/policy validation only. Only verifyGrant
// authenticates the issuer. Offline callers have no server signing key.
func decodeGrant(token string, allowRoot bool) (*macaroon.Macaroon, grantRoot, delegationClaims, error) {
	invalid := func() (*macaroon.Macaroon, grantRoot, delegationClaims, error) {
		return nil, grantRoot{}, delegationClaims{}, jobdb.ErrExecutionLeaseLost
	}
	if !isDelegationToken(token) || len(token) > maxDelegationTokenBytes {
		return invalid()
	}
	raw, err := decodeTokenBase64(strings.TrimPrefix(token, delegationTokenPrefix))
	if err != nil {
		return invalid()
	}
	var m macaroon.Macaroon
	if m.UnmarshalBinary(raw) != nil || m.Version() != macaroon.V2 || m.Location() != "" {
		return invalid()
	}
	// The library accepts trailing bytes; require its exact canonical binary output.
	encoded, err := m.MarshalBinary()
	if err != nil || !bytes.Equal(raw, encoded) {
		return invalid()
	}
	var root grantRoot
	if decodeCanonical(m.Id(), &root) != nil || root.Version != 1 || root.Purpose != grantPurpose || root.Ceiling != grantCeiling || root.ID == "" || root.Audience == "" || root.KeyID == "" || root.Parent.TenantID == "" || root.Parent.JobID == "" || root.Parent.LeaseID == "" {
		return invalid()
	}
	caveats := m.Caveats()
	if len(caveats) > maxDelegationDepth || (!allowRoot && (len(caveats) == 0 || root.Parent.WorkerID == "")) {
		return invalid()
	}
	c := delegationClaims{Parent: root.Parent.claims()}
	for i, cv := range caveats {
		if len(cv.VerificationId) != 0 || cv.Location != "" {
			return invalid()
		}
		var step grantStep
		if decodeCanonical(cv.Id, &step) != nil || step.Kind != "restrict" || step.Version != 1 || step.ID == "" || len(step.ID) > 128 || validateDelegationPolicy(step.Permissions) != nil {
			return invalid()
		}
		if step.NotAfter != nil && (step.NotAfter.IsZero() || step.NotAfter.Year() < 1 || step.NotAfter.Year() > 9999 || step.NotAfter.Location() != time.UTC) {
			return invalid()
		}
		if i > 0 && !permissionsSubset(step.Permissions, c.Grant.Permissions) {
			return invalid()
		}
		if c.Grant.NotAfter != nil && step.NotAfter != nil && step.NotAfter.After(*c.Grant.NotAfter) {
			return invalid()
		}
		if step.NotAfter != nil {
			c.Grant.NotAfter = step.NotAfter
		}
		// Each step is proven to be a subset of the previous effective policy, so
		// this equals the conjunction of every authenticated restriction.
		c.Grant.ID = step.ID
		c.Grant.Permissions = step.Permissions
	}
	return &m, root, c, nil
}
func (s *leaseTokenSigner) verifyGrant(token string, allowRoot bool) (*macaroon.Macaroon, grantRoot, delegationClaims, error) {
	m, root, c, err := decodeGrant(token, allowRoot)
	if err != nil {
		return nil, root, c, err
	}
	if root.Audience != s.audienceID() || root.KeyID != s.grantKeyID() {
		return nil, root, c, jobdb.ErrExecutionLeaseLost
	}
	// decodeGrant has evaluated every first-party caveat and rejected third parties.
	if _, err = m.VerifySignature(s.grantKey(), nil); err != nil {
		return nil, root, c, jobdb.ErrExecutionLeaseLost
	}
	return m, root, c, nil
}
func (s *leaseTokenSigner) parseDelegation(token string) (delegationClaims, error) {
	_, _, c, err := s.verifyGrant(token, false)
	return c, err
}
func attenuateGrant(token string, req jobdb.CreateDelegationRequest) (string, delegationClaims, error) {
	m, root, c, err := decodeGrant(token, true)
	if err != nil {
		return "", c, err
	}
	if root.Parent.WorkerID == "" {
		return "", c, jobdb.ErrExecutionLeaseLost
	}
	if err = validateDelegationRequest(&req); err != nil {
		return "", c, err
	}
	if len(m.Caveats()) >= maxDelegationDepth {
		return "", c, fmt.Errorf("delegation exceeds maximum restriction depth")
	}
	if len(m.Caveats()) > 0 && !permissionsSubset(req.Permissions, c.Grant.Permissions) {
		return "", c, jobdb.ErrLeasePermissionDenied
	}
	if c.Grant.NotAfter != nil {
		if !time.Now().Before(*c.Grant.NotAfter) || req.NotAfter != nil && req.NotAfter.After(*c.Grant.NotAfter) {
			return "", c, jobdb.ErrLeasePermissionDenied
		}
		if req.NotAfter == nil {
			req.NotAfter = c.Grant.NotAfter
		}
	}
	if req.NotAfter != nil {
		t := req.NotAfter.UTC()
		req.NotAfter = &t
	}
	step := grantStep{"restrict", 1, uuid.NewString(), req.Permissions, req.NotAfter}
	raw, err := json.Marshal(step)
	if err != nil {
		return "", c, err
	}
	// Unmarshal into a fresh value so caller-owned slices cannot mutate the result.
	step = grantStep{}
	if err = decodeCanonical(raw, &step); err != nil {
		return "", c, err
	}
	m = m.Clone()
	if err = m.AddFirstPartyCaveat(raw); err != nil {
		return "", c, jobdb.ErrExecutionLeaseLost
	}
	next, err := encodeGrant(m)
	if err != nil {
		return "", c, err
	}
	c.Grant = jobdb.DelegationInfo{ID: step.ID, Permissions: step.Permissions, NotAfter: step.NotAfter}
	return next, c, nil
}
func namesSubset(child, parent jobdb.DelegationNames) bool {
	if parent.Any {
		return true
	}
	if child.Any {
		return false
	}
	for _, n := range child.Names {
		if !slices.Contains(parent.Names, n) {
			return false
		}
	}
	return true
}
func ordinalsSubset(child, parent []int64) bool {
	for _, n := range child {
		if !slices.Contains(parent, n) {
			return false
		}
	}
	return true
}
func chapterIntervals(p *jobdb.DelegationChapters) []jobdb.DelegationOrdinalRange {
	rs := append([]jobdb.DelegationOrdinalRange(nil), p.Ranges...)
	for _, n := range p.Ordinals {
		rs = append(rs, jobdb.DelegationOrdinalRange{Start: n, End: n})
	}
	slices.SortFunc(rs, func(a, b jobdb.DelegationOrdinalRange) int {
		if a.Start < b.Start {
			return -1
		}
		if a.Start > b.Start {
			return 1
		}
		return 0
	})
	merged := make([]jobdb.DelegationOrdinalRange, 0, len(rs))
	for _, r := range rs {
		n := len(merged)
		if n == 0 || (r.Start > merged[n-1].End && r.Start-merged[n-1].End > 1) {
			merged = append(merged, r)
		} else if r.End > merged[n-1].End {
			merged[n-1].End = r.End
		}
	}
	return merged
}
func chaptersSubset(c, p *jobdb.DelegationChapters) bool {
	if c == nil {
		return true
	}
	if p == nil {
		return false
	}
	if p.Any {
		return true
	}
	if c.Any {
		return false
	}
	parent := chapterIntervals(p)
	for _, r := range chapterIntervals(c) {
		covered := false
		for _, a := range parent {
			if a.Start <= r.Start && a.End >= r.End {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}
func permissionsSubset(c, p jobdb.DelegationPermissions) bool {
	if c.Submit != nil && (p.Submit == nil || !namesSubset(c.Submit.JobTypes, p.Submit.JobTypes) || !namesSubset(c.Submit.JobIDs, p.Submit.JobIDs)) {
		return false
	}
	if !namesSubset(jobdb.DelegationNames{Names: c.Secrets}, jobdb.DelegationNames{Names: p.Secrets}) || !chaptersSubset(c.Read, p.Read) {
		return false
	}
	if c.Write != nil && (p.Write == nil || !ordinalsSubset(c.Write.Ordinals, p.Write.Ordinals) || !namesSubset(c.Write.TaskTypes, p.Write.TaskTypes) || !namesSubset(c.Write.ArtifactNames, p.Write.ArtifactNames)) {
		return false
	}
	return true
}
