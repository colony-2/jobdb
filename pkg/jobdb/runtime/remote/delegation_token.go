package remote

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// A distinct, signed format prevents old servers and owner-token validators from
// accepting a delegation as unrestricted authority. The entire payload is signed.
const delegationTokenPrefix = "jdb-d2."
const maxDelegationTokenBytes = 16 << 10

// Credential freshness is an internal transport bound, not the grant lifetime.
const delegationAccessTTL = 30 * time.Second

type delegationClaims struct {
	Parent    leaseTokenClaims     `json:"parent"`
	Grant     jobdb.DelegationInfo `json:"grant"`
	Route     jobdb.Route          `json:"route"`
	ExpiresAt time.Time            `json:"expiresAt"`
}

func isDelegationToken(token string) bool { return strings.HasPrefix(token, delegationTokenPrefix) }

func (s *leaseTokenSigner) signDelegation(c delegationClaims) (string, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	payload := delegationTokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(payload))
	token := payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if len(token) > maxDelegationTokenBytes {
		return "", fmt.Errorf("delegation policy is too large")
	}
	return token, nil
}

func (s *leaseTokenSigner) parseDelegation(token string) (delegationClaims, error) {
	var c delegationClaims
	if !isDelegationToken(token) || len(token) > maxDelegationTokenBytes {
		return c, jobdb.ErrExecutionLeaseLost
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return c, jobdb.ErrExecutionLeaseLost
	}
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
		return c, jobdb.ErrExecutionLeaseLost
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(raw, &c) != nil || c.Grant.ID == "" || c.Parent.TenantID == "" || c.Parent.JobID == "" || c.Parent.LeaseID == "" || c.Parent.WorkerID == "" || c.ExpiresAt.IsZero() {
		return delegationClaims{}, jobdb.ErrExecutionLeaseLost
	}
	return c, nil
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
	c.ExpiresAt = time.Now().UTC().Add(delegationAccessTTL)
	if c.ExpiresAt.After(parentExpiry) {
		c.ExpiresAt = parentExpiry
	}
	if c.Grant.NotAfter != nil && c.ExpiresAt.After(*c.Grant.NotAfter) {
		c.ExpiresAt = *c.Grant.NotAfter
	}
}
func namesAllow(s jobdb.DelegationNames, name string) bool {
	return s.Any || slices.Contains(s.Names, name)
}
func chaptersAllow(s *jobdb.DelegationChapters, ordinal int64) bool {
	if s == nil || ordinal < 0 {
		return false
	}
	if s.Any || slices.Contains(s.Ordinals, ordinal) {
		return true
	}
	for _, r := range s.Ranges {
		if ordinal >= r.Start && ordinal <= r.End {
			return true
		}
	}
	return false
}

func validateDelegationRequest(req *jobdb.CreateDelegationRequest) error {
	if req.NotAfter != nil && !req.NotAfter.After(time.Now()) {
		return fmt.Errorf("notAfter must be in the future")
	}
	names := func(s jobdb.DelegationNames) bool {
		if s.Any && len(s.Names) > 0 || len(s.Names) > 256 {
			return false
		}
		for _, n := range s.Names {
			if n == "" || len(n) > 1024 {
				return false
			}
		}
		return true
	}
	ordinals := func(ns []int64) bool {
		if len(ns) > 256 {
			return false
		}
		for _, n := range ns {
			if n < 0 {
				return false
			}
		}
		return true
	}
	p := req.Permissions
	if p.Submit != nil && (!names(p.Submit.JobTypes) || !names(p.Submit.JobIDs)) {
		return fmt.Errorf("invalid submission selectors")
	}
	if !names(jobdb.DelegationNames{Names: p.Secrets}) {
		return fmt.Errorf("invalid secret names")
	}
	if p.Read != nil {
		if !ordinals(p.Read.Ordinals) || len(p.Read.Ranges) > 256 || p.Read.Any && (len(p.Read.Ordinals) > 0 || len(p.Read.Ranges) > 0) {
			return fmt.Errorf("invalid chapter selectors")
		}
		for _, r := range p.Read.Ranges {
			if r.Start < 0 || r.End < r.Start {
				return fmt.Errorf("invalid chapter range")
			}
		}
	}
	if p.Write != nil && (!ordinals(p.Write.Ordinals) || !names(p.Write.TaskTypes) || !names(p.Write.ArtifactNames)) {
		return fmt.Errorf("invalid chapter write selectors")
	}
	return nil
}
