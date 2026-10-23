package remote

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
)

const (
	defaultLeaseTokenTTL     = 30 * time.Second
	leaseTokenExpirySkew     = 100 * time.Millisecond
	minimumLeaseTokenKeySize = 32
)

type leaseTokenClaims struct {
	Audience             string  `json:"audience"`
	Grant                string  `json:"grant,omitempty"`
	Principal            *string `json:"principal,omitempty"`
	TenantID             string  `json:"tenant_id"`
	JobID                string  `json:"job_id"`
	LeaseID              string  `json:"lease_id"`
	WorkerID             string  `json:"worker_id,omitempty"`
	SchemaHash           string  `json:"schema_hash,omitempty"`
	LeaseDurationSeconds int64   `json:"lease_duration_seconds"`
	IssuedAt             int64   `json:"iat"`
	ExpiresAt            int64   `json:"exp"`
	ExpiresAtNS          int64   `json:"exp_ns,omitempty"`
}

type leaseTokenSigner struct {
	key      []byte
	audience string
}

func newLeaseTokenSigner() *leaseTokenSigner {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Errorf("generate lease token signing key: %w", err))
	}
	return &leaseTokenSigner{key: key}
}

func newLeaseTokenSignerWithKey(key []byte) (*leaseTokenSigner, error) {
	if len(key) < minimumLeaseTokenKeySize {
		return nil, fmt.Errorf("lease token signing key must contain at least %d bytes", minimumLeaseTokenKeySize)
	}
	return &leaseTokenSigner{key: append([]byte(nil), key...)}, nil
}

func (s *leaseTokenSigner) mintForLease(lease jobdb.ExecutionLease, ttl time.Duration) (string, error) {
	if lease == nil {
		return "", fmt.Errorf("lease is required")
	}
	return s.mintForLeaseExpiry(lease.Job().JobKey, lease.LeaseID(), leaseWorkerID(lease), leaseSchemaHash(lease), leaseExpiry(lease), ttl)
}

func (s *leaseTokenSigner) mint(jobKey jobdb.JobKey, leaseID string, workerID string, ttl time.Duration) (string, error) {
	return s.mintForLeaseExpiry(jobKey, leaseID, workerID, "", time.Time{}, ttl)
}

func (s *leaseTokenSigner) mintForLeaseExpiry(jobKey jobdb.JobKey, leaseID string, workerID string, schemaHash string, leaseExpiresAt time.Time, ttl time.Duration) (string, error) {
	return s.mintWithPrincipal(jobKey, leaseID, workerID, schemaHash, leaseExpiresAt, ttl, nil)
}

func (s *leaseTokenSigner) mintWithPrincipal(jobKey jobdb.JobKey, leaseID string, workerID string, schemaHash string, leaseExpiresAt time.Time, ttl time.Duration, principal *string) (string, error) {
	if s == nil || len(s.key) == 0 {
		return "", fmt.Errorf("lease token signer is required")
	}
	if ttl <= 0 {
		ttl = defaultLeaseTokenTTL
	}
	now := time.Now().UTC()
	leaseExpiresAt = leaseExpiresAt.UTC()
	leaseDuration := ttl
	if !leaseExpiresAt.IsZero() && leaseExpiresAt.After(now) {
		leaseDuration = leaseExpiresAt.Sub(now)
	}
	tokenExpiresAt := leaseTokenExpiresAt(now, leaseExpiresAt, ttl)
	claims := leaseTokenClaims{
		Audience:             s.audienceID(),
		Principal:            principal,
		TenantID:             jobKey.TenantId,
		JobID:                jobKey.JobId,
		LeaseID:              leaseID,
		WorkerID:             workerID,
		SchemaHash:           schemaHash,
		LeaseDurationSeconds: int64((leaseDuration + time.Second - 1) / time.Second),
		IssuedAt:             now.Unix(),
		ExpiresAt:            tokenExpiresAt.Unix(),
		ExpiresAtNS:          tokenExpiresAt.UnixNano(),
	}
	grant, err := s.newGrant(claims)
	if err != nil {
		return "", err
	}
	claims.Grant = grant
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encodedPayload := ownerTokenPrefix + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(encodedPayload))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	token := encodedPayload + "." + signature
	if len(token) > maxOwnerTokenBytes {
		return "", fmt.Errorf("owner credential exceeds token size limit")
	}
	return token, nil
}

func (s *leaseTokenSigner) validate(token string, jobKey jobdb.JobKey, leaseID string, now time.Time) error {
	_, err := s.validateAndParse(token, jobKey, leaseID, now)
	return err
}

func (s *leaseTokenSigner) validateAndParse(token string, jobKey jobdb.JobKey, leaseID string, now time.Time) (leaseTokenClaims, error) {
	claims, err := s.parse(token)
	if err != nil {
		return leaseTokenClaims{}, err
	}
	if claims.TenantID != jobKey.TenantId || claims.JobID != jobKey.JobId || claims.LeaseID != leaseID {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if !now.UTC().Before(claims.expiresAt()) {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	return claims, nil
}

func (s *leaseTokenSigner) parse(token string) (leaseTokenClaims, error) {
	if s == nil || len(s.key) == 0 {
		return leaseTokenClaims{}, fmt.Errorf("lease token signer is required")
	}
	if !strings.HasPrefix(token, ownerTokenPrefix) || len(token) > maxOwnerTokenBytes {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	actual, err := decodeTokenBase64(parts[2])
	if err != nil || !hmac.Equal(actual, mac.Sum(nil)) {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	claims, err := decodeOwner(token)
	if err != nil || claims.Audience != s.audienceID() {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	m, root, _, err := s.verifyGrant(claims.Grant, true)
	if err != nil || len(m.Caveats()) != 0 || !sameGrantParent(root.Parent, parentOf(claims)) {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	return claims, nil
}

func (c leaseTokenClaims) expiresAt() time.Time {
	if c.ExpiresAtNS > 0 {
		return time.Unix(0, c.ExpiresAtNS).UTC()
	}
	return time.Unix(c.ExpiresAt, 0).UTC()
}

func (c leaseTokenClaims) leaseDuration() time.Duration {
	if c.LeaseDurationSeconds <= 0 {
		return defaultLeaseTokenTTL
	}
	return time.Duration(c.LeaseDurationSeconds) * time.Second
}

func (c leaseTokenClaims) leaseAuthClaims() leaseauth.Claims {
	return leaseauth.Claims{
		Principal:  c.Principal,
		TenantID:   c.TenantID,
		JobID:      c.JobID,
		LeaseID:    c.LeaseID,
		WorkerID:   c.WorkerID,
		SchemaHash: c.SchemaHash,
		ExpiresAt:  c.expiresAt(),
	}
}

func leaseTokenTTL(requested time.Duration) time.Duration {
	if requested > 0 {
		return requested
	}
	return defaultLeaseTokenTTL
}

func leaseTokenExpiresAt(now time.Time, leaseExpiresAt time.Time, fallbackTTL time.Duration) time.Time {
	if fallbackTTL <= 0 {
		fallbackTTL = defaultLeaseTokenTTL
	}
	if leaseExpiresAt.IsZero() {
		return now.Add(fallbackTTL)
	}
	if !leaseExpiresAt.After(now) {
		return now
	}
	remaining := leaseExpiresAt.Sub(now)
	skew := leaseTokenExpirySkew
	if remaining <= 2*skew {
		skew = remaining / 10
		if skew <= 0 {
			skew = time.Nanosecond
		}
	}
	expiresAt := leaseExpiresAt.Add(-skew)
	if expiresAt.Before(now) {
		return now
	}
	return expiresAt
}

type leaseTokenWorkerSource interface {
	LeaseWorkerID() string
}

type leaseTokenExpirySource interface {
	LeaseExpiry() time.Time
}

type leaseTokenSchemaHashSource interface {
	LeaseSchemaHash() string
}

func leaseWorkerID(lease jobdb.ExecutionLease) string {
	if source, ok := lease.(leaseTokenWorkerSource); ok {
		return source.LeaseWorkerID()
	}
	return ""
}

func leaseExpiry(lease jobdb.ExecutionLease) time.Time {
	if source, ok := lease.(leaseTokenExpirySource); ok {
		return source.LeaseExpiry()
	}
	return time.Time{}
}

func leaseSchemaHash(lease jobdb.ExecutionLease) string {
	if source, ok := lease.(leaseTokenSchemaHashSource); ok {
		return source.LeaseSchemaHash()
	}
	return ""
}

func leaseTokenValidationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, jobdb.ErrExecutionLeaseLost) {
		return err
	}
	return jobdb.ErrExecutionLeaseLost
}

// decodeOwner is syntax inspection for local derivation, not authentication.
func decodeOwner(token string) (leaseTokenClaims, error) {
	var c leaseTokenClaims
	if !strings.HasPrefix(token, ownerTokenPrefix) || len(token) > maxOwnerTokenBytes {
		return c, jobdb.ErrExecutionLeaseLost
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[2] == "" {
		return c, jobdb.ErrExecutionLeaseLost
	}
	raw, err := decodeTokenBase64(parts[1])
	if err != nil || decodeCanonical(raw, &c) != nil || c.TenantID == "" || c.JobID == "" || c.LeaseID == "" || c.ExpiresAt == 0 || c.Audience == "" || c.Grant == "" {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	return c, nil
}
func sameGrantParent(a, b grantParent) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
