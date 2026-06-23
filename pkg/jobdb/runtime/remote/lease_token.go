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
	defaultLeaseTokenTTL = 30 * time.Second
	leaseTokenExpirySkew = 100 * time.Millisecond
)

type leaseTokenClaims struct {
	TenantID             string `json:"tenant_id"`
	JobID                string `json:"job_id"`
	LeaseID              string `json:"lease_id"`
	WorkerID             string `json:"worker_id,omitempty"`
	SchemaHash           string `json:"schema_hash,omitempty"`
	LeaseDurationSeconds int64  `json:"lease_duration_seconds"`
	IssuedAt             int64  `json:"iat"`
	ExpiresAt            int64  `json:"exp"`
	ExpiresAtNS          int64  `json:"exp_ns,omitempty"`
}

type leaseTokenSigner struct {
	key []byte
}

func newLeaseTokenSigner() *leaseTokenSigner {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Errorf("generate lease token signing key: %w", err))
	}
	return &leaseTokenSigner{key: key}
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
	payload, err := json.Marshal(leaseTokenClaims{
		TenantID:             jobKey.TenantId,
		JobID:                jobKey.JobId,
		LeaseID:              leaseID,
		WorkerID:             workerID,
		SchemaHash:           schemaHash,
		LeaseDurationSeconds: int64((leaseDuration + time.Second - 1) / time.Second),
		IssuedAt:             now.Unix(),
		ExpiresAt:            tokenExpiresAt.Unix(),
		ExpiresAtNS:          tokenExpiresAt.UnixNano(),
	})
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(encodedPayload))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encodedPayload + "." + signature, nil
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
	parts := strings.Split(token, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(parts[0]))
	expected := mac.Sum(nil)
	actual, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	if !hmac.Equal(actual, expected) {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	var claims leaseTokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return leaseTokenClaims{}, jobdb.ErrExecutionLeaseLost
	}
	if claims.TenantID == "" || claims.JobID == "" || claims.LeaseID == "" || claims.ExpiresAt == 0 {
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
