package leaseauth

import (
	"context"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type contextKey struct{}

// Claims is the write capability material after a remote lease token has been
// validated by the jobdb remote server.
type Claims struct {
	TenantID   string
	JobID      string
	LeaseID    string
	WorkerID   string
	SchemaHash string
	ExpiresAt  time.Time
}

func WithClaims(ctx context.Context, claims Claims) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, contextKey{}, claims)
}

func ClaimsFromContext(ctx context.Context) (Claims, bool) {
	if ctx == nil {
		return Claims{}, false
	}
	claims, ok := ctx.Value(contextKey{}).(Claims)
	return claims, ok
}

func Matches(claims Claims, jobKey jobdb.JobKey, leaseID string) bool {
	return claims.TenantID == jobKey.TenantId && claims.JobID == jobKey.JobId && claims.LeaseID == leaseID
}

func Authorize(ctx context.Context, jobKey jobdb.JobKey, leaseID string) (bool, error) {
	claims, ok := ClaimsFromContext(ctx)
	if !ok {
		return false, nil
	}
	if !Matches(claims, jobKey, leaseID) {
		return true, jobdb.ErrExecutionLeaseLost
	}
	if !claims.ExpiresAt.IsZero() && !time.Now().UTC().Before(claims.ExpiresAt) {
		return true, jobdb.ErrExecutionLeaseLost
	}
	return true, nil
}
