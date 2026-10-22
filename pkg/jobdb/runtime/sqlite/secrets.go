package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsbroker"
	sqlitesecrets "github.com/colony-2/jobdb/pkg/jobdb/secrets/sqlite"
)

func (r *Runtime) SecretResolutionLimits() jobdb.SecretLimits { return r.secretLimits.WithDefaults() }

// ResolveSecrets accepts claims verified by the HTTP transport. Embedded workers
// use their issued lease's ResolveSecrets method instead of presenting raw IDs.
func (r *Runtime) ResolveSecrets(ctx context.Context, req jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	access, err := secretsbroker.FromRequest(ctx, req)
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	return r.resolveSecrets(ctx, access.JobKey, access.LeaseID, access.WorkerID, access.Principal, access.TokenExpiry, req.Names)
}

func (l *executionLease) ResolveSecrets(ctx context.Context, names []string) (jobdb.SecretResolveResult, error) {
	if err := l.checkExpiry(); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	return l.runtime.resolveSecrets(ctx, l.jobKey, l.leaseID, l.workerID, nil, l.LeaseExpiry(), names)
}

func (r *Runtime) resolveSecrets(ctx context.Context, key jobdb.JobKey, leaseID, workerID string, principal *string, tokenExpiry time.Time, names []string) (jobdb.SecretResolveResult, error) {
	return secretsbroker.Resolve(ctx, r.secretProvider, r.secretLimits, secretsbroker.Access{
		JobKey: key, LeaseID: leaseID, WorkerID: workerID, Principal: principal, TokenExpiry: tokenExpiry,
	}, names, func(ctx context.Context) (secretsbroker.Snapshot, error) {
		row, err := r.validateLease(ctx, key, leaseID, workerID)
		if err != nil {
			return secretsbroker.Snapshot{}, err
		}
		var envelope jobdb.JobMetadataEnvelope
		if len(row.metadata) > 0 && json.Unmarshal(row.metadata, &envelope) != nil {
			return secretsbroker.Snapshot{}, jobdb.ErrSecretResolutionFailed
		}
		return secretsbroker.Snapshot{AppMetadata: envelope.App, LeaseExpiresAt: timeFromNS(row.leaseExpiresAtNS.Int64)}, nil
	})
}

// SetDevSecret and RemoveDevSecret are development administration operations.
// HTTP exposure is deliberately limited to runtimes implementing these methods.
func (r *Runtime) SetDevSecret(ctx context.Context, tenant, name, value string) error {
	if r.devSecrets == nil {
		return jobdb.ErrDevSecretsNotConfigured
	}
	raw, _ := json.Marshal(struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}{name, value})
	if int64(len(raw)) > r.SecretResolutionLimits().MaxResponseBytes {
		return jobdb.ErrSecretRequestTooLarge
	}
	return r.devSecrets.Set(ctx, tenant, name, value)
}
func (r *Runtime) RemoveDevSecret(ctx context.Context, tenant, name string) error {
	if r.devSecrets == nil {
		return jobdb.ErrDevSecretsNotConfigured
	}
	return r.devSecrets.Remove(ctx, tenant, name)
}

// Ask SQLite for the actual main file so URI DSNs and caller-owned handles
// follow the same naming rule. In-memory and temporary databases have no file.
func openDefaultSecrets(ctx context.Context, db *sql.DB, limits jobdb.SecretLimits) (*sqlitesecrets.Store, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		return nil, errors.New("could not locate SQLite database for development secrets")
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name, path string
		if err := rows.Scan(&seq, &name, &path); err != nil {
			return nil, errors.New("could not locate SQLite database for development secrets")
		}
		if name == "main" {
			if path == "" {
				path = ":memory:"
			} else {
				path += ".secrets.db"
			}
			return sqlitesecrets.Open(ctx, path, limits)
		}
	}
	return nil, errors.New("could not locate SQLite database for development secrets")
}
