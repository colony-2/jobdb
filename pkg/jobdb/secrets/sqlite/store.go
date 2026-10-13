// Package sqlitesecrets implements a SQLite development secret provider.
// It can be injected into a runtime regardless of that runtime's job storage.
package sqlitesecrets

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
	_ "modernc.org/sqlite"
)

// Store keeps plaintext tenant secret values in its own SQLite database.
// Callers own its lifecycle and may share it across runtimes.
type Store struct {
	db               *sql.DB
	maxResponseBytes int64
}

var _ jobdb.SecretProvider = (*Store)(nil)

// Open opens or creates a plaintext development secret store. Use :memory: for
// an ephemeral store. The caller must close the returned Store.
func Open(ctx context.Context, path string, limits jobdb.SecretLimits) (*Store, error) {
	fail := errors.New("development secret store could not be opened")
	if path == "" {
		return nil, fail
	}
	dsn := path
	if path != ":memory:" {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, fail
		}
		if err := os.MkdirAll(filepath.Dir(abs), 0700); err != nil {
			return nil, fail
		}
		file, err := os.OpenFile(abs, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err == nil {
			_ = file.Close()
		} else if !errors.Is(err, os.ErrExist) {
			return nil, fail
		}
		info, err := os.Stat(abs)
		if err != nil || !info.Mode().IsRegular() {
			return nil, fail
		}
		dsn = (&url.URL{Scheme: "file", Path: abs}).String()
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fail
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ok := false
	defer func() {
		if !ok {
			_ = db.Close()
		}
	}()
	if _, err = db.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		return nil, fail
	}
	// Never reinterpret an old encrypted store as plaintext or overwrite it.
	var legacy int
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='format'").Scan(&legacy); err != nil {
		return nil, fail
	}
	if legacy != 0 {
		return nil, errors.New("encrypted development secret stores are no longer supported; populate a new store")
	}
	if _, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS secrets (tenant_id TEXT NOT NULL, name TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(tenant_id,name))`); err != nil {
		return nil, fail
	}
	ok = true
	return &Store{db: db, maxResponseBytes: limits.WithDefaults().MaxResponseBytes}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Set(ctx context.Context, tenant, name, value string) error {
	if tenant == "" || name == "" || !utf8.ValidString(tenant) || !utf8.ValidString(name) || !utf8.ValidString(value) {
		return jobdb.ErrInvalidSecretRequest
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO secrets(tenant_id,name,value) VALUES(?,?,?) ON CONFLICT(tenant_id,name) DO UPDATE SET value=excluded.value`, tenant, name, value)
	return storeError(err)
}
func (s *Store) Remove(ctx context.Context, tenant, name string) error {
	if tenant == "" || name == "" || !utf8.ValidString(tenant) || !utf8.ValidString(name) {
		return jobdb.ErrInvalidSecretRequest
	}
	_, err := s.db.ExecContext(ctx, "DELETE FROM secrets WHERE tenant_id=? AND name=?", tenant, name)
	return storeError(err)
}

func (s *Store) ResolveSecrets(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	result := jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{}}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return jobdb.SecretResolveResult{}, storeError(err)
	}
	defer tx.Rollback()
	var size int64
	for _, name := range req.Names {
		var value string
		err := tx.QueryRowContext(ctx, "SELECT value FROM secrets WHERE tenant_id=? AND name=?", req.TenantID, name).Scan(&value)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return jobdb.SecretResolveResult{}, storeError(err)
		}
		size += int64(len(name)) + int64(len(value))
		if size > s.maxResponseBytes {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
		}
		result.Secrets[name] = jobdb.SecretValue{Value: value}
	}
	if err := tx.Commit(); err != nil {
		return jobdb.SecretResolveResult{}, storeError(err)
	}
	return result, nil
}

func storeError(err error) error {
	if err == nil {
		return nil
	}
	var code interface{ Code() int }
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &code) && (code.Code()&255 == 5 || code.Code()&255 == 6)) {
		return jobdb.ErrSecretsUnavailable
	}
	return jobdb.ErrSecretResolutionFailed
}

func (s *Store) String() string { return "SQLite development secret store (redacted)" }
