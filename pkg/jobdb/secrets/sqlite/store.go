// Package sqlitesecrets implements an encrypted SQLite development secret provider.
// It can be injected into a runtime regardless of that runtime's job storage.
package sqlitesecrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
	_ "modernc.org/sqlite"
)

// Store encrypts tenant secret values in its own SQLite database.
// Callers own its lifecycle and may share it across runtimes.
type Store struct {
	db               *sql.DB
	aead             cipher.AEAD
	maxResponseBytes int64
}

var _ jobdb.SecretProvider = (*Store)(nil)

// Open opens or creates a development secret store. Existing stores must
// authenticate their key check. The caller must close the returned Store.
func Open(ctx context.Context, path, keyFile string, limits jobdb.SecretLimits) (*Store, error) {
	fail := errors.New("development secret store could not be opened; check its configuration and encryption key")
	rawKey, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fail
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(rawKey)))
	if err != nil || len(key) != 32 || path == "" {
		return nil, fail
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fail
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fail
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fail
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	created := err == nil
	if created {
		_ = file.Close()
	} else if !errors.Is(err, os.ErrExist) {
		return nil, fail
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, fail
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fail
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, aead: aead, maxResponseBytes: limits.WithDefaults().MaxResponseBytes}
	ok := false
	defer func() {
		if !ok {
			_ = db.Close()
		}
	}()
	if _, err = db.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		return nil, fail
	}
	if created {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fail
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, `CREATE TABLE format (id INTEGER PRIMARY KEY CHECK(id=1), check_value BLOB NOT NULL);
CREATE TABLE secrets (tenant_id TEXT NOT NULL, name TEXT NOT NULL, value BLOB NOT NULL, PRIMARY KEY(tenant_id,name));`); err != nil {
			return nil, fail
		}
		check := s.seal([]byte("jobdb-dev-secrets-v1"), []byte("format"))
		if _, err = tx.ExecContext(ctx, "INSERT INTO format(id,check_value) VALUES(1,?)", check); err != nil {
			return nil, fail
		}
		if err = tx.Commit(); err != nil {
			return nil, fail
		}
	}
	var check []byte
	if err = db.QueryRowContext(ctx, "SELECT check_value FROM format WHERE id=1").Scan(&check); err != nil {
		return nil, fail
	}
	plain, err := s.open(check, []byte("format"))
	if err != nil || string(plain) != "jobdb-dev-secrets-v1" {
		return nil, fail
	}
	ok = true
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) seal(value, aad []byte) []byte {
	nonce := make([]byte, s.aead.NonceSize())
	// crypto/rand.Read cannot fail on supported Go platforms.
	_, _ = rand.Read(nonce)
	return s.aead.Seal(nonce, nonce, value, aad)
}
func (s *Store) open(value, aad []byte) ([]byte, error) {
	n := s.aead.NonceSize()
	if len(value) < n {
		return nil, jobdb.ErrSecretResolutionFailed
	}
	return s.aead.Open(nil, value[:n], value[n:], aad)
}
func associatedData(tenant, name string) []byte {
	raw, _ := json.Marshal([]string{tenant, name})
	return raw
}

func (s *Store) Set(ctx context.Context, tenant, name, value string) error {
	if tenant == "" || name == "" || !utf8.ValidString(tenant) || !utf8.ValidString(name) || !utf8.ValidString(value) {
		return jobdb.ErrInvalidSecretRequest
	}
	ciphertext := s.seal([]byte(value), associatedData(tenant, name))
	_, err := s.db.ExecContext(ctx, `INSERT INTO secrets(tenant_id,name,value) VALUES(?,?,?) ON CONFLICT(tenant_id,name) DO UPDATE SET value=excluded.value`, tenant, name, ciphertext)
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
		var ciphertext []byte
		err := tx.QueryRowContext(ctx, "SELECT value FROM secrets WHERE tenant_id=? AND name=?", req.TenantID, name).Scan(&ciphertext)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return jobdb.SecretResolveResult{}, storeError(err)
		}
		value, err := s.open(ciphertext, associatedData(req.TenantID, name))
		if err != nil {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
		}
		size += int64(len(name)) + int64(len(value))
		if size > s.maxResponseBytes {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
		}
		result.Secrets[name] = jobdb.SecretValue{Value: string(value)}
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
