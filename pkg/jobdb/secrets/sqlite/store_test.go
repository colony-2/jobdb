package sqlitesecrets

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestStoreIsolationRotationAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "nested", "secrets ?#.db")
	s, err := Open(ctx, path, jobdb.SecretLimits{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	const secret = "development-credential\n"
	require.NoError(t, s.Set(ctx, "a", "token", secret))
	require.NoError(t, s.Set(ctx, "b", "token", "other"))
	require.NoError(t, s.Set(ctx, "a", "empty", ""))
	result, err := s.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "a", Names: []string{"token", "empty", "missing"}})
	require.NoError(t, err)
	require.Len(t, result.Secrets, 2)
	require.Equal(t, secret, result.Secrets["token"].Value)
	require.Contains(t, result.Secrets, "empty")
	require.Empty(t, result.Secrets["empty"].Value)
	var stored string
	require.NoError(t, s.db.QueryRow("SELECT value FROM secrets WHERE tenant_id='a' AND name='token'").Scan(&stored))
	require.Equal(t, secret, stored)
	require.NoError(t, s.Close())
	s, err = Open(ctx, path, jobdb.SecretLimits{})
	require.NoError(t, err)
	result, err = s.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "a", Names: []string{"token"}})
	require.NoError(t, err)
	require.Equal(t, secret, result.Secrets["token"].Value)
	require.NoError(t, s.Set(ctx, "a", "token", "rotated"))
	result, err = s.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "a", Names: []string{"token"}})
	require.NoError(t, err)
	require.Equal(t, "rotated", result.Secrets["token"].Value)
	require.NoError(t, s.Remove(ctx, "a", "token"))
	require.NoError(t, s.Remove(ctx, "a", "token"))
	result, err = s.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "a", Names: []string{"token"}})
	require.NoError(t, err)
	require.Empty(t, result.Secrets)
	result, err = s.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "b", Names: []string{"token", "empty"}})
	require.NoError(t, err)
	require.Equal(t, map[string]jobdb.SecretValue{"token": {Value: "other"}}, result.Secrets)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func TestStoreInitializesExistingEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	s, err := Open(context.Background(), path, jobdb.SecretLimits{})
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestStoreRejectsLegacyEncryptionWithoutChangingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE format (id INTEGER PRIMARY KEY, check_value BLOB); INSERT INTO format VALUES (1, x'010203')")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	s, err := Open(context.Background(), path, jobdb.SecretLimits{})
	require.ErrorContains(t, err, "encrypted development secret stores are no longer supported")
	require.Nil(t, s)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestStoreBoundsBatchOutput(t *testing.T) {
	s, err := Open(context.Background(), ":memory:", jobdb.SecretLimits{MaxResponseBytes: 128})
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.Set(context.Background(), "tenant", "one", string(bytes.Repeat([]byte{'x'}, 80))))
	require.NoError(t, s.Set(context.Background(), "tenant", "two", string(bytes.Repeat([]byte{'y'}, 80))))
	result, err := s.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{TenantID: "tenant", Names: []string{"one", "two"}})
	require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
	require.Empty(t, result.Secrets)
}
