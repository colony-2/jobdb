package devsecrets

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestEncryptedStoreIsolationRotationAndReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "secrets.db")
	key := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(key, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))), 0600))
	s, err := Open(ctx, path, key, jobdb.SecretLimits{})
	require.NoError(t, err)
	const secret = "credential-that-must-never-be-written-in-plaintext\n"
	require.NoError(t, s.Set(ctx, "a", "token", secret))
	require.NoError(t, s.Set(ctx, "b", "token", "other"))
	require.NoError(t, s.Set(ctx, "a", "empty", ""))
	result, err := s.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "a", Names: []string{"token", "empty", "missing"}})
	require.NoError(t, err)
	require.Len(t, result.Secrets, 2)
	require.Equal(t, secret, result.Secrets["token"].Value)
	var first, second []byte
	require.NoError(t, s.db.QueryRow("SELECT value FROM secrets WHERE tenant_id='a' AND name='token'").Scan(&first))
	require.NoError(t, s.Set(ctx, "a", "token", secret))
	require.NoError(t, s.db.QueryRow("SELECT value FROM secrets WHERE tenant_id='a' AND name='token'").Scan(&second))
	require.NotEqual(t, first, second)
	// A ciphertext copied to another tenant is not decryptable there.
	_, err = s.db.Exec("UPDATE secrets SET value=? WHERE tenant_id='b'", first)
	require.NoError(t, err)
	result, err = s.ResolveSecrets(ctx, jobdb.SecretResolveRequest{TenantID: "b", Names: []string{"token"}})
	require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
	require.Empty(t, result.Secrets)
	require.NoError(t, s.Close())
	files, err := filepath.Glob(path + "*")
	require.NoError(t, err)
	for _, file := range files {
		raw, err := os.ReadFile(file)
		require.NoError(t, err)
		require.NotContains(t, string(raw), secret)
	}
	s, err = Open(ctx, path, key, jobdb.SecretLimits{})
	require.NoError(t, err)
	defer s.Close()
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
	require.NoError(t, os.WriteFile(key, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32))), 0600))
	_, err = Open(ctx, path, key, jobdb.SecretLimits{})
	require.Error(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func TestEmptyStoreRejectsWrongOrMissingKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.db")
	key := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(key, []byte(base64.StdEncoding.EncodeToString(make([]byte, 32))), 0600))
	s, err := Open(context.Background(), path, key, jobdb.SecretLimits{})
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.NoError(t, os.WriteFile(key, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))), 0600))
	_, err = Open(context.Background(), path, key, jobdb.SecretLimits{})
	require.Error(t, err)
	_, err = Open(context.Background(), path, key+"missing", jobdb.SecretLimits{})
	require.Error(t, err)
}

func TestStoreBoundsBatchOutput(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "key")
	require.NoError(t, os.WriteFile(key, []byte(base64.StdEncoding.EncodeToString(make([]byte, 32))), 0600))
	s, err := Open(context.Background(), filepath.Join(dir, "store.db"), key, jobdb.SecretLimits{MaxResponseBytes: 128})
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.Set(context.Background(), "tenant", "one", string(bytes.Repeat([]byte{'x'}, 80))))
	require.NoError(t, s.Set(context.Background(), "tenant", "two", string(bytes.Repeat([]byte{'y'}, 80))))
	result, err := s.ResolveSecrets(context.Background(), jobdb.SecretResolveRequest{TenantID: "tenant", Names: []string{"one", "two"}})
	require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
	require.Empty(t, result.Secrets)
}
