package impl

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/colony-2/pgwf-go/installer"
	"github.com/colony-2/strata/strata-go/pkg/daemon"
	"github.com/fergusstrange/embedded-postgres"
)

// InstallPGWF installs the pgwf schema into the provided Postgres instance.
// Adjust the implementation if the upstream installer lives in a different package.
func InstallPGWF(ctx context.Context, db *sql.DB) error {
	inst := installer.Installer{DB: db}
	if err := inst.Apply(ctx); err != nil {
		return err
	}
	return inst.Verify(ctx)
}

// EmbeddedStrataHandle is a lightweight handle to an embedded Strata daemon.
type EmbeddedStrataHandle struct {
	BaseURL  string
	APIKey   string
	Shutdown func()
}

// StartEmbeddedStrata spins up an embedded Strata daemon for tests.
func StartEmbeddedStrata() (*EmbeddedStrataHandle, error) {
	rowDir, err := os.MkdirTemp("", "strata-rows-*")
	if err != nil {
		return nil, fmt.Errorf("create row store dir: %w", err)
	}
	blobDir, err := os.MkdirTemp("", "strata-blobs-*")
	if err != nil {
		os.RemoveAll(rowDir)
		return nil, fmt.Errorf("create blob store dir: %w", err)
	}

	cfg := daemon.Config{
		ListenAddr:             "127.0.0.1:0",
		RowStoreURI:            fmt.Sprintf("pebble://%s", filepath.ToSlash(rowDir)),
		BlobStoreURI:           fmt.Sprintf("blobfs://%s", filepath.ToSlash(blobDir)),
		MaxInlineArtifactBytes: daemon.DefaultMaxInlineArtifactBytes,
	}

	d, err := daemon.StartEmbedded(context.Background(), cfg)
	if err != nil {
		os.RemoveAll(rowDir)
		os.RemoveAll(blobDir)
		return nil, err
	}
	addr, err := d.Addr()
	if err != nil {
		d.Shutdown(context.Background())
		os.RemoveAll(rowDir)
		os.RemoveAll(blobDir)
		return nil, err
	}

	return &EmbeddedStrataHandle{
		BaseURL: "http://" + addr,
		APIKey:  "test-token",
		Shutdown: func() {
			d.Shutdown(context.Background())
			os.RemoveAll(rowDir)
			os.RemoveAll(blobDir)
		},
	}, nil
}

// StartEmbeddedPostgres spins up an embedded Postgres with isolated runtime/data/cache and returns DSN and stop func.
func StartEmbeddedPostgres() (string, func(), error) {
	pgPort := uint32(20000 + (time.Now().UnixNano() % 1000))
	tmpDir, err := os.MkdirTemp("", "pgwf-embedded-*")
	if err != nil {
		return "", nil, fmt.Errorf("temp dir: %w", err)
	}
	runtimePath := filepath.Join(tmpDir, "runtime")
	dataPath := filepath.Join(tmpDir, "data")
	cachePath := filepath.Join(tmpDir, "cache")
	_ = os.MkdirAll(runtimePath, 0o755)
	_ = os.MkdirAll(dataPath, 0o755)
	_ = os.MkdirAll(cachePath, 0o755)

	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(pgPort).
			RuntimePath(runtimePath).
			DataPath(dataPath).
			CachePath(cachePath),
	)
	if err := postgres.Start(); err != nil {
		return "", nil, err
	}
	stop := func() {
		_ = postgres.Stop()
		_ = os.RemoveAll(tmpDir)
	}
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", pgPort)
	return dsn, stop, nil
}
