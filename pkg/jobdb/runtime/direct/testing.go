package direct

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/directtestsupport"
)

type EmbeddedRuntime struct {
	Runtime *Runtime
	stopPG  func()
	blobDir string
}

func (e *EmbeddedRuntime) Shutdown() {
	if e == nil {
		return
	}
	e.stopPG()
	if e.blobDir != "" {
		_ = os.RemoveAll(e.blobDir)
	}
}

func StartEmbeddedRuntime(ctx context.Context) (*EmbeddedRuntime, error) {
	dsn, stopPG, err := directtestsupport.StartEmbeddedPostgres()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		stopPG()
		return nil, err
	}
	cleanup := func() {
		_ = db.Close()
		stopPG()
	}

	setupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := directtestsupport.InstallPGWF(setupCtx, db); err != nil {
		cleanup()
		return nil, err
	}
	blobDir, err := os.MkdirTemp("", "jobdb-direct-blobs-*")
	if err != nil {
		cleanup()
		return nil, err
	}

	rt, err := NewFromConfig(Config{
		PostgresDSN:  dsn,
		BlobStoreURI: fmt.Sprintf("blobfs://%s", filepath.ToSlash(blobDir)),
	})
	if err != nil {
		_ = os.RemoveAll(blobDir)
		cleanup()
		return nil, err
	}

	return &EmbeddedRuntime{
		Runtime: rt,
		stopPG:  cleanup,
		blobDir: blobDir,
	}, nil
}
