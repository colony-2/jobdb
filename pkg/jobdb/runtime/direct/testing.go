package direct

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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
	if e.Runtime != nil {
		_ = e.Runtime.Close(context.Background())
	}
	e.stopPG()
	if e.blobDir != "" {
		_ = os.RemoveAll(e.blobDir)
	}
}

func StartEmbeddedRuntime(ctx context.Context) (*EmbeddedRuntime, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	dsn, stopPG, err := directtestsupport.StartEmbeddedPostgres()
	if err != nil {
		return nil, err
	}
	cleanup := func() {
		stopPG()
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
