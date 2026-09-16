// Package direct is the compatibility entry point for JobDB's Postgres
// runtime. The implementation lives in the optional pgjobdb/runtime module.
package direct

import (
	"context"
	"fmt"
	"log/slog"

	pgjobdbruntime "github.com/colony-2/pgjobdb/runtime"
	"gorm.io/gorm"
)

type Runtime = pgjobdbruntime.Runtime

// Config describes a direct Postgres-backed JobDB runtime.
type Config struct {
	PostgresDSN string
	// BlobStoreURI is a blob bucket URL for large chapter artifacts.
	// blobfs:// is supported by default. Other schemes require an explicit provider import.
	BlobStoreURI           string
	MaxInlineArtifactBytes int64
	Logger                 *slog.Logger
}

// New wraps a caller-owned Gorm database with the pgjobdb runtime.
func New(db *gorm.DB, cfg Config) (*Runtime, error) {
	if db == nil {
		return nil, fmt.Errorf("db is required")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	return pgjobdbruntime.New(context.Background(), sqlDB, cfg.toPgjobdb())
}

// NewFromConfig opens a Postgres connection owned by the returned runtime.
func NewFromConfig(cfg Config) (*Runtime, error) {
	return pgjobdbruntime.OpenDSN(context.Background(), cfg.PostgresDSN, cfg.toPgjobdb())
}

func (c Config) toPgjobdb() pgjobdbruntime.Config {
	return pgjobdbruntime.Config{
		BlobStoreURI:           c.BlobStoreURI,
		MaxInlineArtifactBytes: c.MaxInlineArtifactBytes,
		Logger:                 c.Logger,
	}
}
