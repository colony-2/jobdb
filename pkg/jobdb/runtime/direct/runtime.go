package direct

import (
	"log/slog"

	directimpl "github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/directimpl"
	"gorm.io/gorm"
)

type Runtime = directimpl.Runtime

// Config describes a direct Postgres-backed JobDB runtime.
type Config struct {
	PostgresDSN string
	// BlobStoreURI is a Go CDK blob bucket URL for large chapter artifacts.
	BlobStoreURI           string
	MaxInlineArtifactBytes int64
	Logger                 *slog.Logger
}

func New(db *gorm.DB, cfg Config) (*Runtime, error) {
	return directimpl.New(db, cfg.toImpl())
}

func NewFromConfig(cfg Config) (*Runtime, error) {
	return directimpl.NewFromConfig(cfg.toImpl())
}

func (c Config) toImpl() directimpl.Config {
	return directimpl.Config{
		PostgresDSN:            c.PostgresDSN,
		BlobStoreURI:           c.BlobStoreURI,
		MaxInlineArtifactBytes: c.MaxInlineArtifactBytes,
		Logger:                 c.Logger,
	}
}
