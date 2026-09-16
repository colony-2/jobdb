// Package direct composes JobDB's workflow runtime with the pgjobdb Postgres
// scheduler. Applications select this package when they use Postgres.
package direct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterpostgres "github.com/colony-2/jobdb/pkg/jobdb/chapterstore/postgres"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/runtimeadapter"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb/installer"
	_ "github.com/lib/pq"
	"gorm.io/gorm"
)

// Config selects Postgres and artifact storage for the runtime.
type Config struct {
	PostgresDSN string
	// BlobStoreURI is a blob bucket URL for large chapter artifacts.
	// blobfs:// is supported by default. Other schemes require an explicit provider import.
	BlobStoreURI           string
	MaxInlineArtifactBytes int64
	Logger                 *slog.Logger
}

// Runtime is JobDB's workflow facade backed by pgjobdb's typed scheduler.
// The caller owns the database passed to New or NewSQLDB; OpenDSN owns its connection.
type Runtime struct {
	*runtimecore.Runtime
	*runtimecore.SchemaRegistry
	chapters *chapterpostgres.Store
	db       *sql.DB
	ownsDB   bool
	close    sync.Once
	closeErr error
}

var _ jobdb.WorkflowRuntime = (*Runtime)(nil)
var _ jobdb.JobSchemaRegistry = (*Runtime)(nil)

// New wraps a caller-owned Gorm database with the direct Postgres runtime.
func New(db *gorm.DB, cfg Config) (*Runtime, error) {
	if db == nil {
		return nil, fmt.Errorf("db is required")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	return NewSQLDB(context.Background(), sqlDB, cfg)
}

// NewFromConfig opens a Postgres connection owned by the returned runtime.
func NewFromConfig(cfg Config) (*Runtime, error) {
	return OpenDSN(context.Background(), cfg.PostgresDSN, cfg)
}

// NewSQLDB initializes a clean Postgres database or verifies an existing
// pgjobdb installation, then composes JobDB's public runtime core.
func NewSQLDB(ctx context.Context, db *sql.DB, cfg Config) (*Runtime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("direct runtime: context is nil")
	}
	if db == nil {
		return nil, fmt.Errorf("direct runtime: database is required")
	}
	if cfg.BlobStoreURI == "" {
		return nil, fmt.Errorf("direct runtime: blob store URI is required")
	}
	install := installer.Installer{DB: db}
	if err := install.Apply(ctx); err != nil {
		return nil, fmt.Errorf("direct runtime: install scheduler: %w", err)
	}
	if err := install.Verify(ctx); err != nil {
		return nil, fmt.Errorf("direct runtime: verify scheduler: %w", err)
	}
	chapters, err := chapterpostgres.NewSQLDB(ctx, db, chapterpostgres.Config{
		BlobStoreURI:           cfg.BlobStoreURI,
		MaxInlineArtifactBytes: cfg.MaxInlineArtifactBytes,
		Logger:                 cfg.Logger,
	})
	if err != nil {
		return nil, err
	}
	schemas, err := schemapostgres.NewSQLDB(ctx, db)
	if err != nil {
		_ = chapters.Close(ctx)
		return nil, err
	}
	core, err := runtimecore.NewRuntime(runtimecore.Config{
		Scheduler: runtimeadapter.Scheduler{DB: db}, Chapters: chapters, Schemas: schemas,
	})
	if err != nil {
		_ = chapters.Close(ctx)
		return nil, err
	}
	registry, err := runtimecore.NewSchemaRegistry(runtimecore.SchemaRegistryConfig{Store: schemas})
	if err != nil {
		_ = chapters.Close(ctx)
		return nil, err
	}
	return &Runtime{Runtime: core, SchemaRegistry: registry, chapters: chapters, db: db}, nil
}

// OpenDSN opens and owns a Postgres connection.
func OpenDSN(ctx context.Context, dsn string, cfg Config) (*Runtime, error) {
	if ctx == nil {
		return nil, fmt.Errorf("direct runtime: context is nil")
	}
	if dsn == "" {
		return nil, fmt.Errorf("direct runtime: Postgres DSN is required")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	runtime, err := NewSQLDB(ctx, db, cfg)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	runtime.ownsDB = true
	return runtime, nil
}

// GetJobRun assembles the public run view from scheduler and chapter records.
func (r *Runtime) GetJobRun(ctx context.Context, req jobdb.GetJobRunRequest) (jobdb.GetJobRunResponse, error) {
	return jobdb.GetJobRun(ctx, r, req)
}

// Close releases artifact storage and a connection opened by OpenDSN.
func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.close.Do(func() {
		var errs []error
		if r.chapters != nil {
			errs = append(errs, r.chapters.Close(ctx))
		}
		if r.ownsDB && r.db != nil {
			errs = append(errs, r.db.Close())
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}
