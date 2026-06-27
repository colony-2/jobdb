package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/blobstore"
	sqliterowstore "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/sqlite"
	"github.com/segmentio/ksuid"

	_ "modernc.org/sqlite"
)

const (
	sqliteDriverName             = "sqlite"
	defaultRemoteLeaseDuration   = 30 * time.Second
	defaultSQLiteBusyTimeoutMS   = 5000
	defaultInlineArtifactMaxSize = 256
)

// Config describes a local SQLite-backed jobdb runtime.
type Config struct {
	// DSN is passed to modernc.org/sqlite. If empty, DBPath is used.
	DSN string
	// DBPath is the durable SQLite database path used when DSN is empty.
	DBPath string
	// BlobDir stores large chapter artifacts on local blobfs. If empty, it is derived from DBPath.
	BlobDir string
	// BlobStoreURI is a Go CDK blob bucket URL for large chapter artifacts. If set, it overrides BlobDir.
	BlobStoreURI string
	// MaxInlineArtifactBytes controls when artifacts move from rows to blobstore.
	MaxInlineArtifactBytes int64
	// Logger is used for runtime diagnostics.
	Logger *slog.Logger
	// WorkerID overrides the runtime's fallback worker id.
	WorkerID string
}

// Runtime is a SQLite WorkflowRuntime that composes scheduler rows, chapter
// rows, and Go CDK blob artifact storage.
type Runtime struct {
	db           *sql.DB
	ownsDB       bool
	chapterStore *chapterstore.Store
	logger       *slog.Logger
	workerID     string

	closeOnce sync.Once
	closeErr  error
}

var _ jobdb.WorkflowRuntime = (*Runtime)(nil)

// New builds a runtime around a caller-owned SQLite handle.
func New(db *sql.DB, opts ...Option) (*Runtime, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite runtime: db is required")
	}
	cfg := runtimeOptions{
		logger:       slog.Default(),
		blobStoreURI: defaultBlobStoreURI(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.workerID == "" {
		cfg.workerID = defaultWorkerID()
	}
	chapterStore, err := buildChapterStore(db, cfg.blobStoreURI, cfg.maxInlineArtifactBytes, cfg.logger)
	if err != nil {
		return nil, err
	}
	rt := &Runtime{
		db:           db,
		chapterStore: chapterStore,
		logger:       cfg.logger,
		workerID:     cfg.workerID,
	}
	if err := migrate(ctxOrBackground(nil), db); err != nil {
		return nil, err
	}
	return rt, nil
}

// NewFromConfig opens a SQLite database, creates a chapter store over the same
// *sql.DB rowstore, and returns the composed runtime.
func NewFromConfig(ctx context.Context, cfg Config) (*Runtime, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	dsn, dbPath, err := resolveDSN(cfg)
	if err != nil {
		return nil, err
	}
	if dbPath != "" {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return nil, fmt.Errorf("sqlite runtime: create db dir: %w", err)
		}
	}
	db, err := sql.Open(sqliteDriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite runtime: open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	cleanupDB := true
	defer func() {
		if cleanupDB {
			_ = db.Close()
		}
	}()

	if err := configureSQLite(ctx, db); err != nil {
		return nil, err
	}
	if err := migrate(ctx, db); err != nil {
		return nil, err
	}

	blobURI, err := resolveConfiguredBlobStoreURI(cfg, dbPath)
	if err != nil {
		return nil, err
	}
	chapterStore, err := buildChapterStore(db, blobURI, cfg.MaxInlineArtifactBytes, logger)
	if err != nil {
		return nil, err
	}

	workerID := cfg.WorkerID
	if workerID == "" {
		workerID = defaultWorkerID()
	}
	cleanupDB = false
	return &Runtime{
		db:           db,
		ownsDB:       true,
		chapterStore: chapterStore,
		logger:       logger,
		workerID:     workerID,
	}, nil
}

// Close releases resources owned by a runtime constructed with NewFromConfig.
func (r *Runtime) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		var errs []error
		if r.chapterStore != nil {
			errs = append(errs, r.chapterStore.Close(ctx))
		}
		if r.ownsDB && r.db != nil {
			errs = append(errs, r.db.Close())
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}

type runtimeOptions struct {
	logger                 *slog.Logger
	workerID               string
	blobStoreURI           string
	maxInlineArtifactBytes int64
}

// Option customizes New.
type Option func(*runtimeOptions)

func WithLogger(logger *slog.Logger) Option {
	return func(o *runtimeOptions) {
		if logger != nil {
			o.logger = logger
		}
	}
}

func WithWorkerID(workerID string) Option {
	return func(o *runtimeOptions) {
		o.workerID = workerID
	}
}

func WithBlobStoreURI(uri string) Option {
	return func(o *runtimeOptions) {
		if strings.TrimSpace(uri) != "" {
			o.blobStoreURI = uri
		}
	}
}

func WithMaxInlineArtifactBytes(limit int64) Option {
	return func(o *runtimeOptions) {
		o.maxInlineArtifactBytes = limit
	}
}

func (r *Runtime) validate() error {
	if r == nil {
		return fmt.Errorf("sqlite runtime is required")
	}
	if r.db == nil {
		return fmt.Errorf("sqlite runtime db is required")
	}
	if r.chapterStore == nil {
		return fmt.Errorf("sqlite runtime chapter store is required")
	}
	return nil
}

func (r *Runtime) requestWorkerID(workerID string) string {
	if workerID != "" {
		return workerID
	}
	return r.workerID
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "jobdb"
	}
	return fmt.Sprintf("%s:%d-%s", host, os.Getpid(), ksuid.New().String())
}

func buildChapterStore(db *sql.DB, blobURI string, maxInline int64, logger *slog.Logger) (*chapterstore.Store, error) {
	rows, err := sqliterowstore.New(db)
	if err != nil {
		return nil, fmt.Errorf("sqlite runtime: create chapter rowstore: %w", err)
	}
	blobs, err := blobstore.OpenURI(blobURI)
	if err != nil {
		return nil, fmt.Errorf("sqlite runtime: create chapter blobstore: %w", err)
	}
	store, err := chapterstore.New(rows, blobs, chapterstore.Config{
		MaxInlineArtifactBytes: maxInline,
		Logger:                 logger,
	})
	if err != nil {
		return nil, fmt.Errorf("sqlite runtime: create chapter store: %w", err)
	}
	return store, nil
}

func resolveConfiguredBlobStoreURI(cfg Config, dbPath string) (string, error) {
	if strings.TrimSpace(cfg.BlobStoreURI) != "" {
		return cfg.BlobStoreURI, nil
	}
	blobDir, err := resolveBlobDir(cfg, dbPath)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		return "", fmt.Errorf("sqlite runtime: create blob dir: %w", err)
	}
	return "blobfs://" + filepath.ToSlash(blobDir), nil
}

func defaultBlobStoreURI() string {
	path, err := filepath.Abs("jobdb-blobs")
	if err != nil {
		path = "jobdb-blobs"
	}
	return "blobfs://" + filepath.ToSlash(path)
}

func resolveDSN(cfg Config) (dsn string, dbPath string, err error) {
	if strings.TrimSpace(cfg.DSN) != "" {
		return cfg.DSN, "", nil
	}
	path := strings.TrimSpace(cfg.DBPath)
	if path == "" {
		path = "jobdb.db"
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", fmt.Errorf("sqlite runtime: resolve db path: %w", err)
	}
	return abs, abs, nil
}

func resolveBlobDir(cfg Config, dbPath string) (string, error) {
	if strings.TrimSpace(cfg.BlobDir) != "" {
		return filepath.Abs(cfg.BlobDir)
	}
	if dbPath != "" {
		return dbPath + ".blobs", nil
	}
	return filepath.Abs("jobdb.blobs")
}

func configureSQLite(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		"PRAGMA foreign_keys = ON",
		fmt.Sprintf("PRAGMA busy_timeout = %d", defaultSQLiteBusyTimeoutMS),
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("sqlite runtime: %s: %w", pragma, err)
		}
	}
	return nil
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
