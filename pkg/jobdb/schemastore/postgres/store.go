// Package postgres persists JobDB schema records in PostgreSQL for runtime
// implementations that compose the public runtime core.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS jobdb_schemas (
	tenant_id TEXT NOT NULL,
	schema_hash TEXT NOT NULL,
	schema_json JSONB NOT NULL,
	state TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	archived_at TIMESTAMPTZ,
	PRIMARY KEY (tenant_id, schema_hash)
);

CREATE INDEX IF NOT EXISTS jobdb_schemas_list_idx
	ON jobdb_schemas (tenant_id, state, created_at DESC, schema_hash ASC);
`

// Store implements runtimecore.SchemaStore using a caller-owned database.
type Store struct {
	db *sql.DB
}

var _ runtimecore.SchemaStore = (*Store)(nil)

// NewSQLDB migrates the schema table and wraps a caller-owned database.
func NewSQLDB(ctx context.Context, db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres schema store: db is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return nil, fmt.Errorf("postgres schema store: migrate: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) StoreJobSchema(ctx context.Context, schema runtimecore.StoredJobSchema) (runtimecore.StoredJobSchema, error) {
	ctx = contextOrBackground(ctx)
	state := schema.State
	if state == "" {
		state = jobdb.JobSchemaStateActive
	}
	createdAt := schema.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO jobdb_schemas (tenant_id, schema_hash, schema_json, state, created_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, schema_hash) DO NOTHING`,
		schema.TenantId, schema.SchemaHash, string(schema.Schema), string(state), createdAt); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	return s.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: schema.TenantId, SchemaHash: schema.SchemaHash})
}

func (s *Store) GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (runtimecore.StoredJobSchema, error) {
	ctx = contextOrBackground(ctx)
	row, err := scanRow(s.db.QueryRowContext(ctx, `
SELECT tenant_id, schema_hash, schema_json, state, created_at, archived_at
FROM jobdb_schemas
WHERE tenant_id = $1 AND schema_hash = $2`, key.TenantId, key.SchemaHash))
	if err == sql.ErrNoRows {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	return row, err
}

func (s *Store) ListJobSchemas(ctx context.Context, req runtimecore.ListJobSchemasRequest) (runtimecore.ListJobSchemasResponse, error) {
	ctx = contextOrBackground(ctx)
	query := `
SELECT tenant_id, schema_hash, schema_json, state, created_at, archived_at
FROM jobdb_schemas
WHERE tenant_id = $1`
	args := []any{req.TenantId}
	switch req.State {
	case "", jobdb.JobSchemaListStateActive:
		query += ` AND state = $2`
		args = append(args, string(jobdb.JobSchemaStateActive))
	case jobdb.JobSchemaListStateArchived:
		query += ` AND state = $2`
		args = append(args, string(jobdb.JobSchemaStateArchived))
	case jobdb.JobSchemaListStateAll:
	default:
		return runtimecore.ListJobSchemasResponse{}, fmt.Errorf("postgres schema store: unknown list state %q", req.State)
	}
	query += ` ORDER BY created_at DESC, schema_hash ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return runtimecore.ListJobSchemasResponse{}, err
	}
	defer rows.Close()
	out := make([]runtimecore.StoredJobSchema, 0)
	for rows.Next() {
		row, err := scanRow(rows)
		if err != nil {
			return runtimecore.ListJobSchemasResponse{}, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return runtimecore.ListJobSchemasResponse{}, err
	}
	return runtimecore.ListJobSchemasResponse{Schemas: out}, nil
}

func (s *Store) ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey, archivedAt time.Time) (runtimecore.StoredJobSchema, error) {
	ctx = contextOrBackground(ctx)
	row, err := scanRow(s.db.QueryRowContext(ctx, `
UPDATE jobdb_schemas
SET state = $3, archived_at = COALESCE(archived_at, $4)
WHERE tenant_id = $1 AND schema_hash = $2
RETURNING tenant_id, schema_hash, schema_json, state, created_at, archived_at`,
		key.TenantId, key.SchemaHash, string(jobdb.JobSchemaStateArchived), archivedAt.UTC()))
	if err == sql.ErrNoRows {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	return row, err
}

type rowScanner interface{ Scan(...any) error }

func scanRow(scanner rowScanner) (runtimecore.StoredJobSchema, error) {
	var out runtimecore.StoredJobSchema
	var schemaJSON []byte
	var state string
	var archivedAt sql.NullTime
	if err := scanner.Scan(&out.TenantId, &out.SchemaHash, &schemaJSON, &state, &out.CreatedAt, &archivedAt); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	out.Schema = jobdb.NormalizeJSON(json.RawMessage(schemaJSON))
	out.State = jobdb.JobSchemaState(state)
	out.CreatedAt = out.CreatedAt.UTC()
	if archivedAt.Valid {
		value := archivedAt.Time.UTC()
		out.ArchivedAt = &value
	}
	return out, nil
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
