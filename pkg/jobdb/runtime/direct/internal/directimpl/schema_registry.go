package directimpl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

const jobSchemaSQL = `
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

var _ jobdb.JobSchemaRegistry = (*Runtime)(nil)

type schemaRow struct {
	tenantID   string
	schemaHash string
	schemaJSON json.RawMessage
	state      string
	createdAt  time.Time
	archivedAt sql.NullTime
}

type schemaContextDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (r *Runtime) schemaDB(ctx context.Context) schemaContextDB {
	if tx := r.sqlTxFromCtx(ctx); tx != nil {
		return tx
	}
	return r.udb
}

func migrateJobSchemas(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is required")
	}
	if _, err := db.ExecContext(ctx, jobSchemaSQL); err != nil {
		return fmt.Errorf("migrate schemas: %w", err)
	}
	return nil
}

func scanSchemaRow(scanner interface{ Scan(dest ...any) error }) (schemaRow, error) {
	var row schemaRow
	var schemaJSON []byte
	if err := scanner.Scan(
		&row.tenantID,
		&row.schemaHash,
		&schemaJSON,
		&row.state,
		&row.createdAt,
		&row.archivedAt,
	); err != nil {
		return schemaRow{}, err
	}
	row.schemaJSON = append(json.RawMessage(nil), schemaJSON...)
	return row, nil
}

func (r *Runtime) RegisterJobSchema(ctx context.Context, req jobdb.RegisterJobSchemaRequest) (jobdb.JobSchemaInfo, error) {
	registry, err := r.coreSchemaRegistry()
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	return registry.RegisterJobSchema(ctx, req)
}

func (r *Runtime) GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	registry, err := r.coreSchemaRegistry()
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	return registry.GetJobSchema(ctx, key)
}

func (r *Runtime) ListJobSchemas(ctx context.Context, req jobdb.ListJobSchemasRequest) (jobdb.ListJobSchemasResponse, error) {
	registry, err := r.coreSchemaRegistry()
	if err != nil {
		return jobdb.ListJobSchemasResponse{}, err
	}
	return registry.ListJobSchemas(ctx, req)
}

func (r *Runtime) ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	registry, err := r.coreSchemaRegistry()
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	return registry.ArchiveJobSchema(ctx, key)
}

func (r *Runtime) coreSchemaRegistry() (*runtimecore.SchemaRegistry, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	return runtimecore.NewSchemaRegistry(runtimecore.SchemaRegistryConfig{
		Store: directSchemaStore{runtime: r},
		Now: func() time.Time {
			return time.Now().UTC()
		},
	})
}

type directSchemaStore struct {
	runtime *Runtime
}

func (s directSchemaStore) StoreJobSchema(ctx context.Context, schema runtimecore.StoredJobSchema) (runtimecore.StoredJobSchema, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r := s.runtime
	if err := r.validate(); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	state := schema.State
	if state == "" {
		state = jobdb.JobSchemaStateActive
	}
	createdAt := schema.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	if _, err := r.schemaDB(ctx).ExecContext(ctx, `
INSERT INTO jobdb_schemas (
	tenant_id, schema_hash, schema_json, state, created_at
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, schema_hash) DO NOTHING`,
		schema.TenantId, schema.SchemaHash, string(schema.Schema), string(state), createdAt); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	return s.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: schema.TenantId, SchemaHash: schema.SchemaHash})
}

func (s directSchemaStore) GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (runtimecore.StoredJobSchema, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r := s.runtime
	if err := r.validate(); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	row, err := scanSchemaRow(r.schemaDB(ctx).QueryRowContext(ctx, `
SELECT tenant_id, schema_hash, schema_json, state, created_at, archived_at
FROM jobdb_schemas
WHERE tenant_id = $1 AND schema_hash = $2`, key.TenantId, key.SchemaHash))
	if err == sql.ErrNoRows {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	if err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	return storedJobSchemaFromRow(row), nil
}

func (s directSchemaStore) ListJobSchemas(ctx context.Context, req runtimecore.ListJobSchemasRequest) (runtimecore.ListJobSchemasResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r := s.runtime
	if err := r.validate(); err != nil {
		return runtimecore.ListJobSchemasResponse{}, err
	}
	query := `
SELECT tenant_id, schema_hash, schema_json, state, created_at, archived_at
FROM jobdb_schemas
WHERE tenant_id = $1`
	args := []any{req.TenantId}
	switch req.State {
	case jobdb.JobSchemaListStateActive:
		query += ` AND state = $2`
		args = append(args, string(jobdb.JobSchemaStateActive))
	case jobdb.JobSchemaListStateArchived:
		query += ` AND state = $2`
		args = append(args, string(jobdb.JobSchemaStateArchived))
	case jobdb.JobSchemaListStateAll:
	}
	query += ` ORDER BY created_at DESC, schema_hash ASC`
	rows, err := r.schemaDB(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return runtimecore.ListJobSchemasResponse{}, err
	}
	defer rows.Close()
	out := make([]runtimecore.StoredJobSchema, 0)
	for rows.Next() {
		row, err := scanSchemaRow(rows)
		if err != nil {
			return runtimecore.ListJobSchemasResponse{}, err
		}
		out = append(out, storedJobSchemaFromRow(row))
	}
	if err := rows.Err(); err != nil {
		return runtimecore.ListJobSchemasResponse{}, err
	}
	return runtimecore.ListJobSchemasResponse{Schemas: out}, nil
}

func (s directSchemaStore) ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey, archivedAt time.Time) (runtimecore.StoredJobSchema, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r := s.runtime
	if err := r.validate(); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	row, err := scanSchemaRow(r.schemaDB(ctx).QueryRowContext(ctx, `
UPDATE jobdb_schemas
SET state = $3, archived_at = COALESCE(archived_at, $4)
WHERE tenant_id = $1 AND schema_hash = $2
RETURNING tenant_id, schema_hash, schema_json, state, created_at, archived_at`,
		key.TenantId, key.SchemaHash, string(jobdb.JobSchemaStateArchived), archivedAt.UTC()))
	if err == sql.ErrNoRows {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	if err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	return storedJobSchemaFromRow(row), nil
}

func storedJobSchemaFromRow(row schemaRow) runtimecore.StoredJobSchema {
	out := runtimecore.StoredJobSchema{
		TenantId:   row.tenantID,
		SchemaHash: row.schemaHash,
		Schema:     jobdb.NormalizeJSON(row.schemaJSON),
		State:      jobdb.JobSchemaState(row.state),
		CreatedAt:  row.createdAt.UTC(),
	}
	if row.archivedAt.Valid {
		archivedAt := row.archivedAt.Time.UTC()
		out.ArchivedAt = &archivedAt
	}
	return out
}
