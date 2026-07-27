package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

var _ jobdb.JobSchemaRegistry = (*Runtime)(nil)

type schemaRow struct {
	tenantID     string
	schemaHash   string
	schemaJSON   []byte
	state        string
	createdAtNS  int64
	archivedAtNS sql.NullInt64
}

func scanSchemaRow(scanner interface{ Scan(dest ...any) error }) (schemaRow, error) {
	var row schemaRow
	var schemaJSON []byte
	if err := scanner.Scan(
		&row.tenantID,
		&row.schemaHash,
		&schemaJSON,
		&row.state,
		&row.createdAtNS,
		&row.archivedAtNS,
	); err != nil {
		return schemaRow{}, err
	}
	row.schemaJSON = cloneBytes(schemaJSON)
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
		Store: sqliteSchemaStore{runtime: r},
		Now:   timeNowUTC,
	})
}

type sqliteSchemaStore struct {
	runtime *Runtime
}

func (s sqliteSchemaStore) StoreJobSchema(ctx context.Context, schema runtimecore.StoredJobSchema) (runtimecore.StoredJobSchema, error) {
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
		createdAt = timeNowUTC()
	}
	err := r.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO jobdb_schemas (
	tenant_id, schema_hash, schema_json, state, created_at_ns
) VALUES (?, ?, ?, ?, ?)`,
			schema.TenantId, schema.SchemaHash, cloneJSON(schema.Schema), string(state), timeToNS(createdAt))
		return err
	})
	if err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	return s.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: schema.TenantId, SchemaHash: schema.SchemaHash})
}

func (s sqliteSchemaStore) GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (runtimecore.StoredJobSchema, error) {
	r := s.runtime
	if err := r.validate(); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	row, err := scanSchemaRow(r.db.QueryRowContext(ctx, `
SELECT tenant_id, schema_hash, schema_json, state, created_at_ns, archived_at_ns
FROM jobdb_schemas
WHERE tenant_id = ? AND schema_hash = ?`, key.TenantId, key.SchemaHash))
	if err == sql.ErrNoRows {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	if err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	return storedJobSchemaFromRow(row), nil
}

func (s sqliteSchemaStore) ListJobSchemas(ctx context.Context, req runtimecore.ListJobSchemasRequest) (runtimecore.ListJobSchemasResponse, error) {
	r := s.runtime
	if err := r.validate(); err != nil {
		return runtimecore.ListJobSchemasResponse{}, err
	}
	query := `
SELECT tenant_id, schema_hash, schema_json, state, created_at_ns, archived_at_ns
FROM jobdb_schemas
WHERE tenant_id = ?`
	args := []any{req.TenantId}
	switch req.State {
	case jobdb.JobSchemaListStateActive:
		query += ` AND state = ?`
		args = append(args, string(jobdb.JobSchemaStateActive))
	case jobdb.JobSchemaListStateArchived:
		query += ` AND state = ?`
		args = append(args, string(jobdb.JobSchemaStateArchived))
	case jobdb.JobSchemaListStateAll:
	}
	query += ` ORDER BY created_at_ns DESC, schema_hash ASC`
	rows, err := r.db.QueryContext(ctx, query, args...)
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

func (s sqliteSchemaStore) ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey, archivedAt time.Time) (runtimecore.StoredJobSchema, error) {
	r := s.runtime
	if err := r.validate(); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	result, err := r.db.ExecContext(ctx, `
UPDATE jobdb_schemas
SET state = ?, archived_at_ns = COALESCE(archived_at_ns, ?)
WHERE tenant_id = ? AND schema_hash = ?`,
		string(jobdb.JobSchemaStateArchived), timeToNS(archivedAt.UTC()), key.TenantId, key.SchemaHash)
	if err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	return s.GetJobSchema(ctx, key)
}

func storedJobSchemaFromRow(row schemaRow) runtimecore.StoredJobSchema {
	return runtimecore.StoredJobSchema{
		TenantId:   row.tenantID,
		SchemaHash: row.schemaHash,
		Schema:     cloneJSON(row.schemaJSON),
		State:      jobdb.JobSchemaState(row.state),
		CreatedAt:  timeFromNS(row.createdAtNS),
		ArchivedAt: nullTimeFromNS(row.archivedAtNS),
	}
}

func timeNowUTC() time.Time {
	return time.Now().UTC()
}
