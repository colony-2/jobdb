package runtimecore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobschema"
)

// SchemaStore persists canonical job schema documents and lifecycle state.
type SchemaStore interface {
	StoreJobSchema(ctx context.Context, schema StoredJobSchema) (StoredJobSchema, error)
	GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (StoredJobSchema, error)
	ListJobSchemas(ctx context.Context, req ListJobSchemasRequest) (ListJobSchemasResponse, error)
	ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey, archivedAt time.Time) (StoredJobSchema, error)
}

// StoredJobSchema is a canonical persisted schema record.
type StoredJobSchema struct {
	TenantId   string
	SchemaHash string
	Schema     json.RawMessage
	State      jobdb.JobSchemaState
	CreatedAt  time.Time
	ArchivedAt *time.Time
}

// ListJobSchemasRequest selects schema records from a SchemaStore.
type ListJobSchemasRequest struct {
	TenantId string
	State    jobdb.JobSchemaListState
}

// ListJobSchemasResponse is the SchemaStore schema list result.
type ListJobSchemasResponse struct {
	Schemas []StoredJobSchema
}

// SchemaRegistryConfig wires a validating JobDB schema registry.
type SchemaRegistryConfig struct {
	Store SchemaStore
	Now   func() time.Time
}

// SchemaRegistry validates schema documents and delegates persistence to a
// SchemaStore.
type SchemaRegistry struct {
	store SchemaStore
	now   func() time.Time
}

var _ jobdb.JobSchemaRegistry = (*SchemaRegistry)(nil)

// NewSchemaRegistry returns a jobdb.JobSchemaRegistry backed by store.
func NewSchemaRegistry(cfg SchemaRegistryConfig) (*SchemaRegistry, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("runtime core schema store is required")
	}
	return &SchemaRegistry{store: cfg.Store, now: nowFunc(cfg.Now)}, nil
}

// RegisterJobSchema canonicalizes and validates a schema before storing it.
func (r *SchemaRegistry) RegisterJobSchema(ctx context.Context, req jobdb.RegisterJobSchemaRequest) (jobdb.JobSchemaInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if req.TenantId == "" {
		return jobdb.JobSchemaInfo{}, fmt.Errorf("tenantId is required")
	}
	hash, canonical, err := jobdb.JobSchemaHash(req.Schema)
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if err := jobschema.ValidateSchemaDocument(hash, canonical); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	stored, err := r.store.StoreJobSchema(ctx, StoredJobSchema{
		TenantId:   req.TenantId,
		SchemaHash: hash,
		Schema:     cloneRaw(canonical),
		State:      jobdb.JobSchemaStateActive,
		CreatedAt:  r.now(),
	})
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if stored.SchemaHash != hash || !bytes.Equal(stored.Schema, canonical) {
		return jobdb.JobSchemaInfo{}, jobdb.ErrConflict
	}
	return jobSchemaInfoFromStored(stored), nil
}

// GetJobSchema returns one schema record.
func (r *SchemaRegistry) GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if err := key.Validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	stored, err := r.store.GetJobSchema(ctx, key)
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	return jobSchemaInfoFromStored(stored), nil
}

// ListJobSchemas returns schema records for one tenant.
func (r *SchemaRegistry) ListJobSchemas(ctx context.Context, req jobdb.ListJobSchemasRequest) (jobdb.ListJobSchemasResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.ListJobSchemasResponse{}, err
	}
	if req.TenantId == "" {
		return jobdb.ListJobSchemasResponse{}, fmt.Errorf("tenantId is required")
	}
	state := req.State
	if state == "" {
		state = jobdb.JobSchemaListStateActive
	}
	switch state {
	case jobdb.JobSchemaListStateActive, jobdb.JobSchemaListStateArchived, jobdb.JobSchemaListStateAll:
	default:
		return jobdb.ListJobSchemasResponse{}, fmt.Errorf("unknown schema state %q", req.State)
	}
	listed, err := r.store.ListJobSchemas(ctx, ListJobSchemasRequest{
		TenantId: req.TenantId,
		State:    state,
	})
	if err != nil {
		return jobdb.ListJobSchemasResponse{}, err
	}
	out := make([]jobdb.JobSchemaInfo, 0, len(listed.Schemas))
	for _, stored := range listed.Schemas {
		out = append(out, jobSchemaInfoFromStored(stored))
	}
	return jobdb.ListJobSchemasResponse{Schemas: out}, nil
}

// ArchiveJobSchema archives one schema record.
func (r *SchemaRegistry) ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if err := key.Validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	stored, err := r.store.ArchiveJobSchema(ctx, key, r.now())
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	return jobSchemaInfoFromStored(stored), nil
}

// ResolveActiveSchemaForNewJob resolves a submit-time schema selector and
// rejects missing or archived schemas.
func ResolveActiveSchemaForNewJob(ctx context.Context, registry jobdb.JobSchemaRegistry, tenantID string, selector *jobdb.JobSchemaSelector) (string, error) {
	return jobschema.ResolveActiveForNewJob(ctx, registry, tenantID, selector)
}

// ValidateSchemaDocument validates the JobDB workflow-schema document shape.
func ValidateSchemaDocument(schemaHash string, schema json.RawMessage) error {
	return jobschema.ValidateSchemaDocument(schemaHash, schema)
}

// ValidateFirstChapter validates a first chapter against a job schema.
func ValidateFirstChapter(ctx context.Context, registry jobdb.JobSchemaRegistry, key jobdb.JobSchemaKey, chapter jobdb.Chapter) error {
	return jobschema.ValidateFirstChapter(ctx, registry, key, chapter)
}

// ValidateOrdinaryChapter validates a non-final ordinary chapter against a job schema.
func ValidateOrdinaryChapter(ctx context.Context, registry jobdb.JobSchemaRegistry, key jobdb.JobSchemaKey, chapter jobdb.Chapter) error {
	return jobschema.ValidateOrdinaryChapter(ctx, registry, key, chapter)
}

// ValidateLastChapter validates a final chapter against a job schema.
func ValidateLastChapter(ctx context.Context, registry jobdb.JobSchemaRegistry, key jobdb.JobSchemaKey, chapter jobdb.Chapter) error {
	return jobschema.ValidateLastChapter(ctx, registry, key, chapter)
}

func (r *SchemaRegistry) validate() error {
	if r == nil || r.store == nil {
		return fmt.Errorf("runtime core schema registry is required")
	}
	if r.now == nil {
		r.now = nowFunc(nil)
	}
	return nil
}

func jobSchemaInfoFromStored(stored StoredJobSchema) jobdb.JobSchemaInfo {
	return jobdb.JobSchemaInfo{
		TenantId:   stored.TenantId,
		SchemaHash: stored.SchemaHash,
		Schema:     cloneRaw(stored.Schema),
		State:      stored.State,
		CreatedAt:  stored.CreatedAt.UTC(),
		ArchivedAt: cloneTime(stored.ArchivedAt),
	}
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	out := t.UTC()
	return &out
}
