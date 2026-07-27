package toyimpl

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

var _ jobdb.JobSchemaRegistry = (*Runtime)(nil)

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
	if r == nil || r.engine == nil {
		return nil, fmt.Errorf("toy runtime is required")
	}
	return runtimecore.NewSchemaRegistry(runtimecore.SchemaRegistryConfig{
		Store: toySchemaStore{runtime: r},
	})
}

type toySchemaStore struct {
	runtime *Runtime
}

func (s toySchemaStore) StoreJobSchema(ctx context.Context, schema runtimecore.StoredJobSchema) (runtimecore.StoredJobSchema, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
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
	key := jobdb.JobSchemaKey{TenantId: schema.TenantId, SchemaHash: schema.SchemaHash}

	s.runtime.engine.mu.Lock()
	defer s.runtime.engine.mu.Unlock()
	if existing := s.runtime.engine.schemas[key]; existing != nil {
		return storedJobSchemaFromInfo(existing.info), nil
	}
	info := jobdb.JobSchemaInfo{
		TenantId:   schema.TenantId,
		SchemaHash: schema.SchemaHash,
		Schema:     cloneJSON(schema.Schema),
		State:      state,
		CreatedAt:  createdAt,
		ArchivedAt: cloneTime(schema.ArchivedAt),
	}
	s.runtime.engine.schemas[key] = &toySchemaRecord{info: info}
	return storedJobSchemaFromInfo(info), nil
}

func (s toySchemaStore) GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (runtimecore.StoredJobSchema, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	s.runtime.engine.mu.Lock()
	defer s.runtime.engine.mu.Unlock()
	record := s.runtime.engine.schemas[key]
	if record == nil {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	return storedJobSchemaFromInfo(record.info), nil
}

func (s toySchemaStore) ListJobSchemas(ctx context.Context, req runtimecore.ListJobSchemasRequest) (runtimecore.ListJobSchemasResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return runtimecore.ListJobSchemasResponse{}, err
	}
	state := req.State
	if state == "" {
		state = jobdb.JobSchemaListStateActive
	}

	s.runtime.engine.mu.Lock()
	defer s.runtime.engine.mu.Unlock()
	out := make([]runtimecore.StoredJobSchema, 0)
	for key, record := range s.runtime.engine.schemas {
		if key.TenantId != req.TenantId {
			continue
		}
		if !schemaListStateMatches(record.info.State, state) {
			continue
		}
		out = append(out, storedJobSchemaFromInfo(record.info))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].SchemaHash < out[j].SchemaHash
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return runtimecore.ListJobSchemasResponse{Schemas: out}, nil
}

func (s toySchemaStore) ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey, archivedAt time.Time) (runtimecore.StoredJobSchema, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return runtimecore.StoredJobSchema{}, err
	}
	s.runtime.engine.mu.Lock()
	defer s.runtime.engine.mu.Unlock()
	record := s.runtime.engine.schemas[key]
	if record == nil {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	if record.info.ArchivedAt == nil {
		record.info.ArchivedAt = cloneTime(&archivedAt)
	}
	record.info.State = jobdb.JobSchemaStateArchived
	return storedJobSchemaFromInfo(record.info), nil
}

func schemaListStateMatches(state jobdb.JobSchemaState, filter jobdb.JobSchemaListState) bool {
	switch filter {
	case jobdb.JobSchemaListStateAll:
		return true
	case jobdb.JobSchemaListStateArchived:
		return state == jobdb.JobSchemaStateArchived
	default:
		return state == jobdb.JobSchemaStateActive
	}
}

func storedJobSchemaFromInfo(info jobdb.JobSchemaInfo) runtimecore.StoredJobSchema {
	return runtimecore.StoredJobSchema{
		TenantId:   info.TenantId,
		SchemaHash: info.SchemaHash,
		Schema:     cloneJSON(info.Schema),
		State:      info.State,
		CreatedAt:  info.CreatedAt.UTC(),
		ArchivedAt: cloneTime(info.ArchivedAt),
	}
}
