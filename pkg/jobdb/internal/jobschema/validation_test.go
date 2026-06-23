package jobschema

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestValidatorCacheIsKeyedBySchemaHash(t *testing.T) {
	oldValidator := defaultValidator
	defaultValidator = &validatorCache{schemas: make(map[string]*jsonschema.Schema)}
	defer func() {
		defaultValidator = oldValidator
	}()

	rawSchema := json.RawMessage(`{"type":"object","required":["body"]}`)
	hash, canonical, err := jobdb.JobSchemaHash(rawSchema)
	if err != nil {
		t.Fatalf("compute schema hash: %v", err)
	}
	registry := &countingRegistry{schemaHash: hash, schema: canonical}
	chapter := jobdb.Chapter{
		Ordinal:   0,
		TaskType:  "schema-cache-test",
		CreatedAt: time.Now().UTC(),
		Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{
			Data: []byte(`{"kind":"schema-cache-test"}`),
		}},
	}

	if err := ValidateChapter(context.Background(), registry, jobdb.JobSchemaKey{
		TenantId:   "tenant-a",
		SchemaHash: hash,
	}, chapter); err != nil {
		t.Fatalf("validate tenant-a: %v", err)
	}
	if err := ValidateChapter(context.Background(), registry, jobdb.JobSchemaKey{
		TenantId:   "tenant-b",
		SchemaHash: hash,
	}, chapter); err != nil {
		t.Fatalf("validate tenant-b: %v", err)
	}
	if registry.gets != 1 {
		t.Fatalf("schema registry gets = %d, want 1", registry.gets)
	}
}

type countingRegistry struct {
	schemaHash string
	schema     json.RawMessage
	gets       int
}

func (r *countingRegistry) RegisterJobSchema(context.Context, jobdb.RegisterJobSchemaRequest) (jobdb.JobSchemaInfo, error) {
	panic("unexpected RegisterJobSchema")
}

func (r *countingRegistry) GetJobSchema(_ context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	r.gets++
	return jobdb.JobSchemaInfo{
		TenantId:   key.TenantId,
		SchemaHash: r.schemaHash,
		Schema:     append(json.RawMessage(nil), r.schema...),
		State:      jobdb.JobSchemaStateActive,
		CreatedAt:  time.Now().UTC(),
	}, nil
}

func (r *countingRegistry) ListJobSchemas(context.Context, jobdb.ListJobSchemasRequest) (jobdb.ListJobSchemasResponse, error) {
	panic("unexpected ListJobSchemas")
}

func (r *countingRegistry) ArchiveJobSchema(context.Context, jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	panic("unexpected ArchiveJobSchema")
}
