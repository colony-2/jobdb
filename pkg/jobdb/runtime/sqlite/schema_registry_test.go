package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestSchemaRegistryLifecycle(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	schema := []byte(`{"type":"object","properties":{"ordinal":{"type":"integer"}}}`)
	registered, err := embedded.Runtime.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
		TenantId: "tenant-schema",
		Schema:   schema,
	})
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}
	got, err := embedded.Runtime.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant-schema", SchemaHash: registered.SchemaHash})
	if err != nil {
		t.Fatalf("get schema: %v", err)
	}
	if got.SchemaHash != registered.SchemaHash || got.State != jobdb.JobSchemaStateActive {
		t.Fatalf("unexpected schema: %+v", got)
	}
	list, err := embedded.Runtime.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{TenantId: "tenant-schema"})
	if err != nil {
		t.Fatalf("list schemas: %v", err)
	}
	if len(list.Schemas) != 1 {
		t.Fatalf("active schemas = %d, want 1", len(list.Schemas))
	}
	archived, err := embedded.Runtime.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant-schema", SchemaHash: registered.SchemaHash})
	if err != nil {
		t.Fatalf("archive schema: %v", err)
	}
	if archived.State != jobdb.JobSchemaStateArchived || archived.ArchivedAt == nil {
		t.Fatalf("schema was not archived: %+v", archived)
	}
	list, err = embedded.Runtime.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{TenantId: "tenant-schema"})
	if err != nil {
		t.Fatalf("list active schemas after archive: %v", err)
	}
	if len(list.Schemas) != 0 {
		t.Fatalf("active schemas after archive = %d, want 0", len(list.Schemas))
	}
}

func TestSchemaRegistryMissing(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	errHash := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	_, err = embedded.Runtime.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant-schema", SchemaHash: errHash})
	if !errors.Is(err, jobdb.ErrJobSchemaNotFound) {
		t.Fatalf("get missing schema error = %v, want ErrJobSchemaNotFound", err)
	}
}
