package postgres_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/colony-2/jobdb/pkg/internal/directtestsupport"
	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	schemapostgres "github.com/colony-2/jobdb/pkg/jobdb/schemastore/postgres"
)

func TestSchemaStoreComposesWithRuntimeCore(t *testing.T) {
	ctx := context.Background()
	dsn, stop, err := directtestsupport.StartEmbeddedPostgres()
	if err != nil {
		t.Fatalf("start Postgres: %v", err)
	}
	t.Cleanup(stop)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open Postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := schemapostgres.NewSQLDB(ctx, db)
	if err != nil {
		t.Fatalf("new schema store: %v", err)
	}
	registry, err := runtimecore.NewSchemaRegistry(runtimecore.SchemaRegistryConfig{Store: store})
	if err != nil {
		t.Fatalf("new schema registry: %v", err)
	}
	info, err := registry.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
		TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":true}`),
	})
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}
	listed, err := registry.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{TenantId: "tenant"})
	if err != nil || len(listed.Schemas) != 1 || listed.Schemas[0].SchemaHash != info.SchemaHash {
		t.Fatalf("active schemas = %+v, err = %v", listed, err)
	}
	key := jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: info.SchemaHash}
	archived, err := registry.ArchiveJobSchema(ctx, key)
	if err != nil || archived.State != jobdb.JobSchemaStateArchived {
		t.Fatalf("archive schema = %+v, err = %v", archived, err)
	}
	listed, err = registry.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{TenantId: "tenant"})
	if err != nil || len(listed.Schemas) != 0 {
		t.Fatalf("active schemas after archive = %+v, err = %v", listed, err)
	}
	if _, err := registry.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: "sha256:" + strings.Repeat("0", 64)}); !errors.Is(err, jobdb.ErrJobSchemaNotFound) {
		t.Fatalf("missing schema = %v, want schema not found", err)
	}
}
