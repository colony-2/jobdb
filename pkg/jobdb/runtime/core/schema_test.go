package runtimecore_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

func TestSchemaRegistryCanonicalizesValidatesAndStores(t *testing.T) {
	store := newMemorySchemaStore()
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	registry, err := runtimecore.NewSchemaRegistry(runtimecore.SchemaRegistryConfig{
		Store: store,
		Now:   func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("new schema registry: %v", err)
	}

	info, err := registry.RegisterJobSchema(context.Background(), jobdb.RegisterJobSchemaRequest{
		TenantId: "tenant-a",
		Schema:   json.RawMessage(`{"description":"test","chapterShape":true}`),
	})
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}
	wantHash, wantCanonical, err := jobdb.JobSchemaHash(json.RawMessage(`{"description":"test","chapterShape":true}`))
	if err != nil {
		t.Fatalf("hash schema: %v", err)
	}
	if info.SchemaHash != wantHash {
		t.Fatalf("schema hash = %q, want %q", info.SchemaHash, wantHash)
	}
	if string(info.Schema) != string(wantCanonical) {
		t.Fatalf("schema = %s, want %s", info.Schema, wantCanonical)
	}
	if !info.CreatedAt.Equal(now) {
		t.Fatalf("createdAt = %s, want %s", info.CreatedAt, now)
	}

	if _, err := registry.RegisterJobSchema(context.Background(), jobdb.RegisterJobSchemaRequest{
		TenantId: "tenant-a",
		Schema:   json.RawMessage(`{"unknown":true,"chapterShape":true}`),
	}); !errors.Is(err, jobdb.ErrJobSchemaValidation) {
		t.Fatalf("invalid schema error = %v, want ErrJobSchemaValidation", err)
	}
}

func TestResolveActiveSchemaForNewJobRejectsArchived(t *testing.T) {
	store := newMemorySchemaStore()
	registry, err := runtimecore.NewSchemaRegistry(runtimecore.SchemaRegistryConfig{Store: store})
	if err != nil {
		t.Fatalf("new schema registry: %v", err)
	}
	ctx := context.Background()
	hash, err := runtimecore.ResolveActiveSchemaForNewJob(ctx, registry, "tenant-a", &jobdb.JobSchemaSelector{
		Schema: json.RawMessage(`{"chapterShape":true}`),
	})
	if err != nil {
		t.Fatalf("resolve inline schema: %v", err)
	}
	if hash == "" {
		t.Fatal("expected resolved schema hash")
	}
	if _, err := registry.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{TenantId: "tenant-a", SchemaHash: hash}); err != nil {
		t.Fatalf("archive schema: %v", err)
	}
	_, err = runtimecore.ResolveActiveSchemaForNewJob(ctx, registry, "tenant-a", &jobdb.JobSchemaSelector{Hash: hash})
	if !errors.Is(err, jobdb.ErrJobSchemaArchived) {
		t.Fatalf("resolve archived schema error = %v, want ErrJobSchemaArchived", err)
	}
}

func TestValidateChapterUsesRegisteredSchema(t *testing.T) {
	store := newMemorySchemaStore()
	registry, err := runtimecore.NewSchemaRegistry(runtimecore.SchemaRegistryConfig{Store: store})
	if err != nil {
		t.Fatalf("new schema registry: %v", err)
	}
	ctx := context.Background()
	info, err := registry.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
		TenantId: "tenant-a",
		Schema: json.RawMessage(`{
			"chapterShape": {
				"type": "object",
				"required": ["taskType"],
				"properties": {"taskType": {"const": "allowed"}}
			}
		}`),
	})
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}

	err = runtimecore.ValidateOrdinaryChapter(ctx, registry, jobdb.JobSchemaKey{
		TenantId:   "tenant-a",
		SchemaHash: info.SchemaHash,
	}, jobdb.Chapter{
		Ordinal:   1,
		TaskType:  "rejected",
		InputHash: "input",
		CreatedAt: time.Now().UTC(),
		Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
		}},
	})
	if !errors.Is(err, jobdb.ErrJobSchemaValidation) {
		t.Fatalf("validation error = %v, want ErrJobSchemaValidation", err)
	}
}

type memorySchemaStore struct {
	records map[jobdb.JobSchemaKey]runtimecore.StoredJobSchema
}

func newMemorySchemaStore() *memorySchemaStore {
	return &memorySchemaStore{records: make(map[jobdb.JobSchemaKey]runtimecore.StoredJobSchema)}
}

func (s *memorySchemaStore) StoreJobSchema(_ context.Context, schema runtimecore.StoredJobSchema) (runtimecore.StoredJobSchema, error) {
	key := jobdb.JobSchemaKey{TenantId: schema.TenantId, SchemaHash: schema.SchemaHash}
	if existing, ok := s.records[key]; ok {
		return cloneStoredJobSchema(existing), nil
	}
	s.records[key] = cloneStoredJobSchema(schema)
	return cloneStoredJobSchema(schema), nil
}

func (s *memorySchemaStore) GetJobSchema(_ context.Context, key jobdb.JobSchemaKey) (runtimecore.StoredJobSchema, error) {
	schema, ok := s.records[key]
	if !ok {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	return cloneStoredJobSchema(schema), nil
}

func (s *memorySchemaStore) ListJobSchemas(_ context.Context, req runtimecore.ListJobSchemasRequest) (runtimecore.ListJobSchemasResponse, error) {
	out := make([]runtimecore.StoredJobSchema, 0, len(s.records))
	for _, schema := range s.records {
		if schema.TenantId != req.TenantId {
			continue
		}
		switch req.State {
		case jobdb.JobSchemaListStateActive:
			if schema.State != jobdb.JobSchemaStateActive {
				continue
			}
		case jobdb.JobSchemaListStateArchived:
			if schema.State != jobdb.JobSchemaStateArchived {
				continue
			}
		case jobdb.JobSchemaListStateAll:
		}
		out = append(out, cloneStoredJobSchema(schema))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SchemaHash < out[j].SchemaHash
	})
	return runtimecore.ListJobSchemasResponse{Schemas: out}, nil
}

func (s *memorySchemaStore) ArchiveJobSchema(_ context.Context, key jobdb.JobSchemaKey, archivedAt time.Time) (runtimecore.StoredJobSchema, error) {
	schema, ok := s.records[key]
	if !ok {
		return runtimecore.StoredJobSchema{}, jobdb.ErrJobSchemaNotFound
	}
	schema.State = jobdb.JobSchemaStateArchived
	at := archivedAt.UTC()
	schema.ArchivedAt = &at
	s.records[key] = cloneStoredJobSchema(schema)
	return cloneStoredJobSchema(schema), nil
}

func cloneStoredJobSchema(schema runtimecore.StoredJobSchema) runtimecore.StoredJobSchema {
	out := schema
	out.Schema = append(json.RawMessage(nil), schema.Schema...)
	if schema.ArchivedAt != nil {
		at := schema.ArchivedAt.UTC()
		out.ArchivedAt = &at
	}
	return out
}
