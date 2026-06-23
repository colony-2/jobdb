package directimpl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestSubmitJobSchemaAssociation(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tenantID := "tenant-schema-submit"
	schema := []byte(`{"type":"object","properties":{"ordinal":{"type":"integer"}}}`)

	schemaHash, _, err := jobdb.JobSchemaHash(schema)
	if err != nil {
		t.Fatalf("compute schema hash: %v", err)
	}
	handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: tenantID,
			JobType:  "schema-job",
			Data:     jobdb.NewTaskDataOrPanic(map[string]int{"n": 1}),
			Schema:   &jobdb.JobSchemaSelector{Schema: schema},
		},
	})
	if err != nil {
		t.Fatalf("submit schema job: %v", err)
	}
	registered, err := rt.GetJobSchema(ctx, jobdb.JobSchemaKey{TenantId: tenantID, SchemaHash: schemaHash})
	if err != nil {
		t.Fatalf("get registered inline schema: %v", err)
	}
	if registered.State != jobdb.JobSchemaStateActive {
		t.Fatalf("inline schema state = %s, want active", registered.State)
	}
	info, err := rt.GetJob(ctx, handle.JobKey)
	if err != nil {
		t.Fatalf("get schema job: %v", err)
	}
	if info.SchemaHash != registered.SchemaHash {
		t.Fatalf("job schema hash = %q, want %q", info.SchemaHash, registered.SchemaHash)
	}
	list, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenantID}})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(list.Jobs) != 1 || list.Jobs[0].SchemaHash != registered.SchemaHash {
		t.Fatalf("list schema hash = %+v, want %s", list.Jobs, registered.SchemaHash)
	}
	leases, err := rt.PollWork(ctx, jobdb.PollWorkRequest{
		TenantId:     tenantID,
		WorkerID:     "schema-worker",
		Capabilities: []string{"schema-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll work: %v", err)
	}
	if len(leases) != 1 || leaseSchemaHashForTest(leases[0]) != registered.SchemaHash {
		t.Fatalf("lease schema hash = %q, want %q", leaseSchemaHashForTest(leases[0]), registered.SchemaHash)
	}

	if _, err := rt.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{TenantId: tenantID, SchemaHash: registered.SchemaHash}); err != nil {
		t.Fatalf("archive schema: %v", err)
	}
	_, err = rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: tenantID,
			JobType:  "archived-schema-job",
			Data:     jobdb.NewTaskDataOrPanic(map[string]int{"n": 2}),
			Schema:   &jobdb.JobSchemaSelector{Hash: registered.SchemaHash},
		},
	})
	if !errors.Is(err, jobdb.ErrJobSchemaArchived) {
		t.Fatalf("submit archived schema error = %v, want ErrJobSchemaArchived", err)
	}

	plain, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: tenantID,
			JobType:  "plain-job",
			Data:     jobdb.NewTaskDataOrPanic(map[string]int{"n": 3}),
		},
	})
	if err != nil {
		t.Fatalf("submit plain job: %v", err)
	}
	plainInfo, err := rt.GetJob(ctx, plain.JobKey)
	if err != nil {
		t.Fatalf("get plain job: %v", err)
	}
	if plainInfo.SchemaHash != "" {
		t.Fatalf("plain job schema hash = %q, want empty", plainInfo.SchemaHash)
	}
}

func leaseSchemaHashForTest(lease jobdb.ExecutionLease) string {
	source, ok := lease.(interface{ LeaseSchemaHash() string })
	if !ok {
		return ""
	}
	return source.LeaseSchemaHash()
}
