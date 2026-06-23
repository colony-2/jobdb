package jobschema

import (
	"context"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func ResolveActiveForNewJob(ctx context.Context, registry jobdb.JobSchemaRegistry, tenantID string, selector *jobdb.JobSchemaSelector) (string, error) {
	hash, schema, hasInline, err := jobdb.ResolveJobSchemaSelector(selector)
	if err != nil {
		return "", err
	}
	if hash == "" {
		return "", nil
	}
	if registry == nil {
		return "", fmt.Errorf("job schema registry is required")
	}

	var info jobdb.JobSchemaInfo
	if hasInline {
		info, err = registry.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
			TenantId: tenantID,
			Schema:   schema,
		})
	} else {
		info, err = registry.GetJobSchema(ctx, jobdb.JobSchemaKey{
			TenantId:   tenantID,
			SchemaHash: hash,
		})
	}
	if err != nil {
		return "", err
	}
	if info.State == jobdb.JobSchemaStateArchived {
		return "", jobdb.ErrJobSchemaArchived
	}
	return info.SchemaHash, nil
}
