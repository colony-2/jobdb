package runtimecore

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

// AdmitWrite forwards the configured durable project admission. Tenant is project ID.
func (r *Runtime) AdmitWrite(ctx context.Context, id string) error {
	return jobdb.RequireProjectWrite(ctx, r.projects, id)
}
func (r *Runtime) CheckProject(ctx context.Context, id string) error {
	if p, ok := r.projects.(jobdb.ProjectExistence); ok {
		return p.CheckProject(ctx, id)
	}
	return jobdb.ErrProjectUnavailable
}
