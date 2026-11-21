package runtimecore

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func (r *Runtime) ClearCrashConcern(ctx context.Context, req jobdb.ClearCrashConcernRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	if err := r.validate(); err != nil {
		return err
	}
	if req.WorkerID == "" {
		req.WorkerID = "jobdb-core"
	}
	if projectErr := jobdb.RequireProjectWrite(ctx, r.projects, req.JobKey.TenantId); projectErr != nil {
		return projectErr
	}

	return r.scheduler.ClearCrashConcern(ctx, req)
}
