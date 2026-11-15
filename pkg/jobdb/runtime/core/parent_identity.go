package runtimecore

import (
	"context"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

func (r *Runtime) submissionParent(ctx context.Context, tenant, claim string) (string, error) {
	parent, err := cellstate.ParentJobID(ctx, tenant, claim)
	if err != nil {
		return "", err
	}
	if p, present := cellstate.Lease(ctx); present {
		if _, err = r.ValidateExecutionLeaseByID(ctx, p.JobKey, p.LeaseID, p.WorkerID); err != nil {
			if errors.Is(err, jobdb.ErrJobNotFound) {
				return "", jobdb.ErrExecutionLeaseLost
			}
			return "", err
		}
	}
	return parent, nil
}
