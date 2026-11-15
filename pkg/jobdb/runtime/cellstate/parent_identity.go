package cellstate

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ParentJobID checks proof completeness and a compatibility parent claim, and
// normalizes an omitted claim to the proof's job. It does not validate storage
// authority: the server/runtime must independently validate the stored live lease
// before accepting a submission, including retries.
func ParentJobID(ctx context.Context, tenant, claim string) (string, error) {
	p, present := Lease(ctx)
	if !present {
		if claim != "" {
			return "", jobdb.ErrExecutionLeaseLost
		}
		return "", nil
	}
	if p.JobKey.Validate() != nil || p.JobKey.TenantId != tenant || p.LeaseID == "" || p.WorkerID == "" {
		return "", jobdb.ErrExecutionLeaseLost
	}
	if claim != "" && claim != p.JobKey.JobId {
		return "", jobdb.ErrExecutionLeaseLost
	}
	return p.JobKey.JobId, nil
}
