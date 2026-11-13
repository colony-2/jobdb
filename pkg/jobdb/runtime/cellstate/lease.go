package cellstate

import (
	"context"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// LeaseProof is trusted backend input, never a wire authorization credential.
// Backends must revalidate these fields against stored live lease state on every
// resolution and publication. The proof itself conveys no permissions.
type LeaseProof struct {
	JobKey            jobdb.JobKey
	LeaseID, WorkerID string
}
type leaseKey struct{}

// CreateWithLease registers a cell using lease authority. The backend must
// validate the exact live lease in the same transaction before calling this.
// Creation grants no access to submit jobs into the new cell.
func (s *State) CreateWithLease(ctx context.Context, p LeaseProof, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	if req.TenantID != "" && req.TenantID != p.JobKey.TenantId {
		return jobdb.Cell{}, jobdb.ErrCellForbidden
	}
	req.TenantID = p.JobKey.TenantId
	ctx = jobdb.WithCellAuthority(ctx, jobdb.CellAuthority{
		TenantID:    req.TenantID,
		Principal:   fmt.Sprintf("lease:%q:%q:%q", p.JobKey.JobId, p.LeaseID, p.WorkerID),
		Permissions: map[string]bool{"cells.create": true},
	})
	return s.Create(ctx, req)
}

func WithLease(ctx context.Context, p LeaseProof) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, leaseKey{}, p)
}
func Lease(ctx context.Context) (LeaseProof, bool) {
	if ctx == nil {
		return LeaseProof{}, false
	}
	p, ok := ctx.Value(leaseKey{}).(LeaseProof)
	return p, ok
}
