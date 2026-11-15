package cellstate

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestParentIdentityClaims(t *testing.T) {
	ctx := context.Background()
	p := LeaseProof{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "A"}, LeaseID: "lease", WorkerID: "worker"}
	parent, err := ParentJobID(ctx, "tenant", "")
	require.NoError(t, err)
	require.Empty(t, parent)
	_, err = ParentJobID(ctx, "tenant", "A")
	require.Error(t, err)
	for _, claim := range []string{"", "A", "B", "nonexistent"} {
		parent, err = ParentJobID(WithLease(ctx, p), "tenant", claim)
		if claim == "" || claim == "A" {
			require.NoError(t, err)
			require.Equal(t, "A", parent)
		} else {
			require.Error(t, err)
		}
		for _, bad := range []LeaseProof{{}, {JobKey: p.JobKey}, {JobKey: p.JobKey, LeaseID: p.LeaseID}, {JobKey: p.JobKey, WorkerID: p.WorkerID}, {JobKey: jobdb.JobKey{TenantId: "other", JobId: "A"}, LeaseID: "lease", WorkerID: "worker"}} {
			_, err = ParentJobID(WithLease(ctx, bad), "tenant", claim)
			require.Error(t, err)
		}
	}
}
