package jobdb_test

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type optionalProvenance struct{ calls int }

func (r *optionalProvenance) ReadExecutionProvenance(context.Context, jobdb.ReadExecutionProvenanceRequest) (jobdb.ExecutionProvenance, error) {
	r.calls++
	return jobdb.ExecutionProvenance{Capability: jobdb.ExecutionProvenanceCapability}, nil
}
func TestOptionalExecutionProvenanceForwardingAndMaxima(t *testing.T) {
	_, err := jobdb.ReadExecutionProvenance(context.Background(), struct{}{}, jobdb.ReadExecutionProvenanceRequest{})
	require.ErrorIs(t, err, jobdb.ErrExecutionProvenanceUnsupported)
	r := &optionalProvenance{}
	out, err := jobdb.ReadExecutionProvenance(context.Background(), r, jobdb.ReadExecutionProvenanceRequest{})
	require.NoError(t, err)
	require.Equal(t, 1, r.calls)
	require.Equal(t, jobdb.ExecutionProvenanceCapability, out.Capability)
	require.Equal(t, jobdb.ExecutionProvenanceLimits{Pages: 64, Records: 10000, ScannedBytes: 16777216, Pairs: 512, Issues: 128, ElapsedMillis: 1500, ResponseBytes: 262144, PageRecords: 256, PageBytes: 1048576, NestingDepth: 64, IdentityBytes: 4096}, jobdb.DefaultExecutionProvenanceLimits())
}
