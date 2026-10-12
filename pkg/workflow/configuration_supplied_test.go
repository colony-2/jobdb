package workflow_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
)

func TestSuppliedLeaseConfigurationReads(t *testing.T) {
	f := newSuppliedFixture(t)
	ctx := context.Background()
	_, err := f.backend.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "x", Value: json.RawMessage(`1`)})
	require.NoError(t, err)
	lease := f.imported(t)
	job := suppliedJob{run: func(work workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
		a, e := work.GetConfiguration(context.Background())
		if e != nil {
			return nil, e
		}
		require.Equal(t, "1", string(a.Values["x"]))
		_, e = f.backend.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "x", Value: json.RawMessage(`2`)})
		if e != nil {
			return nil, e
		}
		b, e := work.GetConfiguration(context.Background())
		if e != nil {
			return nil, e
		}
		require.Equal(t, "1", string(b.Values["x"]))
		require.Equal(t, a.Revision, b.Revision)
		return data, nil
	}}
	runnable, err := workflow.GetJobForRunWithLease(ctx, f.receiver, lease, workflow.GetJobForRunRequest{JobKey: f.key, JobWorker: job})
	require.NoError(t, err)
	result, err := runnable.Run(nil)
	require.NoError(t, err)
	require.Equal(t, workflow.JobRunCompleted, result.Status)
	require.Zero(t, f.acquisitions.Load())
}
