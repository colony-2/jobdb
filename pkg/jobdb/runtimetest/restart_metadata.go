package runtimetest

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func runRestartClonesMetadata(t *testing.T, harnesses []Harness) {
	for _, harness := range harnesses {
		t.Run(harness.Name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				metadata json.RawMessage
			}{
				{name: "absent"},
				{name: "empty", metadata: json.RawMessage(`{}`)},
				{name: "nested", metadata: json.RawMessage(`{"team":"research","requirements":{"cpu":4},"tags":["a","b"],"nullable":null,"id":9007199254740993}`)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					built := buildFixture(t, harness)
					defer built.Shutdown(t)
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					r := built.Runtime
					parent, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
						TenantId: built.WorkerTenantID, JobType: "metadata-parent", Data: NumberTaskData(0),
						Metadata: json.RawMessage(`{"parentOnly":true}`),
					}})
					require.NoError(t, err)
					lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
						JobKey: parent.JobKey, WorkerID: "metadata-worker", Routes: []jobdb.Route{{JobType: "metadata-parent"}}, LeaseDuration: time.Minute,
					})
					require.NoError(t, err)
					require.NotNil(t, lease)
					source, err := lease.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{
						JobType: "metadata-source", Data: NumberTaskData(1), Metadata: tc.metadata,
					}})
					require.NoError(t, err)
					complete := func(h jobdb.JobHandle) {
						l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
							JobKey: h.JobKey, WorkerID: "metadata-worker", Routes: []jobdb.Route{{JobType: "metadata-source"}}, LeaseDuration: time.Minute,
						})
						require.NoError(t, err)
						require.NotNil(t, l)
						completeLeaseForTest(t, ctx, l, 1)
					}
					read := func(h jobdb.JobHandle) jobdb.JobSummary {
						listed, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{h.JobKey.TenantId}, JobKeys: []jobdb.JobKey{h.JobKey}})
						require.NoError(t, err)
						require.Len(t, listed.Jobs, 1)
						return listed.Jobs[0]
					}
					decode := func(raw json.RawMessage) any {
						if len(raw) == 0 {
							return nil
						}
						d := json.NewDecoder(bytes.NewReader(raw))
						d.UseNumber()
						var v any
						require.NoError(t, d.Decode(&v))
						return v
					}
					complete(source) // Metadata must survive restarting an archived job.
					original := read(source)
					require.Equal(t, parent.JobKey.JobId, original.ParentJobID)
					for _, viaLease := range []bool{false, true} {
						req := jobdb.SubmitRestartJobRequest{RequestID: "metadata-root", Job: jobdb.SubmitRestartJob{PriorJobKey: source.JobKey}}
						submit := r.SubmitRestartJob
						wantParent := ""
						if viaLease {
							req.RequestID = "metadata-child"
							submit = lease.SubmitRestartJob
							wantParent = parent.JobKey.JobId
						}
						restarted, err := submit(ctx, req)
						require.NoError(t, err)
						again, err := submit(ctx, req)
						require.NoError(t, err)
						require.Equal(t, restarted, again)
						copied := read(restarted)
						require.Equal(t, decode(original.Metadata), decode(copied.Metadata))
						require.Equal(t, wantParent, copied.ParentJobID)
						complete(restarted)
						next, err := r.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: restarted.JobKey}})
						require.NoError(t, err)
						require.Equal(t, decode(original.Metadata), decode(read(next).Metadata))
					}
					require.Equal(t, decode(original.Metadata), decode(read(source).Metadata))
				})
			}
		})
	}
}
