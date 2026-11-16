package runtimecore_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/stretchr/testify/require"
)

func TestSubmissionAvailabilityPreservesWideInstants(t *testing.T) {
	for _, overHTTP := range []bool{false, true} {
		for _, year := range []int{1, 2300, 9999} {
			t.Run(fmt.Sprintf("http=%v/year=%d", overHTTP, year), func(t *testing.T) {
				history := &goalCaptureHistory{}
				scheduler := &availabilityCaptureScheduler{goalCaptureScheduler: &goalCaptureScheduler{history: history}}
				core, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: scheduler, Chapters: history, Schemas: readTestSchemas{}})
				require.NoError(t, err)
				var rt jobdb.WorkflowRuntime = core
				if overHTTP {
					s := httptest.NewServer(celltest.Handler(remote.NewServer(core)))
					t.Cleanup(s.Close)
					rt, err = remote.New(s.URL, s.Client())
					require.NoError(t, err)
				}
				at := time.Date(year, 1, 1, 0, 0, 0, 123456789, time.UTC)
				_, err = rt.SubmitJob(celltest.Context(context.Background()), jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: "tenant", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil), AvailableAt: &at}})
				require.NoError(t, err)
				require.NotNil(t, scheduler.created.AvailableAt)
				require.True(t, at.Equal(*scheduler.created.AvailableAt))
				require.Equal(t, at.Nanosecond(), scheduler.created.AvailableAt.Nanosecond())
			})
		}
	}
}

// Supply complete response identity while retaining the existing scheduler spy.
type availabilityCaptureScheduler struct{ *goalCaptureScheduler }

func (s *availabilityCaptureScheduler) PrepareJob(ctx context.Context, p runtimecore.JobPreparation) (runtimecore.PreparedJob, error) {
	out, err := s.goalCaptureScheduler.PrepareJob(ctx, p)
	out.Target = jobdb.ResolvedCell{Cell: "test", CellID: "cell", CellRepo: "example.test/test"}
	return out, err
}
func (s *availabilityCaptureScheduler) CreateJob(ctx context.Context, req runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	out, err := s.goalCaptureScheduler.CreateJob(ctx, req)
	out.Cell = jobdb.ResolvedCell{Cell: "test", CellID: "cell", CellRepo: "example.test/test"}
	return out, err
}
