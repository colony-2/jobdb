package runtimecore

import (
	"context"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type submissionFaultScheduler struct {
	Scheduler
	failure error
}

func (s submissionFaultScheduler) PrepareJob(_ context.Context, q JobPreparation) (PreparedJob, error) {
	return PreparedJob{Key: q.Key, Target: jobdb.ResolvedCell{Cell: "test", CellID: "cell", CellRepo: "example.test/test"}, GoalID: "goal"}, s.failure
}
func (s submissionFaultScheduler) DiscardJobPreparation(context.Context, jobdb.JobKey) error {
	return nil
}

type submissionFaultChapters struct {
	ChapterLog
	failure error
}

func (s submissionFaultChapters) Create(context.Context, ChapterLogKey, EncodedChapter) error {
	return s.failure
}

func TestInfrastructureSubmissionPortFailuresKeepCause(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cause := errors.New("backend unavailable")
	for _, operation := range []string{"prepare", "create", "restart-prepare"} {
		t.Run(operation, func(t *testing.T) {
			scheduler := submissionFaultScheduler{}
			if operation != "create" {
				scheduler.failure = cause
			}
			r := &Runtime{projects: celltest.UnrestrictedProjects{}, scheduler: scheduler, chapters: submissionFaultChapters{failure: cause}, schemas: &SchemaRegistry{}, now: time.Now}
			var err error
			key := jobdb.JobKey{TenantId: "tenant", JobId: "child"}
			if operation == "restart-prepare" {
				_, err = r.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", CellRepo: "example.test/test", PriorJobKey: jobdb.JobKey{TenantId: key.TenantId, JobId: "source"}, JobID: key.JobId}})
			} else {
				_, err = r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: key.TenantId, JobID: key.JobId, JobType: "child", Data: jobdb.NewTaskDataOrPanic("input")}})
			}
			require.ErrorIs(t, err, cause)
			var detail *jobdb.ExecutionInfrastructureError
			require.ErrorAs(t, err, &detail)
			require.Equal(t, jobdb.ExecutionInfrastructureRuntimeIO, detail.Reason)
			require.Equal(t, &key, detail.JobKey)
		})
	}
}
