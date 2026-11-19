package jobdb_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type retainedRuntime struct{ jobdb.ConsumerRuntime }

var _ jobdb.ConsumerRuntime = retainedRuntime{}

func TestConsumerRuntimeMethodSets(t *testing.T) {
	for _, pair := range [][2]reflect.Type{
		{reflect.TypeFor[jobdb.ConsumerRuntime](), reflect.TypeFor[jobdb.WorkflowRuntime]()},
		{reflect.TypeFor[jobdb.ConsumerGoalRuntime](), reflect.TypeFor[jobdb.GoalRuntime]()},
	} {
		require.Equal(t, pair[0].NumMethod()+1, pair[1].NumMethod())
		_, found := pair[0].MethodByName("AddGoalCandidate")
		require.False(t, found)
		for i := 0; i < pair[0].NumMethod(); i++ {
			method := pair[0].Method(i)
			legacy, ok := pair[1].MethodByName(method.Name)
			require.True(t, ok, method.Name)
			require.Equal(t, method.Type, legacy.Type, method.Name)
		}
	}
	rt := retainedRuntime{toy.New()}
	_, legacy := any(rt).(jobdb.GoalCandidateMutator)
	require.False(t, legacy)
	_, found := reflect.TypeOf(rt).MethodByName("AddGoalCandidate")
	require.False(t, found)
}

type runReadProbe struct {
	jobdb.JobRunReader
	fail     string
	err      error
	contexts []context.Context
}

func (r *runReadProbe) GetJob(ctx context.Context, key jobdb.JobKey) (jobdb.JobInfo, error) {
	r.contexts = append(r.contexts, ctx)
	if r.fail == "detail" {
		return jobdb.JobInfo{}, r.err
	}
	return r.JobRunReader.GetJob(ctx, key)
}
func (r *runReadProbe) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	r.contexts = append(r.contexts, ctx)
	if r.fail == "list" {
		return jobdb.ListJobsResponse{}, r.err
	}
	return r.JobRunReader.ListJobs(ctx, req)
}
func (r *runReadProbe) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	r.contexts = append(r.contexts, ctx)
	if r.fail == "chapters" {
		return nil, r.err
	}
	return r.JobRunReader.ListChapters(ctx, req)
}
func TestJobRunNarrowReader(t *testing.T) {
	rt := toy.New()
	ctx, cancel := context.WithCancel(celltest.Context(context.Background()))
	defer cancel()
	h, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic("input")}})
	require.NoError(t, err)
	r := &runReadProbe{JobRunReader: rt, err: errors.New("read denied")}
	require.Equal(t, 3, reflect.TypeOf(r).NumMethod())
	out, err := jobdb.GetJobRun(ctx, r, jobdb.GetJobRunRequest{JobKey: h.JobKey})
	require.NoError(t, err)
	require.Equal(t, h.JobKey, out.Job.JobKey)
	require.Equal(t, h.GoalID, out.Job.GoalID)
	require.NotEmpty(t, out.Attempts)
	require.Len(t, r.contexts, 3)
	for _, got := range r.contexts {
		require.Same(t, ctx, got)
	}
	for _, fail := range []string{"list", "detail", "chapters"} {
		r.fail = fail
		_, err = jobdb.GetJobRun(ctx, r, jobdb.GetJobRunRequest{JobKey: h.JobKey})
		require.ErrorIs(t, err, r.err, fail)
	}
	r.contexts = nil
	_, err = jobdb.GetJobRun(ctx, r, jobdb.GetJobRunRequest{})
	require.Error(t, err)
	require.Empty(t, r.contexts)
	r.fail = ""
	_, err = jobdb.GetJobRun(ctx, r, jobdb.GetJobRunRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "missing"}})
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
}
