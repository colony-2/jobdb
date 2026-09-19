package runtimetest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type routeJob struct {
	name  string
	tasks []string
}

func (j routeJob) Name() string { return j.name }
func (j routeJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	var out jobdb.TaskData = data
	for _, task := range j.tasks {
		var err error
		out, err = ctx.DoTask(jobdb.RunPolicy{}, task, out)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type routeTask struct{ calls *atomic.Int32 }

func (routeTask) Name() string { return "local:echo" }
func (t routeTask) Run(_ workflow.TaskContext, data jobdb.TaskData) (jobdb.TaskData, error) {
	t.calls.Add(1)
	return data, nil
}

// RunRouteConformance verifies exact identifiers across workflow and scheduler APIs.
func RunRouteConformance(t *testing.T, harnesses ...Harness) {
	for _, harness := range harnesses {
		t.Run(harness.Name, func(t *testing.T) {
			t.Run("workflow_identity", func(t *testing.T) {
				calls := &atomic.Int32{}
				job := routeJob{name: "recipe:食谱", tasks: []string{"local:echo", "two-step-op:second", "input:collect_user_input"}}
				built := buildFixture(t, harness, MustWorkSet(t, job, routeTask{calls: calls}))
				defer built.Shutdown(t)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				key, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{TenantId: built.WorkerTenantID, JobType: job.Name(), Data: NumberTaskData(1)})
				if err != nil {
					t.Fatal(err)
				}
				for _, taskType := range job.tasks[1:] {
					handle := WaitForTaskHandle(t, ctx, built.Engine, job.Name(), taskType, []string{key.TenantId})
					if handle.TaskType() != taskType {
						t.Fatalf("task identity: %q", handle.TaskType())
					}
					if err := handle.Finish(ctx, NumberTaskData(2)); err != nil {
						t.Fatal(err)
					}
				}
				for {
					info, err := built.Runtime.GetJob(ctx, key)
					if err != nil {
						t.Fatal(err)
					}
					if info.Status == jobdb.JobStatusCompleted {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(10 * time.Millisecond):
					}
				}
				if _, err := built.Engine.ReplayJobRun(ctx, workflow.ReplayRunRequest{JobKey: key}); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 1 {
					t.Fatalf("local task executed %d times, including replay", calls.Load())
				}
				chapters, err := built.Runtime.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				if err != nil {
					t.Fatal(err)
				}
				seen := map[string]bool{}
				for _, chapter := range chapters {
					seen[chapter.TaskType] = true
				}
				for _, taskType := range job.tasks {
					if !seen[taskType] {
						t.Fatalf("missing exact chapter identity %q", taskType)
					}
				}
			})
			t.Run("distinct_routes", func(t *testing.T) {
				built := buildFixture(t, harness)
				defer built.Shutdown(t)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				for _, invalid := range []jobdb.Route{{}, {JobType: "bad\x00name"}, {JobType: "bad\xffname"}, {JobType: "job", TaskType: "bad\xfftask"}} {
					if _, err := built.Runtime.PollWork(ctx, jobdb.PollWorkRequest{TenantId: built.WorkerTenantID, WorkerID: "invalid-route", Routes: []jobdb.Route{invalid}}); err == nil {
						t.Fatalf("polled invalid route %+v", invalid)
					}
					if invalid.JobType != "job" {
						if _, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: built.WorkerTenantID, JobType: invalid.JobType, Data: NumberTaskData(1)}}); err == nil {
							t.Fatalf("submitted invalid job type %q", invalid.JobType)
						}
					}
				}
				routes := []jobdb.Route{{JobType: "a:b", TaskType: "c"}, {JobType: "a", TaskType: "b:c"}}
				keys := make([]jobdb.JobKey, len(routes))
				wait := &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: "route-input", ResumeJobType: "resume:job"}
				for i, route := range routes {
					h, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: built.WorkerTenantID, JobType: route.JobType, Data: NumberTaskData(i)}})
					if err != nil {
						t.Fatal(err)
					}
					keys[i] = h.JobKey
					lease, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "route-worker", Routes: []jobdb.Route{{JobType: route.JobType}}})
					if err != nil || lease == nil {
						t.Fatalf("lease: %v", err)
					}
					if err := lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: route, TaskWait: wait}); err != nil {
						t.Fatal(err)
					}
				}
				leases, err := built.Runtime.PollWork(ctx, jobdb.PollWorkRequest{TenantId: built.WorkerTenantID, WorkerID: "route-worker", Routes: []jobdb.Route{{JobType: "a:b:c"}}, Limit: 1})
				if err != nil || len(leases) != 0 {
					t.Fatalf("job route matched task route: %v/%v", leases, err)
				}
				for i, route := range routes {
					listed, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{built.WorkerTenantID}, JobTasks: []jobdb.JobTaskFilter{{JobType: route.JobType, TaskType: route.TaskType}}})
					if err != nil || len(listed.Jobs) != 1 || listed.Jobs[0].JobKey != keys[i] {
						t.Fatalf("filtered routes: %+v / %v", listed, err)
					}
					if listed.Jobs[0].NextRoute == nil || *listed.Jobs[0].NextRoute != route {
						t.Fatal("summary changed route")
					}
					leases, err := built.Runtime.PollWork(ctx, jobdb.PollWorkRequest{TenantId: built.WorkerTenantID, WorkerID: "route-worker", Routes: []jobdb.Route{route}, Limit: 1})
					if err != nil || len(leases) != 1 {
						t.Fatalf("poll: %v", err)
					}
					if leases[0].Route() != route || leases[0].Job().JobKey != keys[i] {
						t.Fatal("poll conflated routes")
					}
					if err := leases[0].Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: route, TaskWait: wait}); err != nil {
						t.Fatal(err)
					}
					req := jobdb.CompleteTaskIfWaitingRequest{JobKey: keys[i], Route: routes[1-i], ResumeJobType: wait.ResumeJobType, OutputOrdinal: 1, InputHash: wait.InputHash, Data: NumberTaskData(2)}
					if err := built.Runtime.CompleteTaskIfWaiting(ctx, req); !errors.Is(err, jobdb.ErrConflict) {
						t.Fatalf("wrong route guard: %v", err)
					}
					req.Route = route
					if err := built.Runtime.CompleteTaskIfWaiting(ctx, req); err != nil {
						t.Fatal(err)
					}
					chapter, err := built.Runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: keys[i], Ordinal: 1})
					if err != nil || chapter.TaskType != route.TaskType {
						t.Fatalf("chapter identity: %+v/%v", chapter, err)
					}
					lease, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: keys[i], WorkerID: "route-worker", Routes: []jobdb.Route{{JobType: wait.ResumeJobType}}})
					if err != nil || lease == nil {
						t.Fatalf("resume: %v", err)
					}
					completeLeaseForTest(t, ctx, lease, 2)
				}
				for _, taskType := range []string{"", "fallback:task"} {
					h, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: built.WorkerTenantID, JobType: "alternate:job", Data: NumberTaskData(1)}})
					if err != nil {
						t.Fatal(err)
					}
					lease, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "route-worker", Routes: []jobdb.Route{{JobType: "alternate:job"}}})
					if err != nil || lease == nil {
						t.Fatalf("alternate initial lease: %v", err)
					}
					alternate := jobdb.Route{JobType: "fallback:job", TaskType: taskType}
					zero := time.Duration(0)
					if err := lease.Reschedule(ctx, jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: "alternate:job", TaskType: "primary:task"}, TaskWait: wait, AlternateRoute: &alternate, AlternateAfter: &zero}); err != nil {
						t.Fatal(err)
					}
					lease, err = built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "route-worker", Routes: []jobdb.Route{alternate}})
					if err != nil || lease == nil {
						t.Fatalf("alternate lease: %v", err)
					}
					if lease.Route() != alternate {
						t.Fatalf("alternate identity = %+v; want %+v", lease.Route(), alternate)
					}
					completeLeaseForTest(t, ctx, lease, 1)
				}
			})
		})
	}
}
