package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// This fixture deliberately loads the recipe from reconstructed job data. Merely
// echoing the submission would miss retained descriptors with no backing bytes.
func runSubmissionRestart(t *testing.T, harnesses []Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			for _, state := range []string{"ready", "cancelled", "executed"} {
				t.Run(state, func(t *testing.T) {
					f := buildFixture(t, h)
					defer f.Shutdown(t)
					ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), 30*time.Second)
					defer cancel()
					rt := f.Runtime
					var setup, tasks int
					task := submissionRecipeTask{run: func(n int) int { tasks++; return n * 3 }}
					worker := workflow.ExecutionWorker{JobType: "artifact-recipe", Execute: func(c workflow.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
						artifacts, err := in.GetArtifacts()
						if err != nil {
							return nil, err
						}
						var recipe struct {
							Setup int `yaml:"setup"`
							Add   int `yaml:"add"`
						}
						found := false
						for _, a := range artifacts {
							if a.Name() != "submission.recipe.yaml" {
								continue
							}
							b, err := a.Bytes(c.Context())
							if err != nil {
								return nil, err
							}
							if err := yaml.Unmarshal(b, &recipe); err != nil {
								return nil, err
							}
							found = true
						}
						if !found {
							return nil, fmt.Errorf("missing submitted recipe")
						}
						setup++
						return c.DoTask(workflow.TaskCall{Site: "compute", TaskType: task.Name(), ContractVersion: 1, Command: json.RawMessage(`{}`)}, func(context.Context) (jobdb.TaskData, error) {
							return NumberTaskData(recipe.Setup + recipe.Add), nil
						})
					}}
					run := func(key jobdb.JobKey, expected int) {
						execution, err := workflow.GetJobForRun(ctx, rt, workflow.GetJobForRunRequest{JobKey: key, JobWorker: worker, TaskWorkers: []workflow.TaskWorker{task}, WorkerID: "recipe-worker", LeaseDuration: time.Minute})
						require.NoError(t, err)
						result, err := execution.Run(nil)
						require.NoError(t, err)
						if expected >= 0 {
							require.Equal(t, workflow.JobRunCompleted, result.Status, "%v", result.JobError)
							require.Equal(t, expected, MustDecodeNumberTaskData(t, result.Output))
						} else {
							require.Equal(t, workflow.JobRunFailed, result.Status)
						}
					}
					recipe := []byte("setup: 7\nadd: 2\n")
					source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: f.WorkerTenantID, JobType: worker.Name(), Metadata: json.RawMessage(`{"original":true}`), Data: jobdb.NewTaskDataOrPanic(map[string]string{"input": "original"}, jobdb.NewArtifactFromBytes("submission.recipe.yaml", recipe), jobdb.NewArtifactFromBytes("empty", []byte{}))}})
					require.NoError(t, err)
					if state == "cancelled" {
						require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: source.JobKey}))
					}
					if state == "executed" {
						run(source.JobKey, 27)
					}
					history, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source.JobKey})
					require.NoError(t, err)
					if state != "executed" {
						require.Len(t, history, 1)
					}
					history, err = captureRestartChapters(history)
					require.NoError(t, err)
					info, err := rt.GetJob(ctx, source.JobKey)
					require.NoError(t, err)
					beforeSource, err := captureRestartSource(info)
					require.NoError(t, err)
					readArtifacts := func(key jobdb.JobKey) {
						for name, want := range map[string][]byte{"submission.recipe.yaml": recipe, "empty": {}} {
							reader, err := rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: key, Ordinal: 0, Name: name})
							require.NoError(t, err)
							rc, err := reader.Open()
							require.NoError(t, err)
							got, err := io.ReadAll(rc)
							require.NoError(t, err)
							require.NoError(t, rc.Close())
							require.Equal(t, string(want), string(got))
						}
					}
					readArtifacts(source.JobKey)
					beforeSetup, beforeTasks := setup, tasks
					req := jobdb.SubmitRestartJobRequest{RequestID: "recipe-restart", Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, LastStepToKeep: 0}}
					destination, err := rt.SubmitRestartJob(ctx, req)
					require.NoError(t, err)
					retained, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: destination.JobKey})
					require.NoError(t, err)
					require.Equal(t, history[:1], retained)
					readArtifacts(destination.JobKey)
					again, err := rt.SubmitRestartJob(ctx, req)
					require.NoError(t, err)
					require.Equal(t, destination, again)
					run(destination.JobKey, 27)
					require.Equal(t, beforeSetup+1, setup)
					require.Equal(t, beforeTasks+1, tasks)
					fresh, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: destination.JobKey, Ordinal: 1})
					require.NoError(t, err)
					execution, ok := fresh.Body.(jobdb.ExecutionChapter)
					require.True(t, ok)
					record, err := jobdb.DecodeExecutionRecord(execution.Record)
					require.NoError(t, err)
					require.Equal(t, "header", record.Kind)
					readArtifacts(destination.JobKey)
					again, err = rt.SubmitRestartJob(ctx, req)
					require.NoError(t, err)
					require.Equal(t, destination, again)
					require.Equal(t, beforeSetup+1, setup)
					require.Equal(t, beforeTasks+1, tasks)
					unchanged, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: source.JobKey})
					require.NoError(t, err)
					require.Equal(t, history, unchanged)
					after, err := rt.GetJob(ctx, source.JobKey)
					require.NoError(t, err)
					afterSource, err := captureRestartSource(after)
					require.NoError(t, err)
					require.Equal(t, beforeSource, afterSource)
					readArtifacts(source.JobKey)
					// A second generation must resolve artifacts independently as well.
					next, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: destination.JobKey}})
					require.NoError(t, err)
					readArtifacts(next.JobKey)
					run(next.JobKey, 27)
					if state == "ready" {
						for _, control := range []struct {
							recipe   string
							expected int
						}{{"setup: 4\nadd: 1\n", 15}, {"setup: [invalid", -1}} {
							controlSource, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: f.WorkerTenantID, JobType: worker.Name(), Data: jobdb.NewTaskDataOrPanic("unchanged", jobdb.NewArtifactFromBytes("submission.recipe.yaml", []byte(control.recipe)))}})
							require.NoError(t, err)
							controlDestination, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: controlSource.JobKey}})
							require.NoError(t, err)
							run(controlDestination.JobKey, control.expected)
						}
					}

				})
			}
		})
	}
}

type submissionRecipeTask struct{ run func(int) int }

func (submissionRecipeTask) Name() string { return "recipe-compute" }
func (t submissionRecipeTask) Run(_ workflow.TaskContext, in jobdb.TaskData) (jobdb.TaskData, error) {
	n, err := numberFromTaskData(in)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(t.run(n)), nil
}

func runSubmissionRestartBoundaries(t *testing.T, harnesses []Harness) {
	for _, h := range harnesses {
		t.Run(h.Name, func(t *testing.T) {
			f := buildFixture(t, h)
			defer f.Shutdown(t)
			ctx, cancel := context.WithTimeout(celltest.Context(context.Background()), 30*time.Second)
			defer cancel()
			rt := f.Runtime
			submit := func(tenant, id string) jobdb.JobHandle {
				source, err := celltest.Submit(rt, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{Cell: "test", CellRepo: "example.test/test", TenantId: tenant, JobID: id, JobType: "boundary", Data: jobdb.NewTaskDataOrPanic(tenant, jobdb.NewArtifactFromBytes("recipe", []byte(tenant)))}})
				require.NoError(t, err)
				return source
			}
			source := submit(f.WorkerTenantID, "source")
			req := jobdb.SubmitRestartJobRequest{RequestID: "receipt", Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, JobID: "destination"}}
			dest, err := rt.SubmitRestartJob(ctx, req)
			require.NoError(t, err)
			checkUnchanged := func() {
				chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: dest.JobKey})
				require.NoError(t, err)
				require.Len(t, chapters, 1)
				var input string
				require.NoError(t, json.Unmarshal(chapters[0].Body.(jobdb.JobStartChapter).Input.Data, &input))
				require.Equal(t, source.JobKey.TenantId, input)
			}
			changed := req
			changed.Job.ClientPayloadUpdate = &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"changed":true}`)}
			_, err = rt.SubmitRestartJob(ctx, changed)
			require.ErrorIs(t, err, jobdb.ErrExistingJobMismatch)
			checkUnchanged()
			changed = req
			changed.RequestID = "different-request"
			_, err = rt.SubmitRestartJob(ctx, changed)
			require.Error(t, err)
			checkUnchanged()
			changed = req
			changed.Job.JobID = "another-destination"
			_, err = rt.SubmitRestartJob(ctx, changed)
			require.Error(t, err)
			checkUnchanged()
			again, err := rt.SubmitRestartJob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, dest, again)
			for _, mode := range []string{"missing-source", "other-cut", "logical", "cell", "prerequisite", "dependency"} {
				t.Run(mode, func(t *testing.T) {
					bad := jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, JobID: "invalid-" + mode}}
					switch mode {
					case "missing-source":
						bad.Job.PriorJobKey.JobId = "absent"
					case "other-cut":
						bad.Job.LastStepToKeep = 1
					case "logical":
						bad.Job.ExecutionRestart = &jobdb.ExecutionRestart{ScopeID: "missing-scope"}
					case "cell":
						bad.Job.Cell = "unregistered"
					case "prerequisite":
						bad.Job.Prerequisites = []jobdb.JobPrerequisite{{JobID: bad.Job.JobID, Condition: jobdb.JobPrereqSuccess}}
					case "dependency":
						bad.Job.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: "missing"}, Condition: jobdb.JobPrereqSuccess}}
					}
					_, err := rt.SubmitRestartJob(ctx, bad)
					require.Error(t, err)
					key := jobdb.JobKey{TenantId: source.JobKey.TenantId, JobId: bad.Job.JobID}
					_, err = rt.GetJob(ctx, key)
					require.Error(t, err)
					_, err = rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: key, Name: "recipe"})
					require.Error(t, err)
				})
			}
			// The same IDs in another tenant carry that tenant's bytes.
			other := submit(f.WorkerTenantID+"-other", "source")
			otherDest, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{RequestID: "receipt", Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: other.JobKey, JobID: "destination"}})
			require.NoError(t, err)
			for _, d := range []jobdb.JobHandle{dest, otherDest} {
				art, err := rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: d.JobKey, Name: "recipe"})
				require.NoError(t, err)
				rc, err := art.Open()
				require.NoError(t, err)
				b, err := io.ReadAll(rc)
				require.NoError(t, err)
				require.NoError(t, rc.Close())
				require.Equal(t, d.JobKey.TenantId, string(b))
			}
			// A closed source goal rejects continuation, but a fresh goal is valid.
			require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: source.JobKey}))
			_, err = rt.FailGoal(ctx, jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: source.JobKey.TenantId, GoalID: source.GoalID}, Message: "closed source goal"})
			require.NoError(t, err)
			_, err = rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, ShareGoal: true, JobID: "closed-goal"}})
			require.ErrorIs(t, err, jobdb.ErrGoalConflict)
			branch, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey}})
			require.NoError(t, err)
			require.NotEqual(t, source.GoalID, branch.GoalID)
			// A live lease may restart a submission-only source; cancellation revokes it.
			parent := submit(f.WorkerTenantID, "parent")
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: parent.JobKey, WorkerID: "parent-worker", Routes: []jobdb.Route{{JobType: "boundary"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NotNil(t, lease)
			child, err := lease.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: other.JobKey}})
			require.Error(t, err)
			require.Empty(t, child.JobKey.JobId)
			_, err = lease.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey}})
			require.NoError(t, err)
			require.NoError(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: parent.JobKey}))
			_, err = lease.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{Cell: "test", PriorJobKey: source.JobKey, JobID: "revoked"}})
			require.Error(t, err)
		})
	}
}
