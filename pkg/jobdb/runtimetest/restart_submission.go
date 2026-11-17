package runtimetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
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

// restartSourceSnapshot owns public observations, never readers, opener closures,
// errors or reader caches. It is private to the restart conformance assertion.
type restartSourceSnapshot struct {
	Info jobdb.JobInfo // Data is captured separately below.
	Data restartDataSnapshot
}
type restartDataSnapshot struct {
	Present       bool
	Outer         restartDataValue
	ResultPresent bool
	Result        restartDataValue
	Outcome       *restartOutcome
}
type restartDataValue struct {
	Envelope        bool
	Kind            string
	Payload         jobdb.Data
	PayloadOutcome  *restartOutcome
	Artifacts       []restartArtifactSnapshot
	ArtifactOutcome *restartOutcome
}
type restartArtifactSnapshot struct {
	Name       string
	Size       int64
	KeyPresent bool
	Key        jobdb.ArtifactKey
	Content    []byte
}
type restartOutcome struct {
	Message                                                               string
	Unfinished, Cancelled, Failed, GoalFailed, ContextCancelled, Deadline bool
	NonRetryablePresent, NonRetryable                                     bool
	Cancellation                                                          *string
	Cause                                                                 *restartOutcome
	App                                                                   *jobdb.AppErrorPayload
	System                                                                *jobdb.SystemErrorPayload
	Timeout                                                               *jobdb.TimeoutPayload
	Goal                                                                  *jobdb.GoalFailure
}

func captureRestartSource(info jobdb.JobInfo) (restartSourceSnapshot, error) {
	data := info.Data
	info.Data = nil
	owned, err := copyRestartValue(info)
	if err != nil {
		return restartSourceSnapshot{}, fmt.Errorf("source job fields: %w", err)
	}
	out := restartSourceSnapshot{Info: owned, Data: restartDataSnapshot{Present: data != nil}}
	if data == nil {
		return out, nil
	}
	result, outcome := jobdb.ExtractTaskDataResult(data)
	out.Data.Outcome, err = captureRestartOutcome(outcome, 0)
	if err != nil {
		return restartSourceSnapshot{}, fmt.Errorf("extract source data: %w", err)
	}
	out.Data.Outer, err = captureRestartDataValue(data)
	if err != nil {
		return restartSourceSnapshot{}, fmt.Errorf("outer source data: %w", err)
	}
	out.Data.ResultPresent = result != nil
	if result != nil {
		out.Data.Result, err = captureRestartDataValue(result)
		if err != nil {
			return restartSourceSnapshot{}, fmt.Errorf("extracted source data: %w", err)
		}
	}
	return out, nil
}

func captureRestartDataValue(data jobdb.TaskData) (restartDataValue, error) {
	var out restartDataValue
	if envelope, ok := data.(*jobdb.EnvelopedTaskData); ok && envelope != nil {
		out.Envelope, out.Kind = true, envelope.Kind
	}
	payload, outcome := data.GetData()
	var err error
	out.PayloadOutcome, err = captureRestartOutcome(outcome, 0)
	if err != nil {
		return out, fmt.Errorf("payload: %w", err)
	}
	out.Payload, err = copyRestartValue(payload)
	if err != nil {
		return out, err
	}
	artifacts, outcome := data.GetArtifacts()
	out.ArtifactOutcome, err = captureRestartOutcome(outcome, 0)
	if err != nil {
		return out, fmt.Errorf("artifact list: %w", err)
	}
	if artifacts != nil {
		out.Artifacts = make([]restartArtifactSnapshot, 0, len(artifacts))
	}
	for i, artifact := range artifacts {
		if artifact == nil {
			return out, fmt.Errorf("artifact %d is nil", i)
		}
		name := artifact.Name()
		key, err := artifact.ArtifactKey()
		keyPresent := err == nil
		if errors.Is(err, jobdb.ErrArtifactKeyUnavailable) {
			key = jobdb.ArtifactKey{}
		} else if err != nil {
			return out, fmt.Errorf("artifact %q key: %w", name, err)
		}
		reader, err := artifact.Open()
		if err != nil {
			return out, fmt.Errorf("artifact %q open: %w", name, err)
		}
		content, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return out, fmt.Errorf("artifact %q read/close: %w", name, err)
		}
		// Observe Size after Open populates any lazy cache. Keep persisted size
		// in Key separately; keyless artifacts still expose their loaded size.
		out.Artifacts = append(out.Artifacts, restartArtifactSnapshot{Name: name, Size: artifact.Size(), KeyPresent: keyPresent, Key: key, Content: bytes.Clone(content)})
	}
	sort.Slice(out.Artifacts, func(i, j int) bool {
		a, b := out.Artifacts[i], out.Artifacts[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Size != b.Size {
			return a.Size < b.Size
		}
		if a.KeyPresent != b.KeyPresent {
			return !a.KeyPresent
		}
		if a.Key.JobId != b.Key.JobId {
			return a.Key.JobId < b.Key.JobId
		}
		if a.Key.TaskOrdinal != b.Key.TaskOrdinal {
			return a.Key.TaskOrdinal < b.Key.TaskOrdinal
		}
		if a.Key.Name != b.Key.Name {
			return a.Key.Name < b.Key.Name
		}
		if a.Key.SizeBytes != b.Key.SizeBytes {
			return a.Key.SizeBytes < b.Key.SizeBytes
		}
		return bytes.Compare(a.Content, b.Content) < 0
	})
	return out, nil
}

func captureRestartOutcome(err error, depth int) (*restartOutcome, error) {
	if err == nil {
		return nil, nil
	}
	if depth > 64 {
		return nil, fmt.Errorf("cyclic or excessively nested deferred outcome")
	}
	out := &restartOutcome{Message: err.Error(),
		Unfinished: errors.Is(err, jobdb.ErrJobNotComplete), Cancelled: errors.Is(err, jobdb.ErrJobCancelled),
		Failed: errors.Is(err, jobdb.ErrJobFailed), GoalFailed: errors.Is(err, jobdb.ErrGoalFailed),
		ContextCancelled: errors.Is(err, context.Canceled), Deadline: errors.Is(err, context.DeadlineExceeded)}
	var nr jobdb.NonRetryableError
	if errors.As(err, &nr) {
		out.NonRetryablePresent, out.NonRetryable = true, nr.NonRetryable()
	}
	var cancelled *jobdb.JobCancelledError
	if errors.As(err, &cancelled) && cancelled != nil {
		detail := cancelled.Detail
		out.Cancellation = &detail
	}
	var failed *jobdb.JobFailedError
	if errors.As(err, &failed) && failed != nil {
		var captureErr error
		out.Cause, captureErr = captureRestartOutcome(failed.Cause, depth+1)
		if captureErr != nil {
			return nil, captureErr
		}
	}
	var app jobdb.AppError
	if errors.As(err, &app) {
		out.App = &app.Payload
	}
	var system jobdb.SystemError
	if errors.As(err, &system) {
		out.System = &system.Payload
	}
	var timeout jobdb.TimeoutError
	if errors.As(err, &timeout) {
		out.Timeout = &timeout.Payload
	}
	var goal *jobdb.GoalFailedError
	if errors.As(err, &goal) && goal != nil {
		out.Goal = &goal.Failure
	}
	if !out.Unfinished && !out.Cancelled && !out.Failed && !out.GoalFailed &&
		!out.ContextCancelled && !out.Deadline && out.App == nil && out.System == nil && out.Timeout == nil {
		return nil, fmt.Errorf("unexpected deferred read error: %w", err)
	}
	return copyRestartValue(out)
}

// copyRestartValue preserves concrete scalar types, nil/empty distinctions and
// exact time values. It copies each occurrence independently, even shared input
// pointers. Unknown mutable representations fail rather than escape as aliases.
func copyRestartValue[T any](value T) (T, error) {
	var zero T
	copied, err := copyRestartReflect(reflect.ValueOf(value), 0)
	if err != nil {
		return zero, err
	}
	if !copied.IsValid() {
		return zero, nil
	}
	return copied.Interface().(T), nil
}
func copyRestartReflect(v reflect.Value, depth int) (reflect.Value, error) {
	if !v.IsValid() {
		return v, nil
	}
	if depth > 64 {
		return reflect.Value{}, fmt.Errorf("cyclic or excessively nested snapshot value %s", v.Type())
	}
	if v.Type() == reflect.TypeFor[time.Time]() {
		return v, nil
	}
	out := reflect.New(v.Type()).Elem()
	switch v.Kind() {
	case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		out.Set(v)
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return out, nil
		}
		copied, err := copyRestartReflect(v.Elem(), depth+1)
		if err != nil {
			return reflect.Value{}, err
		}
		if v.Kind() == reflect.Pointer {
			out.Set(reflect.New(v.Type().Elem()))
			out.Elem().Set(copied)
		} else {
			out.Set(copied)
		}
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice {
			if v.IsNil() {
				return out, nil
			}
			out.Set(reflect.MakeSlice(v.Type(), v.Len(), v.Len()))
		}
		for i := 0; i < v.Len(); i++ {
			copied, err := copyRestartReflect(v.Index(i), depth+1)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("[%d]: %w", i, err)
			}
			out.Index(i).Set(copied)
		}
	case reflect.Map:
		if v.IsNil() {
			return out, nil
		}
		if v.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, fmt.Errorf("unsupported map key %s", v.Type())
		}
		out.Set(reflect.MakeMapWithSize(v.Type(), v.Len()))
		iter := v.MapRange()
		for iter.Next() {
			copied, err := copyRestartReflect(iter.Value(), depth+1)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("[%s]: %w", iter.Key(), err)
			}
			out.SetMapIndex(iter.Key(), copied)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if !field.IsExported() {
				return reflect.Value{}, fmt.Errorf("unsupported private field %s.%s", v.Type(), field.Name)
			}
			copied, err := copyRestartReflect(v.Field(i), depth+1)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("%s: %w", field.Name, err)
			}
			out.Field(i).Set(copied)
		}
	default:
		return reflect.Value{}, fmt.Errorf("unsupported snapshot value %s", v.Type())
	}
	return out, nil
}

func captureRestartChapters(chapters []jobdb.Chapter) ([]jobdb.Chapter, error) {
	for _, chapter := range chapters {
		switch chapter.Body.(type) {
		case jobdb.JobStartChapter, *jobdb.JobStartChapter,
			jobdb.JobAttemptOutcomeChapter, *jobdb.JobAttemptOutcomeChapter,
			jobdb.TaskAttemptOutcomeChapter, *jobdb.TaskAttemptOutcomeChapter,
			jobdb.RestartExtraChapter, *jobdb.RestartExtraChapter,
			jobdb.ExecutionChapter, *jobdb.ExecutionChapter:
		default:
			return nil, fmt.Errorf("chapter %d: unsupported body %T", chapter.Ordinal, chapter.Body)
		}
	}
	return copyRestartValue(chapters)
}
