package publication_test

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
)

// This consumer imports only public APIs. The publication checker copies these
// authored tests into an independent module; it never copies JobDB source.
func TestPublishedGoals(t *testing.T) {
	for _, backend := range []string{"toy", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			ctx := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{
				Principal: "publication-test", TenantID: "c2", Permissions: map[string]bool{
					"cells.create": true, "cells.get": true, "jobs.submit": true, "jobs.get": true, "jobs.list": true, "jobs.acquire": true,
				},
			})
			ctx = jobdb.WithPrimaryGoalSelectionAuthorizer(ctx, func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error { return nil })
			catalog := &projectcatalog.Service{Store: &projectcatalog.MemoryStore{}}
			admin := jobdb.WithProjectAdministration(ctx, func(context.Context) error { return nil })
			must(t, catalog.Install(admin, projectcatalog.Installation{Manifest: projectcatalog.InstallationDigest([]byte("isolated publication fixture")), Seeds: []projectcatalog.Seed{{ID: "c2", Name: "c2"}}}))
			var rt jobdb.WorkflowRuntime
			cfg := sqlite.Config{Projects: catalog, DBPath: filepath.Join(t.TempDir(), "goals.db")}
			if backend == "toy" {
				rt = toy.New(toy.WithProjects(catalog))
			} else {
				r, err := sqlite.NewFromConfig(ctx, cfg)
				must(t, err)
				t.Cleanup(func() { must(t, r.Close(ctx)) })
				rt = r
			}
			caps, ok := rt.(jobdb.JobGoalCapability)
			if !ok || caps.JobGoalCapabilities() != (jobdb.JobGoalCapabilities{ImmutableMembership: true, AtomicAdmission: true, TerminalFailureRecords: true}) {
				t.Fatal("required immutable-goal capabilities are unavailable")
			}
			cells, ok := rt.(jobdb.CellRuntime)
			if !ok {
				t.Fatal("cell registration unavailable")
			}
			_, err := cells.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "c2", Name: "work", Repo: "example.com/publication/work"})
			must(t, err)
			request := func(id, goal string) jobdb.SubmitJobRequest {
				return jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", JobID: id, GoalID: goal,
					Cell: "work", CellRepo: "example.com/publication/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}}
			}
			origin, err := rt.SubmitJob(ctx, request("origin", ""))
			must(t, err)
			must(t, rt.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: origin.JobKey}))
			req := request("recovery", origin.GoalID)
			recovery, err := rt.SubmitJob(ctx, req)
			must(t, err)
			if recovery.GoalID != origin.GoalID || recovery.CellID != origin.CellID {
				t.Fatal("selected recovery lost primary membership or ownership")
			}
			_, err = rt.SubmitJob(ctx, request("competitor", origin.GoalID))
			mustBe(t, err, jobdb.ErrGoalConflict)
			_, err = rt.GetJob(ctx, jobdb.JobKey{TenantId: "c2", JobId: "competitor"})
			mustBe(t, err, jobdb.ErrJobNotFound)
			t.Log("selected admission rejected competing publication")

			key := jobdb.GoalKey{TenantId: "c2", GoalID: origin.GoalID}
			failReq := jobdb.FailGoalRequest{GoalKey: key, Message: "  permanent 保存\nreason  "}
			failed, err := rt.FailGoal(ctx, failReq)
			must(t, err)
			if failed.GoalKey != key || failed.State != jobdb.GoalFailed || failed.FailureJobID == "" || failed.Failure == nil || failed.Failure.Message != failReq.Message ||
				!reflect.DeepEqual(failed.Failure.TerminalJobIDs, []string{"origin"}) ||
				!reflect.DeepEqual(failed.Failure.NonTerminalJobIDs, []string{"recovery"}) {
				t.Fatalf("unexpected frozen prior-member snapshot: %+v", failed)
			}
			replayed, err := rt.FailGoal(ctx, failReq)
			must(t, err)
			if !reflect.DeepEqual(failed, replayed) {
				t.Fatal("equal failure replay changed identity or snapshot")
			}
			changed := failReq
			changed.Message = "different"
			_, err = rt.FailGoal(ctx, changed)
			mustBe(t, err, jobdb.ErrGoalConflict)
			replayedJob, err := rt.SubmitJob(ctx, req)
			must(t, err)
			if !reflect.DeepEqual(recovery, replayedJob) {
				t.Fatal("accepted submission replay changed identity after closure")
			}
			_, err = rt.SubmitJob(ctx, request("after-closure", origin.GoalID))
			mustBe(t, err, jobdb.ErrGoalConflict)
			t.Log("protected failure and accepted submission replay retained identity after closure")

			// FailGoal must leave the existing execution runnable. Its later success
			// must preserve both the first closure and its original member snapshot.
			lease, err := rt.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: recovery.JobKey, WorkerID: "consumer", Routes: []jobdb.Route{{JobType: "work"}}, LeaseDuration: time.Minute})
			must(t, err)
			if lease == nil {
				t.Fatal("goal failure implicitly cancelled the existing execution")
			}
			chapter := jobdb.Chapter{Ordinal: 1, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"done"`)}}}}
			must(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &chapter}))
			after, err := rt.GetGoal(ctx, key)
			must(t, err)
			if !reflect.DeepEqual(failed, after) {
				t.Fatal("late execution success changed goal closure or frozen snapshot")
			}
			inspectFailure(t, ctx, rt, failed)
			if backend == "sqlite" {
				must(t, rt.(*sqlite.Runtime).Close(ctx))
				reopened, err := sqlite.NewFromConfig(ctx, cfg)
				must(t, err)
				defer reopened.Close(ctx)
				saved, err := reopened.GetGoal(ctx, key)
				must(t, err)
				if !reflect.DeepEqual(failed, saved) {
					t.Fatal("SQLite reopen changed retained failure")
				}
				inspectFailure(t, ctx, reopened, saved)
				t.Log("SQLite reopen retained failure identity, message and frozen snapshot")
			}
		})
	}
}

func inspectFailure(t *testing.T, ctx context.Context, rt jobdb.WorkflowRuntime, g jobdb.GoalInfo) {
	t.Helper()
	key := jobdb.JobKey{TenantId: g.GoalKey.TenantId, JobId: g.FailureJobID}
	info, err := rt.GetJob(ctx, key)
	must(t, err)
	must(t, info.ValidateRecord())
	if info.RecordKind != jobdb.JobRecordGoalFailure || info.GoalID != g.GoalKey.GoalID || info.CellID != g.CellID ||
		info.CellRepo != g.CellRepo || info.Status != jobdb.JobStatusCompleted || info.CompletionStatus != "failed" || info.CompletionDetail != g.Failure.Message ||
		!reflect.DeepEqual(info.TerminalFailure, g.Failure) || len(info.OtherGoalIDs) != 0 {
		t.Fatalf("incorrect protected detail: %+v", info)
	}
	data, err := info.Data.GetData()
	checkFailure(t, data, err, g.Failure)
	listed, err := rt.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{key.TenantId}, GoalIDs: []string{info.GoalID}, IncludeHidden: true})
	must(t, err)
	if len(listed.Jobs) != 3 {
		t.Fatalf("expected two executions and exactly one failure record, got %d", len(listed.Jobs))
	}
	found := false
	for _, row := range listed.Jobs {
		if row.JobKey == key {
			found = true
			must(t, row.ValidateRecord())
			if row.ArchivedAt == nil || row.RecordKind != info.RecordKind || !reflect.DeepEqual(row.TerminalFailure, g.Failure) {
				t.Fatal("list lost protected failure")
			}
		}
	}
	if !found {
		t.Fatal("list omitted failure record")
	}
	chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
	must(t, err)
	if len(chapters) != 0 {
		t.Fatal("failure record invented execution chapters")
	}
	run, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: key, IncludeInputs: true, IncludeOutputs: true, IncludeArtifacts: true, IncludeAttemptInputs: true})
	must(t, err)
	if !reflect.DeepEqual(run.Start, jobdb.JobStart{}) || len(run.Execution) != 0 || len(run.Attempts) != 0 {
		t.Fatal("failure run invented execution history")
	}
	output, err := run.GetOutput(nil, key.TenantId)
	if output != nil {
		t.Fatal("failure run invented output data")
	}
	checkFailure(t, nil, err, g.Failure)
	_, err = rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{Job: jobdb.SubmitRestartJob{PriorJobKey: key, JobID: "forbidden-restart", Cell: g.Cell, CellRepo: g.CellRepo}})
	mustBe(t, err, jobdb.ErrJobRecordOperationUnsupported)
	t.Log("chapter-free detail/list/run returned nil data and typed job/goal failure; restart rejected")
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustBe(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("got %v, want %v", err, target)
	}
}

func checkFailure(t *testing.T, data []byte, err error, expected *jobdb.GoalFailure) {
	t.Helper()
	mustBe(t, err, jobdb.ErrJobFailed)
	mustBe(t, err, jobdb.ErrGoalFailed)
	var failed *jobdb.GoalFailedError
	if data != nil || !errors.As(err, &failed) || !reflect.DeepEqual(failed.Failure, *expected) {
		t.Fatalf("incorrect typed failure output: data=%q error=%v", data, err)
	}
}
