package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"os"
	"time"
)

func main() {
	ctx := celltest.Context(context.Background())
	r, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: os.Args[1]})
	if err != nil {
		panic(err)
	}
	defer r.Close(ctx)

	if os.Args[2] == "historical-before" {
		must := func(e error) {
			if e != nil {
				panic(e)
			}
		}
		saved, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "historical-live", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: &jobdb.SimpleTaskData{Data: []byte(`{"saved":true}`), Artifacts: []jobdb.Artifact{jobdb.NewArtifactFromBytes("saved.bin", []byte("historical artifact bytes\x00"))}}}})
		must(e)
		_, e = celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "historical-dependent", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(2), Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: saved.JobKey.JobId}, Condition: jobdb.JobPrereqCondition("success")}}}})
		must(e)
		_, e = r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: saved.JobKey, WorkerID: "historical-live", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Hour})
		must(e)
		_, e = r.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "saved", Value: json.RawMessage(`{"historical":true}`)})
		must(e)
		_, e = celltest.Schedule(ctx, r, jobdb.UpsertScheduleRequest{TenantId: "tenant", ScheduleId: "historical-schedule", RequestTime: time.Now().UTC(), OverlapPolicy: jobdb.ScheduleOverlapSerial, Trigger: jobdb.ScheduleTrigger{Kind: jobdb.ScheduleTriggerInterval, Interval: time.Hour}, Target: jobdb.ScheduleTarget{Cell: "test", CellRepo: "example.test/test", JobType: "scheduled", Data: jobdb.JobData(jobdb.NewTaskDataOrPanic(3))}})
		must(e)
		// Rename and retain the old alias while preserving the immutable cell ID.
		c, e := r.GetCell(ctx, jobdb.CellKey{TenantID: "tenant", CellID: saved.CellID})
		must(e)
		name := "renamed"
		_, e = r.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision), Name: &name})
		must(e)
	}
	h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: os.Args[2], Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	if err != nil {
		panic(err)
	}
	l, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "historical", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	if err != nil {
		panic(err)
	}
	if err = l.KeepAlive(ctx); err != nil {
		panic(err)
	}
	if err = l.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "job", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`2`)}}}}}); err != nil {
		panic(err)
	}
}
