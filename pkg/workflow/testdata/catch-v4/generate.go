//go:build ignore

// Generate sanitized fixtures from a real public-API execution. Run from the
// repository root: go run pkg/workflow/testdata/catch-v4/generate.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	wf "github.com/colony-2/jobdb/pkg/workflow"
)

type chapter struct {
	Record   json.RawMessage
	Metadata jobdb.ChapterMetadata
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func write(name string, v any) {
	raw, err := json.MarshalIndent(v, "", "  ")
	must(err)
	must(os.WriteFile("pkg/workflow/testdata/catch-v4/"+name, append(raw, '\n'), 0644))
}
func main() {
	ctx := context.Background()
	rt := toy.New()
	w := wf.ExecutionWorker{JobType: "fixture", Execute: func(c wf.ExecutionContext, in jobdb.JobData) (jobdb.JobData, error) {
		return c.DoScope("caught", wf.ScopeOptions{Catch: []wf.CatchRule{{ID: "recover", Match: func(error) bool { return true }, Handle: func(wf.ExecutionContext, error) (wf.CatchResult, error) {
			return wf.CatchResult{Disposition: wf.CatchHandled, Output: jobdb.NewTaskDataOrPanic("recovered")}, nil
		}}}}, func(wf.ExecutionContext) (jobdb.JobData, error) { return nil, errors.New("original") })
	}}
	key, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "fixture", JobType: w.Name(), Data: jobdb.NewTaskDataOrPanic("input")}})
	must(err)
	run, err := wf.GetJobForRun(ctx, rt, wf.GetJobForRunRequest{JobKey: key.JobKey, JobWorker: w, WorkerID: "fixture", LeaseDuration: time.Minute})
	must(err)
	_, err = run.Run(nil)
	must(err)
	chapters, err := rt.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key.JobKey})
	must(err)
	// All payloads and identities are intentionally synthetic, with no secrets,
	// artifacts or references to generated job IDs. Keep original engine bytes.
	var compact []chapter
	for _, ch := range chapters {
		if body, ok := ch.Body.(jobdb.ExecutionChapter); ok {
			compact = append(compact, chapter{body.Record, ch.Metadata})
		}
	}
	write("compact.json", compact)
	info, err := jobdb.GetJobRun(ctx, rt, jobdb.GetJobRunRequest{JobKey: key.JobKey})
	must(err)
	var full []jobdb.ExecutionRecord
	for _, e := range info.Execution {
		full = append(full, e.Record)
	}
	write("full.json", full)
}
