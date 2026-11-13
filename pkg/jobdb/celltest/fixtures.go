// Package celltest supplies explicit administrative fixtures for runtime tests.
// It is not installed by production runtimes or authentication middleware.
package celltest

import (
	"context"
	"errors"
	"net/http"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func Context(ctx context.Context) context.Context {
	return jobdb.WithCellAuthority(ctx, jobdb.CellAuthority{Principal: "test-admin", TenantID: "*", Permissions: map[string]bool{"cells.create": true, "cells.get": true, "cells.list": true, "cells.update": true, "cells.retire": true, "jobs.submit": true, "jobs.get": true, "jobs.list": true, "jobs.acquire": true}})
}
func Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { next.ServeHTTP(w, r.WithContext(Context(r.Context()))) })
}

// Submit registers a fixture's cell before submitting its job. Tests of missing
// cells and authorization must call SubmitJob directly instead.
func Submit(rt interface {
	SubmitJob(context.Context, jobdb.SubmitJobRequest) (jobdb.JobHandle, error)
}, ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	if cells, ok := rt.(jobdb.CellRuntime); ok && req.Job.TenantId != "" && req.Job.Cell != "" && req.Job.CellRepo != "" {
		_, err := cells.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: req.Job.TenantId, Name: req.Job.Cell, Repo: req.Job.CellRepo})
		if err != nil && !errors.Is(err, jobdb.ErrCellConflict) {
			return jobdb.JobHandle{}, err
		}
	}
	return rt.SubmitJob(ctx, req)
}

// WithCells supplies explicit cell registration for general workflow fixtures.
// Cell conformance tests use the original runtime so absence remains observable.
func WithCells(rt jobdb.WorkflowRuntime) jobdb.WorkflowRuntime { return fixtureRuntime{rt} }

type fixtureRuntime struct{ jobdb.WorkflowRuntime }

func (r fixtureRuntime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	return Submit(r.WorkflowRuntime, ctx, req)
}
