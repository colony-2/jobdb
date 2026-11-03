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
	return jobdb.WithCellAuthority(ctx, jobdb.CellAuthority{Principal: "test-admin", TenantID: "*", Permissions: map[string]bool{"cells.create": true, "cells.bootstrapRoot": true, "cells.get": true, "cells.list": true, "cells.update": true, "cells.retire": true, "jobs.submit": true, "jobs.get": true, "jobs.list": true, "jobs.acquire": true}})
}
func Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { next.ServeHTTP(w, r.WithContext(Context(r.Context()))) })
}

// Schedule prepares the existing target required by schedule fixtures. Production
// UpsertSchedule never registers a missing cell.
func Schedule(ctx context.Context, rt interface {
	jobdb.CellRuntime
	UpsertSchedule(context.Context, jobdb.UpsertScheduleRequest) (jobdb.ScheduleInfo, error)
}, req jobdb.UpsertScheduleRequest) (jobdb.ScheduleInfo, error) {
	if req.Target.Cell != "" && req.Target.CellRepo != "" {
		_, err := rt.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: req.TenantId, Name: req.Target.Cell, Repo: req.Target.CellRepo})
		if err != nil && !errors.Is(err, jobdb.ErrCellConflict) {
			return jobdb.ScheduleInfo{}, err
		}
	}
	return rt.UpsertSchedule(ctx, req)
}
