package runtimecore

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

func (r *Runtime) ListLeaseEvents(ctx context.Context, req jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	if err := r.validate(); err != nil {
		return jobdb.ListLeaseEventsResponse{}, err
	}
	req, err := jobdb.NormalizeLeaseEventRequest(req)
	if err != nil {
		return jobdb.ListLeaseEventsResponse{}, err
	}
	if err = jobdb.AuthorizeCell(ctx, req.TenantID, "", "jobs.list"); err != nil {
		return jobdb.ListLeaseEventsResponse{}, err
	}
	reader, ok := r.scheduler.(jobdb.LeaseEventReader)
	if !ok {
		return jobdb.ListLeaseEventsResponse{}, jobdb.ErrLeaseEventsUnsupported
	}
	return reader.ListLeaseEvents(ctx, req)
}
