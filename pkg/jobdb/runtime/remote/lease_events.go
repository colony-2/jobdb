package remote

import (
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"net/http"
	"strings"
)

func (r cellResponse) VisitListLeaseEventsResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *proxyServer) ListLeaseEvents(ctx context.Context, q runtimeapi.ListLeaseEventsRequestObject) (runtimeapi.ListLeaseEventsResponseObject, error) {
	var req jobdb.ListLeaseEventsRequest
	raw, _ := ctx.Value(requestBodyKey{}).([]byte)
	if len(raw) == 0 || json.Unmarshal(raw, &req) != nil {
		return cellResponse{err: jobdb.ErrInvalidLeaseEventRequest}, nil
	}
	req.TenantID = q.TenantId
	req, err := jobdb.NormalizeLeaseEventRequest(req)
	if err != nil {
		return cellResponse{err: err}, nil
	}
	if err = jobdb.AuthorizeCell(ctx, req.TenantID, "", "jobs.list"); err != nil {
		return cellResponse{err: err}, nil
	}
	out, err := jobdb.ListLeaseEvents(ctx, s.runtime, req)
	return cellResponse{body: out, err: err}, nil
}
func (r *Runtime) ListLeaseEvents(ctx context.Context, req jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	req, err := jobdb.NormalizeLeaseEventRequest(req)
	if err != nil {
		return jobdb.ListLeaseEventsResponse{}, err
	}
	body, err := convertCellJSON[jobdb.ListLeaseEventsRequest, runtimeapi.ListLeaseEventsRequest](req)
	if err != nil {
		return jobdb.ListLeaseEventsResponse{}, err
	}
	resp, err := r.raw.ListLeaseEvents(ctx, req.TenantID, body)
	if err != nil {
		return jobdb.ListLeaseEventsResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var envelope struct{ Code string }
		_ = json.NewDecoder(resp.Body).Decode(&envelope)
		switch envelope.Code {
		case "lease_events_unsupported":
			return jobdb.ListLeaseEventsResponse{}, jobdb.ErrLeaseEventsUnsupported
		case "invalid_lease_event_page_token":
			return jobdb.ListLeaseEventsResponse{}, jobdb.ErrInvalidLeaseEventPageToken
		case "invalid_request":
			return jobdb.ListLeaseEventsResponse{}, jobdb.ErrInvalidLeaseEventRequest
		case "not_found":
			return jobdb.ListLeaseEventsResponse{}, jobdb.ErrCellNotFound
		case "forbidden":
			return jobdb.ListLeaseEventsResponse{}, jobdb.ErrCellForbidden
		case "lease_permission_denied":
			return jobdb.ListLeaseEventsResponse{}, jobdb.ErrLeasePermissionDenied
		}
		return jobdb.ListLeaseEventsResponse{}, responseError("list lease events", resp.StatusCode, nil, nil)
	}
	var out jobdb.ListLeaseEventsResponse
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}
func isLeaseEventsPath(path string) bool {
	p := strings.Split(path, "/")
	return len(p) == 6 && p[1] == "v1" && p[2] == "tenants" && p[4] == "lease-events" && p[5] == "query"
}
