package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

func convertCellJSON[A, B any](v A) (B, error) {
	var out B
	raw, e := json.Marshal(v)
	if e == nil {
		e = json.Unmarshal(raw, &out)
	}
	return out, e
}

type cellResponse struct {
	body     any
	err      error
	status   int
	revision string
	location string
}

func (r cellResponse) write(w http.ResponseWriter) error {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	if r.err != nil {
		if writeCellError(w, r.err) {
			return nil
		}
		return r.err
	}
	if r.revision != "" {
		w.Header().Set("ETag", fmt.Sprintf("%q", r.revision))
	}
	if r.location != "" {
		w.Header().Set("Location", r.location)
	}
	if r.status == 0 {
		r.status = 200
	}
	w.WriteHeader(r.status)
	return json.NewEncoder(w).Encode(r.body)
}
func (r cellResponse) VisitCreateCellResponse(w http.ResponseWriter) error { return r.write(w) }
func (r cellResponse) VisitGetCellResponse(w http.ResponseWriter) error    { return r.write(w) }
func (r cellResponse) VisitListCellsResponse(w http.ResponseWriter) error  { return r.write(w) }
func (r cellResponse) VisitUpdateCellResponse(w http.ResponseWriter) error { return r.write(w) }
func (r cellResponse) VisitRetireCellResponse(w http.ResponseWriter) error { return r.write(w) }
func (r cellResponse) VisitResolveCellWithLeaseResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func writeCellError(w http.ResponseWriter, e error) bool {
	status, code, ok := jobdb.CellErrorStatus(e)
	if !ok {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": code})
	return true
}
func (s *proxyServer) CreateCell(ctx context.Context, q runtimeapi.CreateCellRequestObject) (runtimeapi.CreateCellResponseObject, error) {
	if q.Body == nil {
		return nil, jobdb.ErrInvalidCell
	}
	r, e := convertCellJSON[runtimeapi.CreateCellRequest, jobdb.CreateCellRequest](*q.Body)
	if e != nil {
		return nil, e
	}
	r.TenantID = q.TenantId
	c, e := s.runtime.CreateCell(ctx, r)
	return cellResponse{body: c, err: e, revision: c.Revision, status: 201, location: "/v1/tenants/" + url.PathEscape(q.TenantId) + "/cells/" + url.PathEscape(c.ID)}, nil
}
func (s *proxyServer) GetCell(ctx context.Context, q runtimeapi.GetCellRequestObject) (runtimeapi.GetCellResponseObject, error) {
	c, e := s.runtime.GetCell(ctx, jobdb.CellKey{TenantID: q.TenantId, CellID: q.CellId})
	return cellResponse{body: c, err: e, revision: c.Revision}, nil
}
func (s *proxyServer) ListCells(ctx context.Context, q runtimeapi.ListCellsRequestObject) (runtimeapi.ListCellsResponseObject, error) {
	r := jobdb.ListCellsRequest{TenantID: q.TenantId}
	if q.Params.Name != nil {
		r.Name = *q.Params.Name
	}
	if q.Params.State != nil {
		r.State = string(*q.Params.State)
	}
	if q.Params.PageSize != nil {
		r.PageSize = int(*q.Params.PageSize)
	}
	if q.Params.PageToken != nil {
		r.PageToken = *q.Params.PageToken
	}
	c, e := s.runtime.ListCells(ctx, r)
	return cellResponse{body: c, err: e}, nil
}
func (s *proxyServer) UpdateCell(ctx context.Context, q runtimeapi.UpdateCellRequestObject) (runtimeapi.UpdateCellResponseObject, error) {
	if q.Body == nil {
		return nil, jobdb.ErrInvalidCell
	}
	r, e := convertCellJSON[runtimeapi.UpdateCellRequest, jobdb.UpdateCellRequest](*q.Body)
	if e != nil {
		return nil, e
	}
	r.CellKey = jobdb.CellKey{TenantID: q.TenantId, CellID: q.CellId}
	if q.Params.IfMatch != nil {
		r.IfMatch = *q.Params.IfMatch
	}
	c, e := s.runtime.UpdateCell(ctx, r)
	return cellResponse{body: c, err: e, revision: c.Revision}, nil
}
func (s *proxyServer) RetireCell(ctx context.Context, q runtimeapi.RetireCellRequestObject) (runtimeapi.RetireCellResponseObject, error) {
	c, e := s.runtime.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: q.TenantId, CellID: q.CellId}, IfMatch: cellIfMatch(q.Params.IfMatch)})
	return cellResponse{body: c, err: e, revision: c.Revision}, nil
}
func (s *proxyServer) ResolveCellWithLease(ctx context.Context, q runtimeapi.ResolveCellWithLeaseRequestObject) (runtimeapi.ResolveCellWithLeaseResponseObject, error) {
	if q.Body == nil {
		return nil, jobdb.ErrInvalidCell
	}
	key := jobdb.JobKey{TenantId: q.TenantId, JobId: q.ParentJobId}
	claims, e := s.validatedLeaseClaims(ctx, q.Params.XJobDBLeaseToken, key, q.LeaseId)
	if e != nil {
		return nil, e
	}
	ops, ok := s.runtime.(interface {
		ResolveCellWithLeaseByID(context.Context, jobdb.JobKey, string, string, string) (jobdb.ResolvedCell, error)
	})
	if !ok {
		return nil, fmt.Errorf("cell lease resolver missing")
	}
	c, e := ops.ResolveCellWithLeaseByID(ctx, key, q.LeaseId, claims.WorkerID, q.Body.Cell)
	return cellResponse{body: c, err: e}, nil
}
func decodeCellResponse[T any](resp *http.Response, err error) (T, error) {
	var out T
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var body struct{ Code string }
		_ = json.NewDecoder(resp.Body).Decode(&body)
		if body.Code == "" {
			return out, fmt.Errorf("cell request: HTTP %d", resp.StatusCode)
		}
		return out, &jobdb.CellError{Code: body.Code, Status: resp.StatusCode}
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}
func (r *Runtime) CreateCell(ctx context.Context, q jobdb.CreateCellRequest) (jobdb.Cell, error) {
	body, e := convertCellJSON[jobdb.CreateCellRequest, runtimeapi.CreateCellRequest](q)
	if e != nil {
		return jobdb.Cell{}, e
	}
	return decodeCellResponse[jobdb.Cell](r.raw.CreateCell(ctx, q.TenantID, body))
}
func (r *Runtime) GetCell(ctx context.Context, q jobdb.CellKey) (jobdb.Cell, error) {
	return decodeCellResponse[jobdb.Cell](r.raw.GetCell(ctx, q.TenantID, q.CellID))
}
func (r *Runtime) ListCells(ctx context.Context, q jobdb.ListCellsRequest) (jobdb.ListCellsResponse, error) {
	params := runtimeapi.ListCellsParams{}
	if q.Name != "" {
		params.Name = &q.Name
	}
	if q.State != "" {
		state := runtimeapi.ListCellsParamsState(q.State)
		params.State = &state
	}
	if q.PageSize != 0 {
		size := int32(q.PageSize)
		params.PageSize = &size
	}
	if q.PageToken != "" {
		params.PageToken = &q.PageToken
	}
	return decodeCellResponse[jobdb.ListCellsResponse](r.raw.ListCells(ctx, q.TenantID, &params))
}
func (r *Runtime) UpdateCell(ctx context.Context, q jobdb.UpdateCellRequest) (jobdb.Cell, error) {
	body, e := convertCellJSON[jobdb.UpdateCellRequest, runtimeapi.UpdateCellRequest](q)
	if e != nil {
		return jobdb.Cell{}, e
	}
	return decodeCellResponse[jobdb.Cell](r.raw.UpdateCell(ctx, q.TenantID, q.CellID, &runtimeapi.UpdateCellParams{IfMatch: &q.IfMatch}, body))
}
func (r *Runtime) RetireCell(ctx context.Context, q jobdb.RetireCellRequest) (jobdb.Cell, error) {
	return decodeCellResponse[jobdb.Cell](r.raw.RetireCell(ctx, q.TenantID, q.CellID, &runtimeapi.RetireCellParams{IfMatch: &q.IfMatch}))
}
func (l *remoteExecutionLease) ResolveCell(ctx context.Context, name string) (jobdb.ResolvedCell, error) {
	return decodeCellResponse[jobdb.ResolvedCell](l.runtime.raw.ResolveCellWithLease(ctx, l.jobKey.TenantId, l.jobKey.JobId, l.leaseID, &runtimeapi.ResolveCellWithLeaseParams{XJobDBLeaseToken: l.LeaseToken()}, runtimeapi.ResolveCellWithLeaseJSONRequestBody{Cell: name}))
}

// Validate the raw cell envelope before decoding optional pointers, preserving
// the distinction between omitted properties and explicit null.
func prepareCellRequest(w http.ResponseWriter, r *http.Request) bool {
	if !strings.Contains(r.URL.Path, "/cells") {
		return true
	}
	fail := func(e error) bool { writeCellError(w, e); return false }
	if len(r.Header.Values("If-Match")) > 1 {
		return fail(jobdb.ErrInvalidCell)
	}

	if r.Method != http.MethodPost && r.Method != http.MethodPatch {
		return true
	}
	if strings.HasSuffix(r.URL.Path, "/retire") {
		return true
	}
	if r.Body == nil {
		return fail(jobdb.ErrInvalidCell)
	}
	var fields map[string]json.RawMessage
	if e := configuration.ReadJSON(r.Body, 32<<10, &fields); e != nil {
		return fail(jobdb.ErrInvalidCell)
	}
	for _, v := range fields {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return fail(jobdb.ErrInvalidCell)
		}
	}
	raw, _ := json.Marshal(fields)
	r.Body = io.NopCloser(bytes.NewReader(raw))
	return true
}

func cellIfMatch(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
