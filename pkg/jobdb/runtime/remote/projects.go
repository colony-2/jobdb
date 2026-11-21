package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
)

func projectError(w http.ResponseWriter, err error) {
	code, status := "project_unavailable", http.StatusServiceUnavailable
	switch {
	case errors.Is(err, jobdb.ErrProjectAccessDenied):
		code, status = "project_access_denied", 403
	case errors.Is(err, jobdb.ErrInvalidProject):
		code, status = "invalid_project", 400
	case errors.Is(err, jobdb.ErrProjectNotFound):
		code, status = "project_not_found", 404
	case errors.Is(err, jobdb.ErrProjectConflict):
		code, status = "project_conflict", 409
	case errors.Is(err, jobdb.ErrProjectPreconditionRequired):
		code, status = "project_precondition_required", 428
	case errors.Is(err, jobdb.ErrProjectRevisionMismatch):
		code, status = "project_revision_mismatch", 412
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "message": code})
}
func projectCatalogHandler(runtime any, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.EscapedPath()
		collection := path == "/v1/projects"
		parts := strings.Split(path, "/")
		single := len(parts) == 4 && parts[1] == "v1" && parts[2] == "projects" && parts[3] != ""
		if !collection && !single {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		// An execution credential never supplies catalog authority, including through
		// an embedding's broad development administration middleware.
		if r.Header.Get("X-JobDB-Lease-Token") != "" || isDelegationToken(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")) {
			projectError(w, jobdb.ErrProjectAccessDenied)
			return
		}
		if e := jobdb.RequireProjectAdministration(r.Context()); e != nil {
			projectError(w, e)
			return
		}
		catalog, ok := runtime.(jobdb.ProjectCatalog)
		if !ok || catalog == nil {
			projectError(w, jobdb.ErrProjectUnavailable)
			return
		}
		var result any
		status := 200
		var err error
		switch {
		case collection && r.Method == http.MethodPost:
			var q jobdb.CreateProjectRequest
			q, err = projectcatalog.DecodeCreate(r.Body)
			if err == nil {
				var receipt jobdb.ProjectCreation
				receipt, err = catalog.CreateProject(r.Context(), q)
				if err == nil {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("ETag", receipt.ETag)
					w.Header().Set("Location", receipt.Location)
					w.WriteHeader(receipt.Status)
					_, _ = w.Write(receipt.Body)
					return
				}
			}
		case collection && r.Method == http.MethodGet:
			q := jobdb.ListProjectsRequest{Name: r.URL.Query().Get("name"), PageToken: r.URL.Query().Get("pageToken")}
			if v := r.URL.Query().Get("pageSize"); v != "" {
				q.PageSize, err = strconv.Atoi(v)
				if err != nil {
					err = jobdb.ErrInvalidProject
				}
			}
			if err == nil {
				result, err = catalog.ListProjects(r.Context(), q)
			}
		case single && (r.Method == http.MethodGet || r.Method == http.MethodDelete):
			var id string
			id, err = url.PathUnescape(parts[3])
			if err != nil || strings.Contains(id, "/") {
				err = jobdb.ErrInvalidProject
			} else if r.Method == http.MethodGet {
				var p jobdb.Project
				p, err = catalog.GetProject(r.Context(), id)
				if err == nil {
					result = p
					w.Header().Set("ETag", strconv.Quote(p.Revision))
				}
			} else {
				if len(r.Header.Values("If-Match")) > 1 {
					err = jobdb.ErrInvalidProject
				} else {
					err = catalog.DeleteProject(r.Context(), jobdb.DeleteProjectRequest{ID: id, IfMatch: r.Header.Get("If-Match")})
					status = 204
				}
			}
		default:
			w.Header().Set("Allow", "GET, POST, DELETE")
			w.WriteHeader(405)
			return
		}
		if err != nil {
			projectError(w, err)
			return
		}
		if status == 204 {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(result)
	})
}
func projectResponseError(status int) error {
	switch status {
	case 400:
		return jobdb.ErrInvalidProject
	case 401, 403:
		return jobdb.ErrProjectAccessDenied
	case 404:
		return jobdb.ErrProjectNotFound
	case 409:
		return jobdb.ErrProjectConflict
	case 412:
		return jobdb.ErrProjectRevisionMismatch
	case 428:
		return jobdb.ErrProjectPreconditionRequired
	}
	return jobdb.ErrProjectUnavailable
}
func (r *Runtime) projectRequest(ctx context.Context, method, path string, body []byte, etag string) (*http.Response, []byte, error) {
	req, e := http.NewRequestWithContext(ctx, method, strings.TrimRight(r.raw.Server, "/")+path, bytes.NewReader(body))
	if e != nil {
		return nil, nil, e
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	res, e := r.delegations.restricted.Do(req)
	if e != nil {
		return nil, nil, e
	}
	defer res.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(res.Body, 20<<20))
	if e != nil {
		return nil, nil, e
	}
	if res.StatusCode >= 300 {
		return nil, nil, projectResponseError(res.StatusCode)
	}
	return res, raw, nil
}
func (r *Runtime) CreateProject(ctx context.Context, q jobdb.CreateProjectRequest) (jobdb.ProjectCreation, error) {
	raw, e := json.Marshal(q)
	if e != nil {
		return jobdb.ProjectCreation{}, jobdb.ErrInvalidProject
	}
	res, raw, e := r.projectRequest(ctx, "POST", "/v1/projects", raw, "")
	if e != nil {
		return jobdb.ProjectCreation{}, e
	}
	var p jobdb.Project
	if res.StatusCode != 201 || json.Unmarshal(raw, &p) != nil || p.ID == "" {
		return jobdb.ProjectCreation{}, jobdb.ErrProjectUnavailable
	}
	return jobdb.ProjectCreation{Project: p, Body: raw, Status: res.StatusCode, ETag: res.Header.Get("ETag"), Location: res.Header.Get("Location")}, nil
}
func (r *Runtime) GetProject(ctx context.Context, id string) (jobdb.Project, error) {
	_, raw, e := r.projectRequest(ctx, "GET", "/v1/projects/"+url.PathEscape(id), nil, "")
	if e != nil {
		return jobdb.Project{}, e
	}
	var out jobdb.Project
	if e = json.Unmarshal(raw, &out); e != nil {
		return out, fmt.Errorf("invalid project response: %w", e)
	}
	return out, nil
}
func (r *Runtime) ListProjects(ctx context.Context, q jobdb.ListProjectsRequest) (jobdb.ListProjectsResponse, error) {
	values := url.Values{}
	if q.Name != "" {
		values.Set("name", q.Name)
	}
	if q.PageToken != "" {
		values.Set("pageToken", q.PageToken)
	}
	if q.PageSize != 0 {
		values.Set("pageSize", strconv.Itoa(q.PageSize))
	}
	_, raw, e := r.projectRequest(ctx, "GET", "/v1/projects?"+values.Encode(), nil, "")
	var out jobdb.ListProjectsResponse
	if e == nil {
		e = json.Unmarshal(raw, &out)
	}
	return out, e
}
func (r *Runtime) DeleteProject(ctx context.Context, q jobdb.DeleteProjectRequest) error {
	res, _, e := r.projectRequest(ctx, "DELETE", "/v1/projects/"+url.PathEscape(q.ID), nil, q.IfMatch)
	if e == nil && res.StatusCode != 204 {
		return jobdb.ErrProjectUnavailable
	}
	return e
}
func isProjectError(err error) bool {
	for _, e := range []error{jobdb.ErrInvalidProject, jobdb.ErrProjectNotFound, jobdb.ErrProjectUnavailable, jobdb.ErrProjectConflict, jobdb.ErrProjectAccessDenied, jobdb.ErrProjectPreconditionRequired, jobdb.ErrProjectRevisionMismatch} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}

func projectBodyError(raw []byte) error {
	var body struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(raw, &body) != nil {
		return nil
	}
	switch body.Code {
	case "invalid_project":
		return jobdb.ErrInvalidProject
	case "project_not_found":
		return jobdb.ErrProjectNotFound
	case "project_unavailable":
		return jobdb.ErrProjectUnavailable
	case "project_conflict":
		return jobdb.ErrProjectConflict
	case "project_access_denied":
		return jobdb.ErrProjectAccessDenied
	case "project_precondition_required":
		return jobdb.ErrProjectPreconditionRequired
	case "project_revision_mismatch":
		return jobdb.ErrProjectRevisionMismatch
	}
	return nil
}
