package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/projectwire"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
)

//go:generate go run ../../../../internal/projectspec ../../../..

type projectWireRoute struct {
	method, path, request string
	responses             map[int]string
}

func projectWireJSON(raw []byte, name string, toProject bool) ([]byte, error) {
	return projectwire.JSON(raw, name, toProject)
}
func matchProjectWire(method, path string) (projectWireRoute, bool) {
	for _, r := range projectWireRoutes {
		if r.method != method {
			continue
		}
		a, b := strings.Split(r.path, "/"), strings.Split(path, "/")
		if len(a) != len(b) {
			continue
		}
		match := true
		for i := range a {
			if strings.HasPrefix(a[i], "{") && strings.HasSuffix(a[i], "}") {
				if b[i] == "" {
					match = false
				}
				continue
			}
			if a[i] != b[i] {
				match = false
				break
			}
		}
		if match {
			return r, true
		}
	}
	// Existing protected extensions carry no scope-bearing response envelopes.
	if isProvenancePath(path) && method == http.MethodGet {
		return projectWireRoute{}, true
	}
	return projectWireRoute{}, false
}
func setWirePath(r *http.Request, path string) error {
	decoded, e := url.PathUnescape(path)
	if e != nil {
		return e
	}
	r.URL.Path = decoded
	r.URL.RawPath = path
	return nil
}
func projectLocation(location string, toProject bool) string {
	from, to := "/v1/tenants/", "/v1/projects/"
	if !toProject {
		from, to = to, from
	}
	if strings.HasPrefix(location, from) {
		return to + strings.TrimPrefix(location, from)
	}
	return location
}
func replaceWireBody(r *http.Request, raw []byte) {
	r.Body = io.NopCloser(bytes.NewReader(raw))
	r.ContentLength = int64(len(raw))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(raw)), nil }
}
func readWireBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

// projectResourceHandler invokes the same protected runtime ingress with explicit
// envelope conversion. It never retries, redirects or sends a second mutation.
func projectResourceHandler(legacy http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escaped := r.URL.EscapedPath()
		if !strings.HasPrefix(escaped, "/v1/projects/") {
			legacy.ServeHTTP(w, r)
			return
		}
		parts := strings.Split(escaped, "/")
		if len(parts) < 5 {
			http.NotFound(w, r)
			return
		}
		id, e := url.PathUnescape(parts[3])
		if e != nil || id == "" {
			http.Error(w, "invalid project ID", 400)
			return
		}
		path := strings.Replace(escaped, "/v1/projects/", "/v1/tenants/", 1)
		poll := len(parts) == 6 && parts[4] == "jobs" && parts[5] == "poll"
		if poll {
			path = "/v1/jobs/poll"
		}
		route, ok := matchProjectWire(r.Method, path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		cloned := r.Clone(context.WithValue(r.Context(), projectRequestKey{}, id))
		if e = setWirePath(cloned, path); e != nil {
			http.Error(w, "invalid project route", 400)
			return
		}
		if r.Body != nil {
			raw, e := readWireBody(r)
			if e == nil && poll {
				var body map[string]json.RawMessage
				e = json.Unmarshal(raw, &body)
				if body == nil {
					e = fmt.Errorf("invalid poll body")
				}
				if body["tenantId"] != nil || body["projectId"] != nil {
					e = fmt.Errorf("poll identity belongs in the project path")
				}
				if e == nil {
					scope, _ := json.Marshal(id)
					body["tenantId"] = scope
					raw, e = json.Marshal(body)
				}
			} else if e == nil && projectPathScopedQuery(path) {
				var body map[string]json.RawMessage
				e = json.Unmarshal(raw, &body)
				if body["tenantId"] != nil || body["projectId"] != nil {
					e = fmt.Errorf("query identity belongs in the path")
				}
			} else if e == nil {
				raw, e = projectWireJSON(raw, route.request, false)
			}
			if e != nil {
				http.Error(w, "invalid project envelope", 400)
				return
			}
			replaceWireBody(cloned, raw)
		}
		shape := route.responses
		capture := false
		for _, s := range shape {
			if s != "" {
				capture = true
			}
		}
		if !capture {
			legacy.ServeHTTP(&projectLocationWriter{ResponseWriter: w}, cloned)
			return
		}
		rec := httptest.NewRecorder()
		legacy.ServeHTTP(rec, cloned)
		res := rec.Result()
		defer res.Body.Close()
		raw := rec.Body.Bytes()
		if res.StatusCode < 300 {
			raw, e = projectWireJSON(raw, shape[res.StatusCode], true)
			if e != nil {
				http.Error(w, "project response unavailable", 500)
				return
			}
		}
		for k, vs := range res.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.Header().Del("Content-Length")
		w.Header().Set("Location", projectLocation(w.Header().Get("Location"), true))
		if w.Header().Get("Location") == "" {
			w.Header().Del("Location")
		}
		w.WriteHeader(res.StatusCode)
		_, _ = w.Write(raw)
	})
}

type projectLocationWriter struct{ http.ResponseWriter }

func (w *projectLocationWriter) WriteHeader(code int) {
	if l := w.Header().Get("Location"); l != "" {
		w.Header().Set("Location", projectLocation(l, true))
	}
	w.ResponseWriter.WriteHeader(code)
}

// projectTransport is selected only by NewProjectRuntime. It sends one request
// on the project contract and returns its result without legacy fallback.
type projectTransport struct{ next http.RoundTripper }

func (t projectTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	path := r.URL.EscapedPath()
	i := strings.Index(path, "/v1/tenants/")
	poll := strings.HasSuffix(path, "/v1/jobs/poll")
	if i < 0 && !poll {
		return t.next.RoundTrip(r)
	}
	routePath := path
	if i >= 0 {
		routePath = path[i:]
	}
	if poll {
		routePath = "/v1/jobs/poll"
	}
	route, ok := matchProjectWire(r.Method, routePath)
	if !ok {
		return nil, fmt.Errorf("unsupported project operation")
	}
	q := r.Clone(r.Context())
	var raw []byte
	var e error
	if r.Body != nil {
		raw, e = readWireBody(r)
		if e != nil {
			return nil, e
		}
	}
	if poll {
		var body map[string]json.RawMessage
		if e = json.Unmarshal(raw, &body); e != nil {
			return nil, e
		}
		var id string
		if e = json.Unmarshal(body["tenantId"], &id); e != nil || id == "" {
			return nil, fmt.Errorf("project ID is required")
		}
		delete(body, "tenantId")
		raw, e = json.Marshal(body)
		path = strings.TrimSuffix(path, "/v1/jobs/poll") + "/v1/projects/" + url.PathEscape(id) + "/jobs/poll"
	} else {
		path = path[:i] + strings.Replace(path[i:], "/v1/tenants/", "/v1/projects/", 1)
		if projectPathScopedQuery(routePath) {
			var body map[string]json.RawMessage
			e = json.Unmarshal(raw, &body)
			if e == nil {
				delete(body, "tenantId")
				raw, e = json.Marshal(body)
			}
		} else {
			raw, e = projectWireJSON(raw, route.request, true)
		}
	}
	if e != nil {
		return nil, e
	}
	if e = setWirePath(q, path); e != nil {
		return nil, e
	}
	if r.Body != nil {
		replaceWireBody(q, raw)
	}
	res, e := t.next.RoundTrip(q)
	if e != nil {
		return res, e
	}
	if l := res.Header.Get("Location"); l != "" {
		res.Header.Set("Location", projectLocation(l, false))
	}
	if res.StatusCode >= 300 || route.responses[res.StatusCode] == "" {
		return res, nil
	}
	defer res.Body.Close()
	raw, e = io.ReadAll(res.Body)
	if e != nil {
		return nil, e
	}
	raw, e = projectWireJSON(raw, route.responses[res.StatusCode], false)
	if e != nil {
		return nil, e
	}
	res.Body = io.NopCloser(bytes.NewReader(raw))
	res.ContentLength = int64(len(raw))
	res.Header.Set("Content-Length", strconv.Itoa(len(raw)))
	return res, nil
}

// NewProjectRuntime explicitly selects project resource DTOs and routes. New
// remains the retained tenant client, including for mutations and error retries.
func NewProjectRuntime(baseURL string, client *http.Client, options ...Option) (*Runtime, error) {
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	next := copy.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	copy.Transport = projectTransport{next: next}
	return New(baseURL, &copy, options...)
}

type projectRequestKey struct{}

func projectPathScopedQuery(path string) bool {
	return strings.HasSuffix(path, "/goals/query") || strings.HasSuffix(path, "/dependency-waits/query")
}

// projectExistenceWriter checks successful protected responses, preserving all
// original denials. It also covers streamed artifacts without buffering them.
type projectExistenceWriter struct {
	http.ResponseWriter
	ctx           context.Context
	projects      jobdb.ProjectExistence
	id            string
	wrote, denied bool
}

func (w *projectExistenceWriter) WriteHeader(status int) {
	if w.wrote {
		return
	}
	w.wrote = true
	if status >= 200 && status < 300 {
		err := jobdb.ErrProjectUnavailable
		if w.projects != nil {
			err = w.projects.CheckProject(w.ctx, w.id)
		}
		if err != nil {
			w.denied = true
			w.Header().Del("Content-Length")
			w.Header().Del("ETag")
			w.Header().Del("Location")
			projectError(w.ResponseWriter, err)
			return
		}
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *projectExistenceWriter) Write(p []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	if w.denied {
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}
func (w *projectExistenceWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
