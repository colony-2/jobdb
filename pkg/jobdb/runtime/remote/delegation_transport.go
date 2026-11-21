package remote

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Credentials never follow redirects on delegated, secret or configuration
// requests. Macaroon grants need no access-token refresh cache or request replay.
type delegationTransport struct {
	original   *http.Client
	restricted *http.Client
}

func newDelegationTransport(client *http.Client) *delegationTransport {
	if client == nil {
		client = http.DefaultClient
	}
	restricted := *client
	restricted.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &delegationTransport{original: client, restricted: &restricted}
}
func (d *delegationTransport) Do(req *http.Request) (*http.Response, error) {
	client := d.original
	if req.URL.EscapedPath() == "/v1/capabilities/job-goals" || isProvenancePath(req.URL.EscapedPath()) || isPrimaryGoalSelectionPath(req.URL.EscapedPath()) || isDelegationToken(req.Header.Get("X-JobDB-Lease-Token")) || isSecretResolutionPath(req.URL.EscapedPath()) || isConfigurationPath(req.URL.EscapedPath()) || isCellPath(req.URL.EscapedPath()) {
		client = d.restricted
	}
	resp, err := client.Do(req)
	if err != nil {
		if strings.HasSuffix(req.URL.Path, "/keepalive") {
			return resp, err
		}
		key, ordinal := requestInfrastructureSource(req)
		return nil, jobdb.ClassifyRuntimeIO(err, "http_transport", key, ordinal)
	}
	if resp.StatusCode >= 400 {
		resp.Body = infrastructureDiagnosticBody{Reader: io.LimitReader(resp.Body, (16<<10)+1), Closer: resp.Body}
	}
	return resp, nil
}

type requestInfrastructureSourceKey struct{}

type infrastructureSource struct {
	key     jobdb.JobKey
	ordinal *int64
}

// Some writes carry the physical ordinal in their body rather than their route.
// Capture that context before transport classifies an error; later classifiers
// deliberately preserve even an unknown origin on an already typed error.
func withRequestInfrastructureSource(ctx context.Context, key jobdb.JobKey, ordinal *int64) context.Context {
	if ordinal != nil {
		copy := *ordinal
		ordinal = &copy
	}
	return context.WithValue(ctx, requestInfrastructureSourceKey{}, infrastructureSource{key: key, ordinal: ordinal})
}

// Attach the operation's explicit source, or identity carried by its route.
// This context is local only and never serialized into the HTTP request.
func requestInfrastructureSource(req *http.Request) (*jobdb.JobKey, *int64) {
	if source, ok := req.Context().Value(requestInfrastructureSourceKey{}).(infrastructureSource); ok {
		return &source.key, source.ordinal
	}
	_, route, ok := strings.Cut(req.URL.EscapedPath(), "/v1/tenants/")
	if !ok {
		return nil, nil
	}
	parts := strings.Split(route, "/")
	if len(parts) < 3 || parts[1] != "jobs" {
		return nil, nil
	}
	tenant, tenantErr := url.PathUnescape(parts[0])
	job, jobErr := url.PathUnescape(parts[2])
	key := &jobdb.JobKey{TenantId: tenant, JobId: job}
	if tenantErr != nil || jobErr != nil || key.Validate() != nil {
		return nil, nil
	}
	for n := 3; n+1 < len(parts); n++ {
		if parts[n] == "chapters" {
			if ordinal, err := strconv.ParseInt(parts[n+1], 10, 64); err == nil && ordinal >= 0 {
				return key, &ordinal
			}
		}
	}
	return key, nil
}
