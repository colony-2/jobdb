package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

func isConfigurationPath(path string) bool {
	p := strings.Split(path, "/")
	return (len(p) == 5 || len(p) == 6) && p[1] == "v1" && p[2] == "tenants" && p[4] == "configuration" || len(p) == 9 && p[1] == "v1" && p[2] == "tenants" && p[4] == "jobs" && p[6] == "leases" && p[8] == "configuration"
}
func (s *proxyServer) configurationLimits() jobdb.ConfigurationLimits {
	if r, ok := s.runtime.(interface {
		ConfigurationLimits() jobdb.ConfigurationLimits
	}); ok {
		return r.ConfigurationLimits().WithDefaults()
	}
	return (jobdb.ConfigurationLimits{}).WithDefaults()
}

// Validate before the generated handler decodes the body. This bounds memory,
// rejects duplicate keys and enforces absent-versus-null JSON values.
func (s *proxyServer) prepareConfigurationRequest(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Cache-Control", "no-store")
	fail := func(err error) bool { configuration.WriteError(w, err); return false }
	if len(r.URL.Query()) != 0 {
		return fail(jobdb.ErrInvalidConfigurationRequest)
	}
	if len(r.Header.Values("If-Match")) > 1 {
		return fail(jobdb.ErrInvalidConfigurationRequest)
	}
	if r.Method == http.MethodGet {
		if len(strings.Split(r.URL.EscapedPath(), "/")) == 9 && r.Header.Get("X-JobDB-Lease-Token") == "" {
			return fail(jobdb.ErrExecutionLeaseLost)
		}
		if r.Body != nil {
			b, e := io.ReadAll(io.LimitReader(r.Body, 1))
			if e != nil || len(b) > 0 {
				return fail(jobdb.ErrInvalidConfigurationRequest)
			}
		}
		return true
	}
	if r.Body == nil {
		return fail(jobdb.ErrInvalidConfigurationRequest)
	}
	var body any
	switch {
	case strings.HasSuffix(r.URL.EscapedPath(), "/set"):
		body = &runtimeapi.SetConfigurationBody{}
	case strings.HasSuffix(r.URL.EscapedPath(), "/remove"):
		body = &runtimeapi.RemoveConfigurationBody{}
	default:
		return fail(jobdb.ErrInvalidConfigurationRequest)
	}
	if err := configuration.ReadJSON(r.Body, s.configurationLimits().MaxRequestBytes, body); err != nil {
		return fail(err)
	}
	if b, ok := body.(*runtimeapi.SetConfigurationBody); ok && len(b.Value) == 0 {
		return fail(jobdb.ErrInvalidConfigurationRequest)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fail(jobdb.ErrInvalidConfigurationRequest)
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	return true
}

type configurationResponse struct {
	snapshot *jobdb.ConfigurationSnapshot
	revision string
	err      error
}

func (r configurationResponse) write(w http.ResponseWriter) error {
	if r.err != nil {
		configuration.WriteError(w, r.err)
		return nil
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("ETag", configuration.ETag(r.revision))
	if r.snapshot == nil {
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	raw, err := json.Marshal(r.snapshot)
	if err != nil {
		configuration.WriteError(w, jobdb.ErrConfigurationFailed)
		return nil
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(raw)
	return err
}
func (r configurationResponse) VisitGetConfigurationWithLeaseResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (r configurationResponse) VisitGetTenantConfigurationResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (r configurationResponse) VisitSetTenantConfigurationValueResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (r configurationResponse) VisitRemoveTenantConfigurationValueResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *proxyServer) GetConfigurationWithLease(ctx context.Context, req runtimeapi.GetConfigurationWithLeaseRequestObject) (runtimeapi.GetConfigurationWithLeaseResponseObject, error) {
	key := jobdb.JobKey{TenantId: req.TenantId, JobId: req.JobId}
	claims, err := s.validatedLeaseClaims(req.Params.XJobDBLeaseToken, key, req.LeaseId)
	if err != nil {
		return configurationResponse{err: jobdb.ErrExecutionLeaseLost}, nil
	}
	result, err := s.runtime.GetConfiguration(leaseauth.WithClaims(ctx, claims.leaseAuthClaims()), jobdb.GetConfigurationRequest{JobKey: key, LeaseID: req.LeaseId, LeaseToken: req.Params.XJobDBLeaseToken})
	if err == nil {
		err = configuration.ValidateSnapshot(result, s.configurationLimits())
	}
	if err == nil {
		_, err = s.validatedLeaseClaims(req.Params.XJobDBLeaseToken, key, req.LeaseId)
	}
	return configurationResponse{snapshot: &result, revision: result.Revision, err: err}, nil
}
func (s *proxyServer) GetTenantConfiguration(ctx context.Context, req runtimeapi.GetTenantConfigurationRequestObject) (runtimeapi.GetTenantConfigurationResponseObject, error) {
	result, err := s.configurationAdmin.GetTenantConfiguration(ctx, req.TenantId)
	if err == nil {
		err = configuration.ValidateSnapshot(result, s.configurationLimits())
	}
	return configurationResponse{snapshot: &result, revision: result.Revision, err: err}, nil
}
func expectedConfigurationRevision(etag *string) (*string, error) {
	if etag == nil {
		return nil, nil
	}
	revision, err := configuration.ParseETag(*etag)
	return &revision, err
}
func (s *proxyServer) SetTenantConfigurationValue(ctx context.Context, req runtimeapi.SetTenantConfigurationValueRequestObject) (runtimeapi.SetTenantConfigurationValueResponseObject, error) {
	if req.Body == nil || len(req.Body.Value) == 0 {
		return configurationResponse{err: jobdb.ErrInvalidConfigurationRequest}, nil
	}
	expected, err := expectedConfigurationRevision(req.Params.IfMatch)
	if err != nil {
		return configurationResponse{err: err}, nil
	}
	revision, err := s.configurationAdmin.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: req.TenantId, Name: req.Body.Name, Value: req.Body.Value, ExpectedRevision: expected})
	return configurationResponse{revision: revision, err: err}, nil
}
func (s *proxyServer) RemoveTenantConfigurationValue(ctx context.Context, req runtimeapi.RemoveTenantConfigurationValueRequestObject) (runtimeapi.RemoveTenantConfigurationValueResponseObject, error) {
	if req.Body == nil {
		return configurationResponse{err: jobdb.ErrInvalidConfigurationRequest}, nil
	}
	expected, err := expectedConfigurationRevision(req.Params.IfMatch)
	if err != nil {
		return configurationResponse{err: err}, nil
	}
	revision, err := s.configurationAdmin.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: req.TenantId, Name: req.Body.Name, ExpectedRevision: expected})
	return configurationResponse{revision: revision, err: err}, nil
}

var _ jobdb.ConfigurationAdmin = (*Runtime)(nil)

func (r *Runtime) GetConfiguration(ctx context.Context, req jobdb.GetConfigurationRequest) (jobdb.ConfigurationSnapshot, error) {
	if req.JobKey.Validate() != nil || req.LeaseID == "" || req.LeaseToken == "" {
		return jobdb.ConfigurationSnapshot{}, jobdb.ErrExecutionLeaseLost
	}
	httpReq, err := runtimeapi.NewGetConfigurationWithLeaseRequest(r.raw.Server, req.JobKey.TenantId, req.JobKey.JobId, req.LeaseID, &runtimeapi.GetConfigurationWithLeaseParams{XJobDBLeaseToken: req.LeaseToken})
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, configuration.SafeError(err)
	}
	result, _, err := r.configurationRequest(ctx, httpReq, true)
	return result, err
}
func (l *remoteExecutionLease) GetConfiguration(ctx context.Context) (jobdb.ConfigurationSnapshot, error) {
	if l.delegation != nil {
		return jobdb.ConfigurationSnapshot{}, jobdb.ErrLeasePermissionDenied
	}
	return l.runtime.GetConfiguration(ctx, jobdb.GetConfigurationRequest{JobKey: l.jobKey, LeaseID: l.leaseID, LeaseToken: l.LeaseToken()})
}
func (r *Runtime) GetTenantConfiguration(ctx context.Context, tenant string) (jobdb.ConfigurationSnapshot, error) {
	if err := configuration.ValidateTenant(tenant); err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	req, err := runtimeapi.NewGetTenantConfigurationRequest(r.raw.Server, tenant)
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, configuration.SafeError(err)
	}
	result, _, err := r.configurationRequest(ctx, req, true)
	return result, err
}
func configurationETag(revision *string) (*string, error) {
	if revision == nil {
		return nil, nil
	}
	if !configuration.ValidateRevision(*revision) {
		return nil, jobdb.ErrInvalidConfigurationRequest
	}
	tag := configuration.ETag(*revision)
	return &tag, nil
}
func (r *Runtime) SetTenantConfigurationValue(ctx context.Context, in jobdb.SetTenantConfigurationValueRequest) (string, error) {
	limits := r.configurationLimits.WithDefaults()
	for _, err := range []error{configuration.ValidateTenant(in.TenantID), configuration.ValidateName(in.Name, limits), configuration.ValidateValue(in.Value, limits)} {
		if err != nil {
			return "", err
		}
	}
	tag, err := configurationETag(in.ExpectedRevision)
	if err != nil {
		return "", err
	}
	req, err := runtimeapi.NewSetTenantConfigurationValueRequest(r.raw.Server, in.TenantID, &runtimeapi.SetTenantConfigurationValueParams{IfMatch: tag}, runtimeapi.SetConfigurationBody{Name: in.Name, Value: in.Value})
	if err != nil {
		return "", configuration.SafeError(err)
	}
	_, revision, err := r.configurationRequest(ctx, req, false)
	return revision, err
}
func (r *Runtime) RemoveTenantConfigurationValue(ctx context.Context, in jobdb.RemoveTenantConfigurationValueRequest) (string, error) {
	if err := configuration.ValidateTenant(in.TenantID); err != nil {
		return "", err
	}
	// No configured name-size limit on removal, so lowered limits remain recoverable.
	if err := configuration.ValidateName(in.Name, jobdb.ConfigurationLimits{MaxNameBytes: len(in.Name)}); err != nil {
		return "", err
	}
	tag, err := configurationETag(in.ExpectedRevision)
	if err != nil {
		return "", err
	}
	req, err := runtimeapi.NewRemoveTenantConfigurationValueRequest(r.raw.Server, in.TenantID, &runtimeapi.RemoveTenantConfigurationValueParams{IfMatch: tag}, runtimeapi.RemoveConfigurationBody{Name: in.Name})
	if err != nil {
		return "", configuration.SafeError(err)
	}
	_, revision, err := r.configurationRequest(ctx, req, false)
	return revision, err
}
func (r *Runtime) configurationRequest(ctx context.Context, req *http.Request, read bool) (jobdb.ConfigurationSnapshot, string, error) {
	fail := func(err error) (jobdb.ConfigurationSnapshot, string, error) {
		return jobdb.ConfigurationSnapshot{}, "", configuration.SafeError(err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	limits := r.configurationLimits.WithDefaults()
	if req.ContentLength > int64(limits.MaxRequestBytes) {
		return fail(jobdb.ErrConfigurationTooLarge)
	}
	ctx, cancel := context.WithTimeout(ctx, limits.ReadTimeout)
	defer cancel()
	req = req.WithContext(ctx)
	for _, edit := range r.raw.RequestEditors {
		if err := edit(ctx, req); err != nil {
			return fail(err)
		}
	}
	client := r.raw.Client
	if original, ok := client.(*http.Client); ok {
		clone := *original
		clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client = &clone
	}
	resp, err := client.Do(req)
	if err != nil {
		return fail(err)
	}
	defer resp.Body.Close()
	want := http.StatusNoContent
	if read {
		want = http.StatusOK
	}
	if resp.StatusCode != want {
		return fail(configuration.ResponseError(resp.StatusCode, resp.Body))
	}
	revision, err := configuration.ParseETag(resp.Header.Get("ETag"))
	if err != nil {
		return fail(jobdb.ErrConfigurationFailed)
	}
	if !read {
		return jobdb.ConfigurationSnapshot{}, revision, nil
	}
	var result jobdb.ConfigurationSnapshot
	if err := configuration.ReadJSON(resp.Body, limits.MaxSnapshotBytes, &result); err != nil {
		return fail(jobdb.ErrConfigurationFailed)
	}
	if result.Revision != revision {
		return fail(jobdb.ErrConfigurationFailed)
	}
	if err := configuration.ValidateSnapshot(result, limits); err != nil {
		return fail(err)
	}
	if ctx.Err() != nil {
		return fail(ctx.Err())
	}
	return result, revision, nil
}
