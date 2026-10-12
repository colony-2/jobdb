package remote

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsutil"
)

func isSecretResolutionPath(path string) bool {
	p := strings.Split(path, "/")
	return len(p) == 10 && p[1] == "v1" && p[2] == "tenants" && p[4] == "jobs" && p[6] == "leases" && p[8] == "secrets" && p[9] == "resolve"
}
func (s *proxyServer) secretLimits() jobdb.SecretLimits {
	if r, ok := s.runtime.(interface{ SecretResolutionLimits() jobdb.SecretLimits }); ok {
		return r.SecretResolutionLimits().WithDefaults()
	}
	return (jobdb.SecretLimits{}).WithDefaults()
}

type secretResponse struct {
	result jobdb.SecretResolveResult
	err    error
}

func (r secretResponse) VisitResolveSecretsWithLeaseResponse(w http.ResponseWriter) error {
	if r.err != nil {
		secretsutil.WriteError(w, r.err)
		return nil
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(r.result)
}

func (s *proxyServer) ResolveSecretsWithLease(ctx context.Context, req runtimeapi.ResolveSecretsWithLeaseRequestObject) (runtimeapi.ResolveSecretsWithLeaseResponseObject, error) {
	if req.Body == nil {
		return secretResponse{err: jobdb.ErrInvalidSecretRequest}, nil
	}
	if err := secretsutil.Names(req.Body.Names, s.secretLimits().MaxRequestBytes); err != nil {
		return secretResponse{err: err}, nil
	}
	key := jobdb.JobKey{TenantId: req.TenantId, JobId: req.JobId}
	claims, err := s.validatedLeaseClaims(req.Params.XJobDBLeaseToken, key, req.LeaseId)
	if err != nil {
		return secretResponse{err: jobdb.ErrExecutionLeaseLost}, nil
	}
	result, err := s.runtime.ResolveSecrets(leaseauth.WithClaims(ctx, claims.leaseAuthClaims()), jobdb.ResolveSecretsRequest{
		JobKey: key, LeaseID: req.LeaseId, LeaseToken: req.Params.XJobDBLeaseToken, Names: req.Body.Names,
	})
	if err == nil {
		_, err = s.validatedLeaseClaims(req.Params.XJobDBLeaseToken, key, req.LeaseId)
	}
	return secretResponse{result: result, err: err}, nil
}

type devSecretAdmin interface {
	SetDevSecret(context.Context, string, string, string) error
	RemoveDevSecret(context.Context, string, string) error
}

func (s *proxyServer) devSecretHandler() http.Handler {
	mux := http.NewServeMux()
	admin, ok := s.runtime.(devSecretAdmin)
	if !ok {
		return mux
	}
	for _, op := range []string{"set", "remove"} {
		mux.HandleFunc("POST /dev/v1/tenants/{tenantId}/secrets/"+op, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			var name, value string
			if op == "set" {
				var body struct {
					Name  string  `json:"name"`
					Value *string `json:"value"`
				}
				if err := secretsutil.ReadJSON(r.Body, s.secretLimits().MaxResponseBytes, &body); err != nil {
					secretsutil.WriteError(w, err)
					return
				}
				if body.Value == nil {
					secretsutil.WriteError(w, jobdb.ErrInvalidSecretRequest)
					return
				}
				name, value = body.Name, *body.Value
			} else {
				var body struct {
					Name string `json:"name"`
				}
				if err := secretsutil.ReadJSON(r.Body, s.secretLimits().MaxRequestBytes, &body); err != nil {
					secretsutil.WriteError(w, err)
					return
				}
				name = body.Name
			}
			tenant := r.PathValue("tenantId")
			if tenant == "" || name == "" || !utf8.ValidString(tenant) {
				secretsutil.WriteError(w, jobdb.ErrInvalidSecretRequest)
				return
			}
			var err error
			if op == "set" {
				err = admin.SetDevSecret(r.Context(), tenant, name, value)
			} else {
				err = admin.RemoveDevSecret(r.Context(), tenant, name)
			}
			if err != nil {
				secretsutil.WriteError(w, err)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
	}
	return mux
}

// ResolveSecrets calls the lease-scoped API without exposing raw transport errors
// or response bodies to workflow failure reporting.
func (r *Runtime) ResolveSecrets(ctx context.Context, req jobdb.ResolveSecretsRequest) (jobdb.SecretResolveResult, error) {
	limits := (jobdb.SecretLimits{}).WithDefaults()
	if err := secretsutil.Names(req.Names, limits.MaxRequestBytes); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	httpReq, err := runtimeapi.NewResolveSecretsWithLeaseRequest(r.raw.Server, req.JobKey.TenantId, req.JobKey.JobId, req.LeaseID, &runtimeapi.ResolveSecretsWithLeaseParams{XJobDBLeaseToken: req.LeaseToken}, runtimeapi.ResolveSecretsBody{Names: req.Names})
	if err != nil {
		return jobdb.SecretResolveResult{}, secretsutil.SafeError(err)
	}
	ctx, cancel := context.WithTimeout(ctx, limits.LookupTimeout)
	defer cancel()
	httpReq = httpReq.WithContext(ctx)
	for _, edit := range r.raw.RequestEditors {
		if err := edit(ctx, httpReq); err != nil {
			return jobdb.SecretResolveResult{}, secretsutil.SafeError(err)
		}
	}
	client := r.raw.Client
	if original, ok := client.(*http.Client); ok {
		clone := *original
		clone.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client = &clone
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return jobdb.SecretResolveResult{}, secretsutil.SafeError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return jobdb.SecretResolveResult{}, secretsutil.StatusError(resp.StatusCode)
	}
	// Decode required value fields explicitly: a malformed response must not turn
	// an omitted value into an apparently valid empty credential.
	var body struct {
		Secrets map[string]struct {
			Value     *string          `json:"value"`
			Version   string           `json:"version,omitempty"`
			ExpiresAt *json.RawMessage `json:"expiresAt,omitempty"`
		} `json:"secrets"`
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, limits.MaxResponseBytes+1))
	if err != nil || int64(len(raw)) > limits.MaxResponseBytes || !utf8.Valid(raw) || json.Unmarshal(raw, &body) != nil || body.Secrets == nil {
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
	}
	var result jobdb.SecretResolveResult
	if json.Unmarshal(raw, &result) != nil {
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
	}
	for _, v := range body.Secrets {
		if v.Value == nil {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
		}
	}
	return secretsutil.Result(result, req.Names, limits.MaxResponseBytes)
}

func (l *remoteExecutionLease) ResolveSecrets(ctx context.Context, names []string) (jobdb.SecretResolveResult, error) {
	return l.runtime.ResolveSecrets(ctx, jobdb.ResolveSecretsRequest{JobKey: l.jobKey, LeaseID: l.leaseID, LeaseToken: l.LeaseToken(), Names: names})
}
