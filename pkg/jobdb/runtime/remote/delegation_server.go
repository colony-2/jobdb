package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"github.com/google/uuid"
)

type delegationError struct {
	status int
	code   string
}

// NewDelegationServerWithOptions exposes only delegated lease operations. Use
// this handler on an ingress available to isolated workers, keeping the regular
// owner/admin handler behind a separate access boundary. Both handlers must use
// the same backend and signing key. Missing credentials and owner tokens are
// rejected, including on APIs that normally rely on deployment authorization.
func NewDelegationServerWithOptions(runtime jobdb.WorkflowRuntime, opts ServerOptions) (http.Handler, error) {
	handler, err := NewServerWithOptions(runtime, opts)
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isDelegationToken(r.Header.Get("X-JobDB-Lease-Token")) {
			writeDelegationError(w, &delegationError{401, "invalid_delegation_credential"})
			return
		}
		handler.ServeHTTP(w, r)
	}), nil
}

func (e *delegationError) Error() string { return e.code }
func delegationDenied() error            { return &delegationError{403, "lease_permission_denied"} }
func writeDelegationError(w http.ResponseWriter, err error) bool {
	var e *delegationError
	if !errors.As(err, &e) {
		return false
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": e.code, "message": e.code})
	return true
}

func (s *proxyServer) parentValidity(ctx context.Context, p leaseTokenClaims) (jobdb.LeaseValidity, error) {
	checker, ok := s.runtime.(interface {
		ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
	})
	if !ok {
		return jobdb.LeaseValidity{}, &delegationError{501, "delegation_not_supported"}
	}
	v, err := checker.ValidateExecutionLeaseByID(ctx, jobdb.JobKey{TenantId: p.TenantID, JobId: p.JobID}, p.LeaseID, p.WorkerID)
	if err != nil {
		if errors.Is(err, jobdb.ErrExecutionLeaseLost) || errors.Is(err, jobdb.ErrJobNotFound) {
			return v, &delegationError{409, "execution_lease_lost"}
		}
		return v, &delegationError{503, "delegation_unavailable"}
	}
	if !v.ExpiresAt.After(time.Now()) {
		return v, &delegationError{409, "execution_lease_lost"}
	}
	return v, nil
}

func (s *proxyServer) checkedDelegation(ctx context.Context, token string, key jobdb.JobKey, leaseID string, allowExpired bool) (delegationClaims, jobdb.LeaseValidity, error) {
	c, err := s.tokens.parseDelegation(token)
	if err != nil || c.key() != key || c.Parent.LeaseID != leaseID {
		return c, jobdb.LeaseValidity{}, &delegationError{401, "invalid_delegation_credential"}
	}
	v, err := s.parentValidity(ctx, c.Parent)
	if err != nil {
		return c, v, err
	}
	if c.Grant.NotAfter != nil && !c.Grant.NotAfter.After(time.Now()) {
		return c, v, &delegationError{403, "delegation_deadline_reached"}
	}
	if !allowExpired && !c.ExpiresAt.After(time.Now()) {
		return c, v, &delegationError{409, "execution_lease_lost"}
	}
	return c, v, nil
}

// This boundary runs BEFORE all routing, including dev/admin and unscoped APIs.
// The expired-token response is produced only here, before any operation starts.
// It is the only response on which clients may replay an operation after renewal.
func (s *proxyServer) prepareDelegationRequest(w http.ResponseWriter, r *http.Request) bool {
	token := r.Header.Get("X-JobDB-Lease-Token")
	if !isDelegationToken(token) {
		return true
	}
	w.Header().Set("Cache-Control", "no-store")
	reject := func(err error) bool { writeDelegationError(w, err); return false }
	if len(r.Header.Values("X-JobDB-Lease-Token")) != 1 {
		return reject(&delegationError{401, "invalid_delegation_credential"})
	}
	p := strings.Split(r.URL.EscapedPath(), "/")
	if len(p) < 8 || p[1] != "v1" || p[2] != "tenants" || p[4] != "jobs" || p[6] != "leases" {
		return reject(delegationDenied())
	}
	tenant, e1 := url.PathUnescape(p[3])
	job, e2 := url.PathUnescape(p[5])
	lease, e3 := url.PathUnescape(p[7])
	if e1 != nil || e2 != nil || e3 != nil {
		return reject(delegationDenied())
	}
	c, _, err := s.checkedDelegation(r.Context(), token, jobdb.JobKey{TenantId: tenant, JobId: job}, lease, true)
	if err != nil {
		return reject(err)
	}
	suffix := strings.Join(p[8:], "/")
	policy := c.Grant.Permissions
	// Renewal is the only action that may use a token past access expiry.
	if r.Method == http.MethodPost && suffix == "keepalive" {
		return true
	}
	allowed := false
	switch {
	case r.Method == http.MethodGet && len(p) == 8:
		allowed = true
	case r.Method == http.MethodGet && suffix == "chapters":
		allowed = policy.Read != nil
	case r.Method == http.MethodGet && len(p) == 10 && p[8] == "chapters":
		ordinal, err := strconv.ParseInt(p[9], 10, 64)
		allowed = err == nil && chaptersAllow(policy.Read, ordinal)
	case r.Method == http.MethodPost && suffix == "secrets/resolve":
		allowed = len(policy.Secrets) > 0
	case r.Method == http.MethodPost && suffix == "add_chapter":
		allowed = policy.Write != nil
	case r.Method == http.MethodPost && suffix == "jobs":
		allowed = policy.Submit != nil && policy.Submit.JobIDs.Any
	case r.Method == http.MethodPut && len(p) == 10 && p[8] == "jobs":
		id, err := url.PathUnescape(p[9])
		allowed = err == nil && policy.Submit != nil && namesAllow(policy.Submit.JobIDs, id)
	}
	if !allowed {
		return reject(delegationDenied())
	}
	if !c.ExpiresAt.After(time.Now()) {
		return reject(&delegationError{401, "delegation_access_token_expired"})
	}
	if r.Method == http.MethodGet || suffix == "secrets/resolve" {
		return true
	}
	// Bound buffering needed to authorize selectors before any runtime side effects.
	if r.Body == nil {
		return reject(&delegationError{400, "invalid_delegation_request"})
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, (64<<20)+1))
	if err != nil || len(raw) > 64<<20 {
		return reject(&delegationError{400, "invalid_delegation_request"})
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	if suffix == "add_chapter" {
		var body runtimeapi.AddChapterRequest
		if json.Unmarshal(raw, &body) != nil {
			return reject(&delegationError{400, "invalid_delegation_request"})
		}
		chapter, uploads, err := writableChapterToRuntimeChapter(body.Chapter, optionalRequestArtifactWrites(body.ArtifactUploads))
		if err != nil {
			return reject(&delegationError{400, "invalid_delegation_request"})
		}
		if !slices.Contains(policy.Write.Ordinals, chapter.Ordinal) || !namesAllow(policy.Write.TaskTypes, chapter.TaskType) {
			return reject(delegationDenied())
		}
		if _, ok := chapter.Body.(jobdb.TaskAttemptOutcomeChapter); !ok {
			return reject(delegationDenied())
		}
		fresh := map[string]bool{}
		for _, a := range uploads {
			if !namesAllow(policy.Write.ArtifactNames, a.Name) {
				return reject(delegationDenied())
			}
			fresh[a.Name] = true
		}
		for _, a := range body.Chapter.Artifacts {
			if !fresh[a.Name] {
				return reject(delegationDenied())
			}
		}
	} else {
		var body runtimeapi.SubmitJobRequest
		if json.Unmarshal(raw, &body) != nil {
			return reject(&delegationError{400, "invalid_delegation_request"})
		}
		if !namesAllow(policy.Submit.JobTypes, body.Job.JobType) || body.Job.Schema != nil && body.Job.Schema.Schema != nil {
			return reject(delegationDenied())
		}
	}
	return true
}

func delegationSnapshot(c delegationClaims, token string) runtimeapi.ExecutionLease {
	return runtimeapi.ExecutionLease{LeaseId: c.Parent.LeaseID, LeaseToken: token, Job: toAPIJobHandle(jobdb.JobHandle{JobKey: c.key()}),
		WorkerId: &c.Parent.WorkerID, ExpiresAt: &c.ExpiresAt, Route: runtimeapi.Route(c.Route),
		ClientPayloadRevision: "0", ExecutionState: runtimeapi.ExecutionState{}, Delegation: &c.Grant}
}

func (s *proxyServer) CreateLeaseDelegation(ctx context.Context, req runtimeapi.CreateLeaseDelegationRequestObject) (runtimeapi.CreateLeaseDelegationResponseObject, error) {
	if req.Body == nil {
		return nil, badRequest("delegation body is required")
	}
	if isDelegationToken(req.Params.XJobDBLeaseToken) {
		return nil, delegationDenied()
	}
	key := jobdb.JobKey{TenantId: req.TenantId, JobId: req.JobId}
	p, err := s.validatedLeaseClaims(req.Params.XJobDBLeaseToken, key, req.LeaseId)
	if err != nil {
		return nil, err
	}
	body := *req.Body
	if err := validateDelegationRequest(&body); err != nil {
		return nil, badRequest(err.Error())
	}
	v, err := s.parentValidity(ctx, p)
	if err != nil {
		return nil, err
	}
	c := delegationClaims{Parent: p, Grant: jobdb.DelegationInfo{ID: uuid.NewString(), Permissions: body.Permissions, NotAfter: body.NotAfter}, Route: v.Route}
	c.setExpiry(v.ExpiresAt)
	token, err := s.tokens.signDelegation(c)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	return runtimeapi.CreateLeaseDelegation200JSONResponse(delegationSnapshot(c, token)), nil
}

func (s *proxyServer) InspectLease(ctx context.Context, req runtimeapi.InspectLeaseRequestObject) (runtimeapi.InspectLeaseResponseObject, error) {
	c, _, err := s.checkedDelegation(ctx, req.Params.XJobDBLeaseToken, jobdb.JobKey{TenantId: req.TenantId, JobId: req.JobId}, req.LeaseId, false)
	if err != nil {
		return nil, err
	}
	return runtimeapi.InspectLease200JSONResponse(delegationSnapshot(c, req.Params.XJobDBLeaseToken)), nil
}
func (s *proxyServer) renewDelegation(ctx context.Context, token string, key jobdb.JobKey, leaseID string) (runtimeapi.KeepAliveLeaseResponseObject, error) {
	c, v, err := s.checkedDelegation(ctx, token, key, leaseID, true)
	if err != nil {
		return nil, err
	}
	c.setExpiry(v.ExpiresAt)
	next, err := s.tokens.signDelegation(c)
	if err != nil {
		return nil, err
	}
	snapshot := delegationSnapshot(c, next)
	return runtimeapi.KeepAliveLease200JSONResponse{LeaseToken: next, Lease: &snapshot}, nil
}

func (s *proxyServer) readLeaseClaims(ctx context.Context, token string, key jobdb.JobKey, leaseID string) (leaseTokenClaims, *jobdb.DelegationChapters, error) {
	if isDelegationToken(token) {
		c, _, err := s.checkedDelegation(ctx, token, key, leaseID, false)
		if err != nil {
			return leaseTokenClaims{}, nil, err
		}
		if c.Grant.Permissions.Read == nil {
			return leaseTokenClaims{}, nil, delegationDenied()
		}
		return c.accessClaims(), c.Grant.Permissions.Read, nil
	}
	p, err := s.validatedLeaseClaims(token, key, leaseID)
	if err == nil {
		_, err = s.parentValidity(ctx, p)
	}
	return p, &jobdb.DelegationChapters{Any: true}, err
}
func (s *proxyServer) GetChapterWithLease(ctx context.Context, req runtimeapi.GetChapterWithLeaseRequestObject) (runtimeapi.GetChapterWithLeaseResponseObject, error) {
	key := jobdb.JobKey{TenantId: req.TenantId, JobId: req.JobId}
	_, policy, err := s.readLeaseClaims(ctx, req.Params.XJobDBLeaseToken, key, req.LeaseId)
	if err != nil {
		return nil, err
	}
	if !chaptersAllow(policy, req.Ordinal) {
		return nil, delegationDenied()
	}
	chapter, err := s.runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: key, Ordinal: req.Ordinal})
	if err != nil {
		return nil, err
	}
	converted, err := toAPIStoredChapter(ctx, chapter)
	if err != nil {
		return nil, err
	}
	if _, _, err = s.readLeaseClaims(ctx, req.Params.XJobDBLeaseToken, key, req.LeaseId); err != nil {
		return nil, err
	}
	return runtimeapi.GetChapterWithLease200JSONResponse(converted), nil
}
func (s *proxyServer) ListChaptersWithLease(ctx context.Context, req runtimeapi.ListChaptersWithLeaseRequestObject) (runtimeapi.ListChaptersWithLeaseResponseObject, error) {
	key := jobdb.JobKey{TenantId: req.TenantId, JobId: req.JobId}
	_, policy, err := s.readLeaseClaims(ctx, req.Params.XJobDBLeaseToken, key, req.LeaseId)
	if err != nil {
		return nil, err
	}
	var end *int64
	if req.Params.EndOrdinal != nil {
		n := int64(*req.Params.EndOrdinal)
		end = &n
	}
	chapters, err := s.runtime.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key, StartOrdinal: req.Params.StartOrdinal, EndOrdinal: end})
	if err != nil {
		return nil, err
	}
	out := make([]runtimeapi.ChapterRecord, 0)
	for _, chapter := range chapters {
		if chaptersAllow(policy, chapter.Ordinal) {
			c, err := toAPIStoredChapter(ctx, chapter)
			if err != nil {
				return nil, err
			}
			out = append(out, c)
		}
	}
	if _, _, err = s.readLeaseClaims(ctx, req.Params.XJobDBLeaseToken, key, req.LeaseId); err != nil {
		return nil, err
	}
	return runtimeapi.ListChaptersWithLease200JSONResponse{Chapters: out}, nil
}
