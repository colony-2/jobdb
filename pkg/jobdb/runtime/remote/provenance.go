package remote

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func isProvenancePath(path string) bool {
	p := strings.Split(path, "/")
	return len(p) == 7 && p[1] == "v1" && p[2] == "tenants" && p[4] == "jobs" && p[6] == "execution-provenance"
}
func (s *proxyServer) serveProvenance(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet {
		w.WriteHeader(405)
		return
	}
	// The entire operation, including credential validation, has one budget.
	ctx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
	defer cancel()
	p := strings.Split(r.URL.EscapedPath(), "/")
	tenant, e1 := url.PathUnescape(p[3])
	job, e2 := url.PathUnescape(p[5])
	if e1 != nil || e2 != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	key := jobdb.JobKey{TenantId: tenant, JobId: job}
	if len(r.Header.Values("X-JobDB-Lease-Token")) > 1 {
		http.Error(w, "invalid lease credential", 401)
		return
	}
	token := r.Header.Get("X-JobDB-Lease-Token")
	if token == "" {
		if v := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "); isDelegationToken(v) {
			token = v
		}
	}
	if token != "" {
		// This job route is optional but never grants broader access than chapters.
		// A delegated subset of ordinals cannot authorize an all-history summary.
		var leaseID string
		if isDelegationToken(token) {
			c, err := s.tokens.parseDelegation(token)
			if err != nil {
				writeProvenanceCredentialError(w, err)
				return
			}
			leaseID = c.Parent.LeaseID
		} else {
			c, err := s.tokens.parse(token)
			if err != nil {
				http.Error(w, "invalid lease credential", 401)
				return
			}
			leaseID = c.LeaseID
		}
		_, policy, err := s.readLeaseClaims(ctx, token, key, leaseID)
		if err != nil {
			writeProvenanceCredentialError(w, err)
			return
		}
		if policy == nil || !policy.Any {
			writeDelegationError(w, delegationDenied())
			return
		}
	}
	out, err := jobdb.ReadExecutionProvenance(ctx, s.runtime, jobdb.ReadExecutionProvenanceRequest{JobKey: key, Consumer: r.URL.Query().Get("consumer")})
	if err != nil {
		switch {
		case errors.Is(err, jobdb.ErrExecutionProvenanceUnsupported):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(501)
			_, _ = w.Write([]byte(`{"code":"execution_provenance_unsupported","message":"execution provenance unsupported"}`))
		case errors.Is(err, jobdb.ErrInvalidExecutionProvenanceRequest):
			http.Error(w, "invalid request", 400)
		case errors.Is(err, jobdb.ErrJobNotFound):
			http.Error(w, "not found", 404)
		case errors.Is(err, jobdb.ErrCellForbidden), errors.Is(err, jobdb.ErrLeasePermissionDenied):
			http.Error(w, "forbidden", 403)
		default:
			if !writeInfrastructureError(w, err) {
				http.Error(w, "provenance unavailable", 500)
			}
		}
		return
	}
	raw, _ := json.Marshal(out)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}
func (r *Runtime) ReadExecutionProvenance(ctx context.Context, q jobdb.ReadExecutionProvenanceRequest) (jobdb.ExecutionProvenance, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	endpoint := strings.TrimRight(r.raw.Server, "/") + "/v1/tenants/" + url.PathEscape(q.JobKey.TenantId) + "/jobs/" + url.PathEscape(q.JobKey.JobId) + "/execution-provenance?consumer=" + url.QueryEscape(q.Consumer)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return jobdb.ExecutionProvenance{}, err
	}
	for _, edit := range r.raw.RequestEditors {
		if err := edit(ctx, req); err != nil {
			return jobdb.ExecutionProvenance{}, err
		}
	}
	resp, err := r.delegations.Do(req)
	if err != nil {
		return jobdb.ExecutionProvenance{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 262145))
	if err != nil {
		return jobdb.ExecutionProvenance{}, jobdb.ClassifyRuntimeIO(err, "execution_provenance", &q.JobKey, nil)
	}
	if resp.StatusCode != 200 {
		var envelope struct{ Code string }
		_ = json.Unmarshal(raw, &envelope)
		if resp.StatusCode == 501 && envelope.Code == "execution_provenance_unsupported" {
			return jobdb.ExecutionProvenance{}, jobdb.ErrExecutionProvenanceUnsupported
		}
		return jobdb.ExecutionProvenance{}, responseError("execution_provenance", resp.StatusCode, raw, nil)
	}
	var out jobdb.ExecutionProvenance
	if len(raw) > 262144 || json.Unmarshal(raw, &out) != nil || !validProvenanceResponse(out, q.Consumer, len(raw)) {
		return out, jobdb.ClassifyExecutionInfrastructure(errors.New("invalid provenance response"), jobdb.ExecutionInfrastructureRuntimeIO, "execution_provenance", &q.JobKey, nil)
	}
	return out, nil
}

// A successful HTTP status alone is not evidence of server capability or coverage.
func validProvenanceResponse(o jobdb.ExecutionProvenance, consumer string, size int) bool {
	m := jobdb.DefaultExecutionProvenanceLimits()
	l := o.Limits
	u := o.Usage
	if o.Capability != jobdb.ExecutionProvenanceCapability || o.Scope != jobdb.ExecutionProvenanceScope || o.Consumer != consumer || o.Pairs == nil || o.Issues == nil {
		return false
	}
	if o.Status != jobdb.ExecutionProvenanceComplete && o.Status != jobdb.ExecutionProvenancePartial && o.Status != jobdb.ExecutionProvenanceUnavailable {
		return false
	}
	if l.Pages <= 0 || l.Pages > m.Pages || l.Records <= 0 || l.Records > m.Records || l.ScannedBytes <= 0 || l.ScannedBytes > m.ScannedBytes || l.Pairs <= 0 || l.Pairs > m.Pairs || l.Issues <= 0 || l.Issues > m.Issues || l.ElapsedMillis < 0 || l.ElapsedMillis > m.ElapsedMillis || l.ResponseBytes <= 0 || l.ResponseBytes > m.ResponseBytes || l.PageRecords <= 0 || l.PageRecords > m.PageRecords || l.PageBytes <= 0 || l.PageBytes > m.PageBytes || l.NestingDepth <= 0 || l.NestingDepth > m.NestingDepth || l.IdentityBytes <= 0 || l.IdentityBytes > m.IdentityBytes {
		return false
	}
	if u.ResponseBytes != size || size > l.ResponseBytes || u.Pages < 0 || u.Pages > l.Pages || u.Records < 0 || u.Records > l.Records || u.ScannedBytes < 0 || u.ScannedBytes > l.ScannedBytes || u.ElapsedMillis < 0 || len(o.Pairs) > l.Pairs || len(o.Issues) > l.Issues || o.OmittedIssueCount < 0 || o.OmittedPairCount < 0 {
		return false
	}
	if o.ThroughOrdinal != nil && *o.ThroughOrdinal < 0 {
		return false
	}
	if o.ScannedThroughOrdinal != nil && (o.ThroughOrdinal == nil || *o.ScannedThroughOrdinal < 0 || *o.ScannedThroughOrdinal > *o.ThroughOrdinal) {
		return false
	}
	if o.UnexaminedRecordCount != nil && *o.UnexaminedRecordCount < 0 {
		return false
	}
	if o.Status == jobdb.ExecutionProvenanceComplete && (o.ThroughOrdinal == nil || o.ScannedThroughOrdinal == nil || *o.ThroughOrdinal != *o.ScannedThroughOrdinal || len(o.Pairs) == 0 || len(o.Issues) > 0 || o.OmittedIssueCount > 0 || o.OmittedPairCount > 0) {
		return false
	}
	for _, p := range o.Pairs {
		for _, v := range []string{p.ConsumerVersion, p.JobDBVersion} {
			if v == "" || len(v) > l.IdentityBytes || !utf8.ValidString(v) {
				return false
			}
		}
		if p.ConsumerCommit != nil && (len(*p.ConsumerCommit) > l.IdentityBytes || !utf8.ValidString(*p.ConsumerCommit)) {
			return false
		}
	}
	return true
}

func writeProvenanceCredentialError(w http.ResponseWriter, err error) {
	if writeDelegationError(w, err) {
		return
	}
	status := http.StatusUnauthorized
	if errors.Is(err, jobdb.ErrExecutionLeaseLost) {
		status = http.StatusConflict
	}
	http.Error(w, "invalid lease credential", status)
}
