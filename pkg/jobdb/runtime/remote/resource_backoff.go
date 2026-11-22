package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

// Strictly decode the new action before canonicalization. The token walker
// detects duplicates at every depth; ordinary JSON maps would discard them.
func validateResourceJSON(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value func(string) error
	value = func(path string) error {
		t, err := d.Token()
		if err != nil {
			return err
		}
		if t == nil && !strings.HasPrefix(path, ".clientPayloadUpdate.value") {
			return jobdb.ErrInvalidResourceBackoff
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{':
				seen := map[string]bool{}
				for d.More() {
					k, e := d.Token()
					if e != nil {
						return e
					}
					key, ok := k.(string)
					if !ok || seen[key] {
						return jobdb.ErrInvalidResourceBackoff
					}
					seen[key] = true
					if e = value(path + "." + key); e != nil {
						return e
					}
				}
				_, err = d.Token()
			case '[':
				for d.More() {
					if err = value(path + "[]"); err != nil {
						return err
					}
				}
				_, err = d.Token()
			default:
				return jobdb.ErrInvalidResourceBackoff
			}
		}
		return err
	}
	if err := value(""); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return jobdb.ErrInvalidResourceBackoff
	}
	var req jobdb.ResourceBackoffRequest
	strict := json.NewDecoder(bytes.NewReader(raw))
	strict.DisallowUnknownFields()
	if err := strict.Decode(&req); err != nil {
		return err
	}
	// All normalized timing values are required, including an explicit zero jitter.
	var root map[string]json.RawMessage
	_ = json.Unmarshal(raw, &root)
	for _, key := range []string{"version", "requestId", "invocation", "attempt", "reason", "policy", "resume"} {
		if _, ok := root[key]; !ok {
			return jobdb.ErrInvalidResourceBackoff
		}
	}
	var resume map[string]json.RawMessage
	_ = json.Unmarshal(root["resume"], &resume)
	for _, key := range []string{"version", "checkpointOrdinal", "checkpointHash", "invocation", "attempt", "route", "payloadRevision"} {
		if _, ok := resume[key]; !ok {
			return jobdb.ErrInvalidResourceBackoff
		}
	}
	var policy map[string]json.RawMessage
	_ = json.Unmarshal(root["policy"], &policy)
	for _, key := range []string{"version", "initial", "growth", "maximum", "jitter"} {
		if _, ok := policy[key]; !ok {
			return jobdb.ErrInvalidResourceBackoff
		}
	}
	return req.Validate()
}
func (s *proxyServer) RescheduleResourceBackoff(ctx context.Context, request runtimeapi.RescheduleResourceBackoffRequestObject) (runtimeapi.RescheduleResourceBackoffResponseObject, error) {
	if request.Body == nil {
		return nil, jobdb.ErrInvalidResourceBackoff
	}
	key := jobdb.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	token := request.Params.XJobDBLeaseToken
	req := *request.Body
	var claims leaseTokenClaims
	var live bool
	if isDelegationToken(token) {
		c, err := s.tokens.parseDelegation(token)
		if err != nil || c.key() != key || c.Parent.LeaseID != request.LeaseId {
			return nil, jobdb.ErrExecutionLeaseLost
		}
		if !c.Grant.Permissions.ResourceBackoff {
			return nil, jobdb.ErrLeasePermissionDenied
		}
		claims = c.Parent
		authority, _ := json.Marshal(struct {
			Parent grantParent
			Grant  jobdb.DelegationInfo
		}{parentOf(c.Parent), c.Grant})
		req.AuthorityBinding = "delegation:" + string(authority)
		_, _, err = s.checkedDelegation(ctx, token, key, request.LeaseId)
		live = err == nil
		if err != nil {
			var denied *delegationError
			if !errors.As(err, &denied) || denied.status == 503 {
				return nil, err
			}
		}
	} else {
		var err error
		claims, err = s.tokens.parse(token)
		if err != nil || claims.TenantID != key.TenantId || claims.JobID != key.JobId || claims.LeaseID != request.LeaseId {
			return nil, jobdb.ErrExecutionLeaseLost
		}
		live = claims.expiresAt().After(time.Now())
		authority, _ := json.Marshal(parentOf(claims))
		req.AuthorityBinding = "owner:" + string(authority)
	}
	var receipt jobdb.ResourceBackoffReceipt
	var err error
	// Even a valid token may outlive lease release. Exact replay uses the same
	// action, while an expired token reaches only the receipt-reader port.
	if !live {
		reader, ok := s.runtime.(jobdb.ResourceBackoffReceiptReader)
		if !ok {
			return nil, jobdb.ErrExecutionLeaseLost
		}
		receipt, err = reader.ReplayResourceBackoffReceiptByID(ctx, key, request.LeaseId, claims.WorkerID, req)
	} else {
		port, ok := s.runtime.(jobdb.ResourceBackoffTransport)
		if !ok {
			return nil, jobdb.ErrResourceBackoffUnsupported
		}
		receipt, err = port.RescheduleResourceBackoffByID(ctx, key, request.LeaseId, claims.WorkerID, req)
	}
	if err != nil {
		return nil, err
	}
	return runtimeapi.RescheduleResourceBackoff200JSONResponse(receipt), nil
}
func (l *remoteExecutionLease) RescheduleResourceBackoff(ctx context.Context, req jobdb.ResourceBackoffRequest) (jobdb.ResourceBackoffReceipt, error) {
	var zero jobdb.ResourceBackoffReceipt
	if err := l.state.ResourceBackoff.Validate(false); err != nil {
		return zero, err
	}
	if l.delegation != nil && !l.delegation.Permissions.ResourceBackoff {
		return zero, jobdb.ErrLeasePermissionDenied
	}
	if err := req.Validate(); err != nil {
		return zero, err
	}
	frozen, err := jobdb.CloneResourceBackoffRequest(req)
	if err != nil {
		return zero, err
	}
	req = frozen
	// Generated transport retries, if configured by a caller, receive exactly the
	// same immutable typed request. No new request ID or timing sample is made.
	resp, err := l.runtime.client.RescheduleResourceBackoffWithResponse(ctx, l.jobKey.TenantId, l.jobKey.JobId, l.leaseID, &runtimeapi.RescheduleResourceBackoffParams{XJobDBLeaseToken: l.LeaseToken()}, req)
	if err != nil {
		return zero, jobdb.ClassifyRuntimeIO(err, "resource_backoff", &l.jobKey, nil)
	}
	if resp.StatusCode() != 200 || resp.JSON200 == nil {
		if resp.StatusCode() == 501 {
			return zero, jobdb.ErrResourceBackoffUnsupported
		}
		return zero, responseErrorAt("resource backoff", resp.StatusCode(), resp.Body, jobdb.ErrExecutionLeaseLost, &l.jobKey, nil)
	}
	receipt := jobdb.ResourceBackoffReceipt(*resp.JSON200)
	if receipt.Version != 1 || receipt.RequestID != req.RequestID || receipt.JobKey != l.jobKey || receipt.LeaseID != l.leaseID {
		return zero, fmt.Errorf("invalid resource receipt")
	}
	return receipt, nil
}
