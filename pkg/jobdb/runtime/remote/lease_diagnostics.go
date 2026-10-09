package remote

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

// LeaseFailurePhase identifies the boundary at which renewal failed. Exchange
// includes connection, TLS, request transmission, redirects, and response headers;
// arbitrary HTTP transports do not reliably distinguish those stages.
type LeaseFailurePhase string

const (
	LeasePhaseRequest  LeaseFailurePhase = "request"
	LeasePhaseExchange LeaseFailurePhase = "exchange"
	LeasePhaseRead     LeaseFailurePhase = "response_read"
	LeasePhaseDecode   LeaseFailurePhase = "response_decode"
	LeasePhaseSnapshot LeaseFailurePhase = "snapshot"
)

// LeaseFailureCategory is a stable classification, never extracted from error text.
type LeaseFailureCategory string

const (
	LeaseFailureUnknown     LeaseFailureCategory = "unknown"
	LeaseFailureDNS         LeaseFailureCategory = "dns"
	LeaseFailureRefused     LeaseFailureCategory = "connection_refused"
	LeaseFailureUnreachable LeaseFailureCategory = "unreachable"
	LeaseFailureTimeout     LeaseFailureCategory = "timeout"
	LeaseFailureCancelled   LeaseFailureCategory = "cancelled"
	LeaseFailureTLS         LeaseFailureCategory = "tls"
	LeaseFailureHTTP        LeaseFailureCategory = "http_rejection"
	LeaseFailureRead        LeaseFailureCategory = "response_read"
	LeaseFailureDecode      LeaseFailureCategory = "response_decode"
	LeaseFailureSnapshot    LeaseFailureCategory = "invalid_snapshot"
	LeaseFailureUnsupported LeaseFailureCategory = "unsupported"
)

// LeaseTransportError describes a remote Renew or KeepAlive failure, including
// invalid responses and authority rejection. Use errors.Is to distinguish lost
// authority and unsupported renewal from ambiguous failures. A failed renewal
// does not prove that the server did not extend the lease; callers must stop work.
//
// Errors returned by this package have credential-safe Error, GoString, SafeMessage,
// and JSON representations. Destination contains only the configured service's
// scheme and host (including port), never user info, path, query, or fragment.
// Operation is "renew" (including import) or "keep_alive". StatusCode is zero only
// when unknown; ResponseReceived indicates whether an HTTP response was observed.
// Err and Unwrap preserve the original cause for errors.Is/As, NOT safe logging.
type LeaseTransportError struct {
	Operation        string               `json:"operation"`
	Phase            LeaseFailurePhase    `json:"phase"`
	Category         LeaseFailureCategory `json:"category"`
	Destination      string               `json:"destination,omitempty"`
	ResponseReceived bool                 `json:"responseReceived"`
	StatusCode       int                  `json:"statusCode,omitempty"`
	Err              error                `json:"-"`
}

// Value receivers also protect formatting and JSON when callers copy the error.
func (e LeaseTransportError) Error() string    { return e.SafeMessage() }
func (e LeaseTransportError) GoString() string { return e.SafeMessage() }
func (e LeaseTransportError) Unwrap() error    { return e.Err }

// SafeMessage opts into safe rendering by jobdb.LeaseRenewalError.
func (e LeaseTransportError) SafeMessage() string {
	detail := map[LeaseFailureCategory]string{
		LeaseFailureDNS: "DNS lookup failed", LeaseFailureRefused: "connection refused",
		LeaseFailureUnreachable: "destination unreachable", LeaseFailureTimeout: "request timed out",
		LeaseFailureCancelled: "request cancelled", LeaseFailureTLS: "TLS negotiation failed",
		LeaseFailureHTTP: "HTTP request rejected", LeaseFailureRead: "response body could not be read",
		LeaseFailureDecode: "invalid renewal response", LeaseFailureSnapshot: "invalid lease snapshot",
		LeaseFailureUnsupported: "lease renewal is unsupported",
	}[e.Category]
	if detail == "" {
		detail = "request failed"
	}
	operation := "lease renewal"
	if e.Operation == "keep_alive" {
		operation = "lease keepalive"
	}
	message := operation + " failed"
	if e.Destination != "" {
		message += " for " + e.Destination
	}
	message += ": " + detail
	if e.Phase != "" {
		message += " (phase " + string(e.Phase) + ")"
	}
	if e.StatusCode != 0 {
		message += fmt.Sprintf(" (HTTP status %d)", e.StatusCode)
	}
	return message
}

func leaseFailureCategory(err error, fallback LeaseFailureCategory) LeaseFailureCategory {
	switch {
	case errors.Is(err, context.Canceled):
		return LeaseFailureCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return LeaseFailureTimeout
	}
	if leaseErrorTimeout(err) {
		return LeaseFailureTimeout
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return LeaseFailureDNS
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return LeaseFailureRefused
	}
	if errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) {
		return LeaseFailureUnreachable
	}
	var verification *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var invalidCert x509.CertificateInvalidError
	var hostname x509.HostnameError
	var record tls.RecordHeaderError
	var alert tls.AlertError
	if errors.As(err, &verification) || errors.As(err, &unknownCA) || errors.As(err, &invalidCert) || errors.As(err, &hostname) || errors.As(err, &record) || errors.As(err, &alert) {
		return LeaseFailureTLS
	}
	return fallback
}

// url.Error implements Timeout but only checks its immediate cause. A fmt wrap
// between it and a net.Error must not hide a timeout deeper in the chain.
func leaseErrorTimeout(err error) bool {
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if leaseErrorTimeout(cause) {
				return true
			}
		}
	}
	if cause := errors.Unwrap(err); cause != nil {
		return leaseErrorTimeout(cause)
	}
	return false
}

func (l *remoteExecutionLease) renewalError(operation string, phase LeaseFailurePhase, category LeaseFailureCategory, status int, err error) *LeaseTransportError {
	destination := ""
	if target, parseErr := url.Parse(l.runtime.raw.Server); parseErr == nil && (target.Scheme == "http" || target.Scheme == "https") && target.Host != "" {
		destination = (&url.URL{Scheme: target.Scheme, Host: target.Host}).String()
	}
	return &LeaseTransportError{Operation: operation, Phase: phase, Category: category, Destination: destination, ResponseReceived: status != 0, StatusCode: status, Err: err}
}

// Read status before touching the body. Generated WithResponse methods discard
// the response on read/decode errors, and can hide an explicit authority rejection.
func (l *remoteExecutionLease) requestRenewal(ctx context.Context, operation string) (*runtimeapi.KeepAliveLeaseResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := runtimeapi.NewKeepAliveLeaseRequest(l.runtime.raw.Server, l.jobKey.TenantId, l.jobKey.JobId, l.leaseID, &runtimeapi.KeepAliveLeaseParams{XJobDBLeaseToken: l.LeaseToken()})
	if err != nil {
		return nil, l.renewalError(operation, LeasePhaseRequest, LeaseFailureUnknown, 0, err)
	}
	req = req.WithContext(ctx)
	for _, edit := range l.runtime.raw.RequestEditors {
		if err := edit(ctx, req); err != nil {
			return nil, l.renewalError(operation, LeasePhaseRequest, LeaseFailureUnknown, 0, err)
		}
	}
	resp, err := l.runtime.raw.Client.Do(req)
	status := 0
	if resp != nil {
		status = resp.StatusCode
		if resp.Body != nil {
			defer resp.Body.Close()
		}
	}
	if err != nil {
		return nil, l.renewalError(operation, LeasePhaseExchange, leaseFailureCategory(err, LeaseFailureUnknown), status, err)
	}
	if status != http.StatusOK {
		var cause error
		switch status {
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict:
			cause = jobdb.ErrExecutionLeaseLost
		}
		return nil, l.renewalError(operation, LeasePhaseExchange, LeaseFailureHTTP, status, cause)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, l.renewalError(operation, LeasePhaseRead, leaseFailureCategory(err, LeaseFailureRead), status, err)
	}
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || (mediaType != "application/json" && !strings.HasSuffix(mediaType, "+json")) {
		return nil, l.renewalError(operation, LeasePhaseDecode, LeaseFailureDecode, status, errors.New("expected a JSON renewal response"))
	}
	var response runtimeapi.KeepAliveLeaseResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, l.renewalError(operation, LeasePhaseDecode, LeaseFailureDecode, status, err)
	}
	return &response, nil
}

func (l *remoteExecutionLease) renewedSnapshot(response *runtimeapi.KeepAliveLeaseResponse, operation string) (jobdb.RenewableExecutionLease, error) {
	if response.Lease == nil {
		return nil, l.renewalError(operation, LeasePhaseSnapshot, LeaseFailureUnsupported, http.StatusOK, jobdb.ErrLeaseRenewalUnsupported)
	}
	snapshot := response.Lease
	if snapshot.LeaseId != l.leaseID || fromAPIJobKey(snapshot.Job.JobKey) != l.jobKey || snapshot.WorkerId == nil || *snapshot.WorkerId == "" || (l.workerID != "" && *snapshot.WorkerId != l.workerID) || snapshot.ExpiresAt == nil || !snapshot.ExpiresAt.After(time.Now()) || snapshot.LeaseToken == "" {
		return nil, l.renewalError(operation, LeasePhaseSnapshot, LeaseFailureSnapshot, http.StatusOK, jobdb.ErrExecutionLeaseLost)
	}
	lease, err := l.runtime.executionLeaseFromAPI(*snapshot)
	if err != nil {
		return nil, l.renewalError(operation, LeasePhaseSnapshot, LeaseFailureSnapshot, http.StatusOK, err)
	}
	return lease.(jobdb.RenewableExecutionLease), nil
}
