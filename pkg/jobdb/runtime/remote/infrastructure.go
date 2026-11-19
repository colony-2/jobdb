package remote

import (
	"encoding/json"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
)

type infrastructureEnvelope struct {
	Code           string              `json:"code"`
	Message        string              `json:"message"`
	Infrastructure *infrastructureWire `json:"infrastructure,omitempty"`
}
type infrastructureWire struct {
	Reason         jobdb.ExecutionInfrastructureReason `json:"reason"`
	Operation      string                              `json:"operation"`
	TenantID       *string                             `json:"tenantId"`
	JobID          *string                             `json:"jobId"`
	Ordinal        *int64                              `json:"ordinal"`
	Cause          jobdb.RemoteExecutionCause          `json:"cause"`
	CauseTruncated bool                                `json:"causeTruncated"`
}

func boundedUTF8(s string, n int) (string, bool) {
	if len(s) <= n && utf8.ValidString(s) {
		return s, false
	}
	if len(s) > n {
		s = s[:n]
	}
	for !utf8.ValidString(s) && len(s) > 0 {
		s = s[:len(s)-1]
	}
	return s, true
}
func writeInfrastructureError(w http.ResponseWriter, err error) bool {
	if !jobdb.IsExecutionInfrastructureError(err) {
		return false
	}
	err = jobdb.ClassifyExecutionInfrastructure(err, jobdb.ExecutionInfrastructureDeterminism, "execution", nil, nil)
	var e *jobdb.ExecutionInfrastructureError
	errors.As(err, &e)
	operation, truncated := boundedUTF8(e.Operation, 128)
	// Backend messages can contain payloads and credentials. Publish only a safe
	// reason descriptor; the original typed cause is retained on the server.
	message, code := "runtime operation failed", "runtime_io"
	switch e.Reason {
	case jobdb.ExecutionInfrastructureDurableDecode:
		message, code = "stored execution record could not be decoded", "decode_error"
	case jobdb.ExecutionInfrastructureDeterminism:
		message, code = "workflow was not deterministic", "determinism"
	}
	detail := &infrastructureWire{Reason: e.Reason, Operation: operation, Ordinal: e.Ordinal, Cause: jobdb.RemoteExecutionCause{Code: code, Message: message}, CauseTruncated: truncated}
	if e.JobKey != nil {
		if len(e.JobKey.TenantId) <= 4096 && len(e.JobKey.JobId) <= 4096 {
			detail.TenantID = &e.JobKey.TenantId
			detail.JobID = &e.JobKey.JobId
		} else {
			detail.CauseTruncated = true
		}
	}
	raw, _ := json.Marshal(infrastructureEnvelope{Code: "execution_infrastructure_error", Message: message, Infrastructure: detail})
	// Escaped Unicode identities can expand substantially. Omit unknown-safe
	// identity rather than emit a misleading shortened identity or oversized body.
	if len(raw) > 16<<10 {
		detail.TenantID = nil
		detail.JobID = nil
		detail.CauseTruncated = true
		raw, _ = json.Marshal(infrastructureEnvelope{Code: "execution_infrastructure_error", Message: message, Infrastructure: detail})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(500)
	_, _ = w.Write(raw)
	return true
}
func infrastructureResponseError(body []byte) error {
	if len(body) > 16<<10 || !utf8.Valid(body) {
		return nil
	}
	var v infrastructureEnvelope
	if json.Unmarshal(body, &v) != nil || v.Code != "execution_infrastructure_error" || v.Infrastructure == nil {
		return nil
	}
	d := v.Infrastructure
	if !validInfrastructureEnvelope(body, v) {
		return nil
	}
	d.Cause.Truncated = d.CauseTruncated
	cause := error(&d.Cause)
	if d.Reason == jobdb.ExecutionInfrastructureDeterminism {
		cause = errors.Join(jobdb.ErrWorkflowNotDeterministic, cause)
	}
	var key *jobdb.JobKey
	if d.TenantID != nil && d.JobID != nil {
		key = &jobdb.JobKey{TenantId: *d.TenantID, JobId: *d.JobID}
	}
	return &jobdb.ExecutionInfrastructureError{Reason: d.Reason, Operation: d.Operation, JobKey: key, Ordinal: d.Ordinal, Err: cause}
}

func validInfrastructureEnvelope(body []byte, v infrastructureEnvelope) bool {
	d := v.Infrastructure
	switch d.Reason {
	case jobdb.ExecutionInfrastructureDeterminism, jobdb.ExecutionInfrastructureDurableDecode, jobdb.ExecutionInfrastructureRuntimeIO:
	default:
		return false
	}
	if len(v.Message) == 0 || len(v.Message) > 4096 || len(d.Operation) > 128 || len(d.Cause.Code) == 0 || len(d.Cause.Code) > 128 || len(d.Cause.Message) == 0 || len(d.Cause.Message) > 4096 || d.Ordinal != nil && *d.Ordinal < 0 {
		return false
	}
	if (d.TenantID == nil) != (d.JobID == nil) {
		return false
	}
	if d.TenantID != nil && (strings.TrimSpace(*d.TenantID) == "" || strings.TrimSpace(*d.JobID) == "" || len(*d.TenantID) > 4096 || len(*d.JobID) > 4096) {
		return false
	}
	// Missing or null required fields are malformed, even when Go's zero value
	// could otherwise make them look like valid absent source information.
	var envelope, detail, cause map[string]json.RawMessage
	if json.Unmarshal(body, &envelope) != nil || json.Unmarshal(envelope["infrastructure"], &detail) != nil || json.Unmarshal(detail["cause"], &cause) != nil {
		return false
	}
	required := func(fields map[string]json.RawMessage, nullable bool, names ...string) bool {
		for _, name := range names {
			raw, ok := fields[name]
			if !ok || !nullable && string(raw) == "null" {
				return false
			}
		}
		return true
	}
	return required(envelope, false, "code", "message", "infrastructure") &&
		required(detail, false, "reason", "operation", "cause", "causeTruncated") &&
		required(detail, true, "tenantId", "jobId", "ordinal") &&
		required(cause, false, "code", "message")
}

type infrastructureDiagnosticBody struct {
	io.Reader
	io.Closer
}
