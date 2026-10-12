package configuration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
)

// SafeError drops backend/transport causes, which may include values or tokens.
func SafeError(err error) error {
	if err == nil {
		return nil
	}
	for _, safe := range []error{jobdb.ErrInvalidConfigurationRequest, jobdb.ErrConfigurationTooLarge, jobdb.ErrConfigurationReadTooLarge, jobdb.ErrConfigurationRevisionMismatch, jobdb.ErrConfigurationUnavailable, jobdb.ErrConfigurationFailed, jobdb.ErrExecutionLeaseLost} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	var n net.Error
	if errors.As(err, &n) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return jobdb.ErrConfigurationUnavailable
	}
	return jobdb.ErrConfigurationFailed
}
func ErrorStatus(err error) (int, string) {
	switch SafeError(err) {
	case jobdb.ErrInvalidConfigurationRequest:
		return 400, "invalid_configuration_request"
	case jobdb.ErrConfigurationTooLarge:
		return 413, "configuration_too_large"
	case jobdb.ErrConfigurationReadTooLarge:
		return 500, "configuration_read_too_large"
	case jobdb.ErrConfigurationRevisionMismatch:
		return 412, "configuration_revision_mismatch"
	case jobdb.ErrConfigurationUnavailable:
		return 503, "configuration_unavailable"
	case jobdb.ErrExecutionLeaseLost:
		return 409, "execution_lease_lost"
	default:
		return 500, "configuration_failed"
	}
}
func WriteError(w http.ResponseWriter, err error) {
	err = SafeError(err)
	status, code := ErrorStatus(err)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, err.Error()})
}
func ResponseError(status int, body io.Reader) error {
	switch status {
	case 400:
		return jobdb.ErrInvalidConfigurationRequest
	case 409:
		return jobdb.ErrExecutionLeaseLost
	case 412:
		return jobdb.ErrConfigurationRevisionMismatch
	case 413:
		return jobdb.ErrConfigurationTooLarge
	case 503:
		return jobdb.ErrConfigurationUnavailable
	case 500:
		var e struct {
			Code string `json:"code"`
		}
		if json.NewDecoder(io.LimitReader(body, 4096)).Decode(&e) == nil && e.Code == "configuration_read_too_large" {
			return jobdb.ErrConfigurationReadTooLarge
		}
	}
	return jobdb.ErrConfigurationFailed
}

// ReadJSON rejects duplicate keys and unknown envelope fields without rounding numbers.
func ReadJSON(r io.Reader, limit int, target any) error {
	raw, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return jobdb.ErrInvalidConfigurationRequest
	}
	if len(raw) > limit {
		return jobdb.ErrConfigurationTooLarge
	}
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return jobdb.ErrInvalidConfigurationRequest
	}
	if _, err := clientpayload.DigestWithLimit(raw, limit); err != nil {
		return jobdb.ErrInvalidConfigurationRequest
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil {
		return jobdb.ErrInvalidConfigurationRequest
	}
	return nil
}

// ParseETag accepts one exact strong ETag, never a wildcard, list, or weak tag.
func ParseETag(raw string) (string, error) {
	if len(raw) < 3 || !strings.HasPrefix(raw, "\"") || !strings.HasSuffix(raw, "\"") {
		return "", jobdb.ErrInvalidConfigurationRequest
	}
	revision := raw[1 : len(raw)-1]
	if !ValidateRevision(revision) {
		return "", jobdb.ErrInvalidConfigurationRequest
	}
	return revision, nil
}
func ETag(revision string) string { return "\"" + revision + "\"" }
