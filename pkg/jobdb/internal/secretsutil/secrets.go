// Package secretsutil contains shared, credential-safe secret API validation.
package secretsutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func Names(names []string, limit int64) error {
	if len(names) == 0 {
		return jobdb.ErrInvalidSecretRequest
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if name == "" || !utf8.ValidString(name) || seen[name] {
			return jobdb.ErrInvalidSecretRequest
		}
		seen[name] = true
	}
	raw, _ := json.Marshal(struct {
		Names []string `json:"names"`
	}{names})
	if int64(len(raw)) > limit {
		return jobdb.ErrSecretRequestTooLarge
	}
	return nil
}

func Result(result jobdb.SecretResolveResult, names []string, limit int64) (jobdb.SecretResolveResult, error) {
	allowed := make(map[string]bool, len(names))
	for _, name := range names {
		allowed[name] = true
	}
	if result.Secrets == nil {
		result.Secrets = map[string]jobdb.SecretValue{}
	}
	for name, value := range result.Secrets {
		if !allowed[name] || !utf8.ValidString(value.Value) || !utf8.ValidString(value.Version) {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
		}
		if value.ExpiresAt != nil && !value.ExpiresAt.After(time.Now()) {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretsUnavailable
		}
	}
	raw, err := json.Marshal(result)
	if err != nil || int64(len(raw))+1 > limit {
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
	}
	return result, nil
}

// SafeError deliberately drops raw causes: callers may persist or format errors.
func SafeError(err error) error {
	if err == nil {
		return nil
	}
	for _, safe := range []error{jobdb.ErrNoSecretProvider, jobdb.ErrSecretsUnavailable, jobdb.ErrSecretResolutionFailed, jobdb.ErrInvalidSecretRequest, jobdb.ErrSecretRequestTooLarge, jobdb.ErrDevSecretsNotConfigured, jobdb.ErrExecutionLeaseLost} {
		if errors.Is(err, safe) {
			return safe
		}
	}
	var networkError net.Error
	if errors.As(err, &networkError) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return jobdb.ErrSecretsUnavailable
	}
	return jobdb.ErrSecretResolutionFailed
}

func ReadJSON(r io.Reader, limit int64, target any) error {
	raw, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return jobdb.ErrInvalidSecretRequest
	}
	if int64(len(raw)) > limit {
		return jobdb.ErrSecretRequestTooLarge
	}
	if !utf8.Valid(raw) || len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return jobdb.ErrInvalidSecretRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil || decoder.Decode(new(any)) != io.EOF {
		return jobdb.ErrInvalidSecretRequest
	}
	return nil
}

func WriteError(w http.ResponseWriter, err error) {
	err = SafeError(err)
	status, code := http.StatusInternalServerError, "secret_resolution_failed"
	switch err {
	case jobdb.ErrNoSecretProvider:
		status, code = 501, "secrets_not_supported"
	case jobdb.ErrSecretsUnavailable:
		status, code = 503, "secrets_unavailable"
	case jobdb.ErrDevSecretsNotConfigured:
		status, code = 503, "dev_secrets_not_configured"
	case jobdb.ErrInvalidSecretRequest:
		status, code = 400, "invalid_secret_request"
	case jobdb.ErrSecretRequestTooLarge:
		status, code = 413, "secret_request_too_large"
	case jobdb.ErrExecutionLeaseLost:
		status, code = 409, "execution_lease_lost"
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, err.Error()})
}

func StatusError(status int) error {
	switch status {
	case 400:
		return jobdb.ErrInvalidSecretRequest
	case 413:
		return jobdb.ErrSecretRequestTooLarge
	case 409:
		return jobdb.ErrExecutionLeaseLost
	case 501:
		return jobdb.ErrNoSecretProvider
	case 503:
		return jobdb.ErrSecretsUnavailable
	default:
		return jobdb.ErrSecretResolutionFailed
	}
}
