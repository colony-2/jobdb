package configuration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
	"github.com/segmentio/ksuid"
)

// Service validates the public admin contract around an atomic backend Store.
type Service struct {
	Store  Store
	Limits jobdb.ConfigurationLimits
}

var _ jobdb.ConfigurationAdmin = (*Service)(nil)

func (s *Service) ConfigurationLimits() jobdb.ConfigurationLimits { return s.Limits.WithDefaults() }
func ValidateTenant(tenant string) error {
	if tenant == "" || !utf8.ValidString(tenant) {
		return jobdb.ErrInvalidConfigurationRequest
	}
	return nil
}
func ValidateName(name string, l jobdb.ConfigurationLimits) error {
	if name == "" || !utf8.ValidString(name) {
		return jobdb.ErrInvalidConfigurationRequest
	}
	if len(name) > l.WithDefaults().MaxNameBytes {
		return jobdb.ErrConfigurationTooLarge
	}
	return nil
}
func ValidateValue(raw json.RawMessage, l jobdb.ConfigurationLimits) error {
	if len(raw) == 0 {
		return jobdb.ErrInvalidConfigurationRequest
	}
	_, err := clientpayload.DigestWithLimit(raw, l.WithDefaults().MaxValueBytes)
	if errors.Is(err, clientpayload.ErrTooLarge) {
		return jobdb.ErrConfigurationTooLarge
	}
	if err != nil {
		return jobdb.ErrInvalidConfigurationRequest
	}
	// Bound the actual JSON encoding (including HTML escaping), and reserve
	// the two object levels used by the snapshot envelope for nesting checks.
	encoded, err := json.Marshal(raw)
	if err != nil {
		return jobdb.ErrInvalidConfigurationRequest
	}
	if len(encoded) > l.WithDefaults().MaxValueBytes {
		return jobdb.ErrConfigurationTooLarge
	}
	envelope := append([]byte(`{"values":{"v":`), encoded...)
	envelope = append(envelope, '}', '}')
	if _, err := clientpayload.DigestWithLimit(envelope, len(envelope)); err != nil {
		return jobdb.ErrInvalidConfigurationRequest
	}
	return nil
}
func ValidateRevision(revision string) bool {
	if len(revision) == 0 || len(revision) > 128 {
		return false
	}
	for _, c := range revision {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
func ValidateSnapshot(s jobdb.ConfigurationSnapshot, l jobdb.ConfigurationLimits) error {
	l = l.WithDefaults()
	if !ValidateRevision(s.Revision) || s.Values == nil {
		return jobdb.ErrConfigurationFailed
	}
	if len(s.Values) > l.MaxEntries {
		return jobdb.ErrConfigurationReadTooLarge
	}
	for name, value := range s.Values {
		for _, err := range []error{ValidateName(name, l), ValidateValue(value, l)} {
			if errors.Is(err, jobdb.ErrConfigurationTooLarge) {
				return jobdb.ErrConfigurationReadTooLarge
			}
			if err != nil {
				return jobdb.ErrConfigurationFailed
			}
		}
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return jobdb.ErrConfigurationFailed
	}
	if len(raw) > l.MaxSnapshotBytes {
		return jobdb.ErrConfigurationReadTooLarge
	}
	return nil
}
func (s *Service) GetTenantConfiguration(ctx context.Context, tenant string) (jobdb.ConfigurationSnapshot, error) {
	if err := ValidateTenant(tenant); err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	if s == nil || s.Store == nil {
		return jobdb.ConfigurationSnapshot{}, jobdb.ErrConfigurationFailed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, s.ConfigurationLimits().ReadTimeout)
	defer cancel()
	result, err := s.Store.Read(ctx, tenant)
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, SafeError(err)
	}
	if err = ValidateSnapshot(result, s.Limits); err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	if ctx.Err() != nil {
		return jobdb.ConfigurationSnapshot{}, SafeError(ctx.Err())
	}
	return result, nil
}
func (s *Service) SetTenantConfigurationValue(ctx context.Context, req jobdb.SetTenantConfigurationValueRequest) (string, error) {
	if err := ValidateValue(req.Value, s.Limits); err != nil {
		return "", err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, req.Value); err != nil {
		return "", jobdb.ErrInvalidConfigurationRequest
	}
	value := append(json.RawMessage(nil), compact.Bytes()...)
	return s.mutate(ctx, req.TenantID, req.Name, value, req.ExpectedRevision, false)
}
func (s *Service) RemoveTenantConfigurationValue(ctx context.Context, req jobdb.RemoveTenantConfigurationValueRequest) (string, error) {
	return s.mutate(ctx, req.TenantID, req.Name, nil, req.ExpectedRevision, true)
}
func (s *Service) mutate(ctx context.Context, tenant, name string, value json.RawMessage, expected *string, remove bool) (string, error) {
	if err := ValidateTenant(tenant); err != nil {
		return "", err
	}
	// Removing oversized names remains possible after limits are lowered.
	if name == "" || !utf8.ValidString(name) {
		return "", jobdb.ErrInvalidConfigurationRequest
	}
	if !remove {
		if err := ValidateName(name, s.Limits); err != nil {
			return "", err
		}
	}
	if expected != nil && !ValidateRevision(*expected) {
		return "", jobdb.ErrInvalidConfigurationRequest
	}
	if ctx == nil {
		ctx = context.Background()
	}
	result, err := s.Store.Update(ctx, tenant, func(current jobdb.ConfigurationSnapshot) (jobdb.ConfigurationSnapshot, error) {
		if !ValidateRevision(current.Revision) || current.Values == nil {
			return jobdb.ConfigurationSnapshot{}, jobdb.ErrConfigurationFailed
		}
		if expected != nil && *expected != current.Revision {
			return jobdb.ConfigurationSnapshot{}, jobdb.ErrConfigurationRevisionMismatch
		}
		old, exists := current.Values[name]
		if remove && !exists {
			return current, nil
		}
		if !remove && exists {
			// Existing values may exceed newly lowered limits. Equality still uses their full bytes.
			a, e := clientpayload.DigestWithLimit(old, len(old))
			if e != nil {
				return jobdb.ConfigurationSnapshot{}, jobdb.ErrConfigurationFailed
			}
			b, _ := clientpayload.DigestWithLimit(value, len(value))
			if a == b {
				return current, nil
			}
		}
		next := Clone(current)
		if remove {
			delete(next.Values, name)
		} else {
			next.Values[name] = value
		}
		next.Revision = "cfg-" + ksuid.New().String()
		if !remove {
			if e := ValidateSnapshot(next, s.Limits); e != nil {
				if errors.Is(e, jobdb.ErrConfigurationReadTooLarge) {
					e = jobdb.ErrConfigurationTooLarge
				}
				return jobdb.ConfigurationSnapshot{}, e
			}
		}
		return next, nil
	})
	if err != nil {
		return "", SafeError(err)
	}
	return result.Revision, nil
}
