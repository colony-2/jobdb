package impl

import (
	"errors"
	"math"
	"reflect"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

func mergeRunPolicy(override, base swf.RunPolicy) swf.RunPolicy {
	merged := base
	merged.Retry = mergeRetryPolicy(override.Retry, base.Retry)
	return merged
}

func mergeRetryPolicy(override, base swf.RetryPolicy) swf.RetryPolicy {
	merged := base
	if override.InitialInterval != 0 {
		merged.InitialInterval = override.InitialInterval
	}
	if override.BackoffCoefficient != 0 {
		merged.BackoffCoefficient = override.BackoffCoefficient
	}
	if override.MaximumInterval != 0 {
		merged.MaximumInterval = override.MaximumInterval
	}
	if override.MaximumAttempts != 0 {
		merged.MaximumAttempts = override.MaximumAttempts
	}
	if len(override.NonRetryableErrorTypes) > 0 {
		merged.NonRetryableErrorTypes = override.NonRetryableErrorTypes
	}
	return normalizeRetryPolicy(merged)
}

func normalizeRetryPolicy(policy swf.RetryPolicy) swf.RetryPolicy {
	rp := policy
	if rp.MaximumAttempts <= 0 {
		rp.MaximumAttempts = 1
	}
	if rp.BackoffCoefficient == 0 {
		rp.BackoffCoefficient = 1
	}
	return rp
}

func computeBackoff(rp swf.RetryPolicy, attempt int) time.Duration {
	base := time.Duration(rp.InitialInterval)
	backoff := float64(base)
	if attempt > 1 {
		backoff = float64(base) * math.Pow(rp.BackoffCoefficient, float64(attempt-1))
	}
	dur := time.Duration(backoff)
	maxInterval := time.Duration(rp.MaximumInterval)
	if maxInterval > 0 && dur > maxInterval {
		dur = maxInterval
	}
	if dur < 0 {
		dur = 0
	}
	return dur
}

func isRetryable(err error, policy swf.RetryPolicy) bool {
	if err == nil {
		return false
	}
	var nr swf.NonRetryableError
	if errors.As(err, &nr) && nr.NonRetryable() {
		return false
	}
	for _, name := range policy.NonRetryableErrorTypes {
		if errorMatchesTypeName(err, name) {
			return false
		}
	}
	if swf.IsSystemError(err) {
		return true
	}
	// Default to retrying all other errors.
	return true
}

func errorMatchesTypeName(err error, typeName string) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		t := reflect.TypeOf(e)
		if t == nil {
			continue
		}
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		if t.Name() == typeName || t.String() == typeName {
			return true
		}
	}
	return false
}
