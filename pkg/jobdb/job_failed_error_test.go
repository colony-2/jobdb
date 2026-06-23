package jobdb

import (
	"errors"
	"testing"
)

func TestJobFailedErrorIsDoesNotPanicWithUnhashableAppErrorCause(t *testing.T) {
	err := &JobFailedError{Cause: &AppError{Payload: AppErrorPayload{
		Message: "child failed",
		Level:   "error",
		Attrs: map[string]interface{}{
			"nested": map[string]interface{}{"k": "v"},
		},
	}}}

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("errors.Is panicked: %v", rec)
		}
	}()

	if !errors.Is(err, ErrJobFailed) {
		t.Fatal("expected errors.Is to match ErrJobFailed")
	}
}

func TestJobFailedErrorErrorDoesNotPanicWithUnhashableAppErrorCause(t *testing.T) {
	err := &JobFailedError{Cause: &AppError{Payload: AppErrorPayload{
		Message: "child failed",
		Level:   "error",
		Attrs: map[string]interface{}{
			"nested": map[string]interface{}{"k": "v"},
		},
	}}}

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("Error() panicked: %v", rec)
		}
	}()

	if got := err.Error(); got != "job failed: child failed" {
		t.Fatalf("unexpected error string %q", got)
	}
}

func TestAppErrorPointerMatchesValueTargetWithErrorsAs(t *testing.T) {
	err := normalizeComparableError(AppError{Payload: AppErrorPayload{
		Message: "child failed",
		Level:   "error",
		Attrs: map[string]interface{}{
			"nested": map[string]interface{}{"k": "v"},
		},
	}})

	var appErr AppError
	if !errors.As(err, &appErr) {
		t.Fatal("expected errors.As to match AppError value target")
	}
	if appErr.Payload.Message != "child failed" {
		t.Fatalf("unexpected app error %+v", appErr)
	}
}

func TestNormalizeComparableErrorMakesErrorChainHashSafe(t *testing.T) {
	err := &JobFailedError{Cause: normalizeComparableError(AppError{Payload: AppErrorPayload{
		Message: "child failed",
		Level:   "error",
		Attrs: map[string]interface{}{
			"nested": map[string]interface{}{"k": "v"},
		},
	}})}

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("hashing error chain panicked: %v", rec)
		}
	}()

	seen := map[error]struct{}{}
	for current := error(err); current != nil; {
		seen[current] = struct{}{}
		next, ok := current.(interface{ Unwrap() error })
		if !ok {
			break
		}
		current = next.Unwrap()
	}
}
