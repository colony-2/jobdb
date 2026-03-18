package swf

import (
	"errors"
	"testing"
)

func TestJobFailedErrorIsDoesNotPanicWithUnhashableAppErrorCause(t *testing.T) {
	err := &JobFailedError{Cause: AppError{Payload: AppErrorPayload{
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
