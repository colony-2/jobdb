package impl

import "github.com/colony-2/swf-go/pkg/swf"

// systemError represents infrastructure/transport failures.
// It is intentionally unexported so app code cannot construct it directly.
type systemError struct {
	payload swf.SystemErrorPayload
}

func (e systemError) Error() string {
	return e.payload.Message
}

func (systemError) systemErrorMarker() {}

func (e systemError) Payload() swf.SystemErrorPayload {
	return e.payload
}

func newSystemError(payload swf.SystemErrorPayload) error {
	return systemError{payload: payload}
}

// NewSystemErrorForTest allows tests to construct system errors without exporting the type to users.
func NewSystemErrorForTest(payload swf.SystemErrorPayload) error {
	return newSystemError(payload)
}
