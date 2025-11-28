package impl

import "github.com/colony-2/swf-go/pkg/swf"

// systemError is an alias to the swf-level system error so IsSystemError works at call sites.
type systemError = swf.SystemError

func newSystemError(payload swf.SystemErrorPayload) error {
	return swf.NewSystemError(payload)
}

// NewSystemErrorForTest allows tests to construct system errors without exporting the type to users.
func NewSystemErrorForTest(payload swf.SystemErrorPayload) error {
	return swf.NewSystemError(payload)
}
