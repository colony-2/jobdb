package swf

import "errors"

var (
	ErrWorkflowNotDeterministic = errors.New("workflow was not deterministic")
	ErrMissingInputHash         = errors.New("workflow deterministic metadata missing input hash")
	ErrJobNotComplete           = errors.New("job not complete")
)

// systemErrorMarker is implemented by internal system error types.
type systemErrorMarker interface {
	error
	systemErrorMarker()
}

type AppErrorPayload struct {
	Message    string                 `json:"message"`
	Level      string                 `json:"level,omitempty"`
	Attrs      map[string]interface{} `json:"attrs,omitempty"`
	Stacktrace []string               `json:"stacktrace,omitempty"`
}

type SystemErrorPayload struct {
	Message    string   `json:"message"`
	Component  string   `json:"component,omitempty"`
	Code       string   `json:"code,omitempty"`
	Retryable  bool     `json:"retryable,omitempty"`
	Stacktrace []string `json:"stacktrace,omitempty"`
}

// AppError represents user-land/task errors; wraps AppErrorPayload.
type AppError struct {
	Payload AppErrorPayload
}

func (e AppError) Error() string {
	return e.Payload.Message
}

// IsAppError reports whether err is a wrapped AppError.
func IsAppError(err error) bool {
	var ae AppError
	return errors.As(err, &ae)
}

// IsSystemError reports whether err represents an internal/system failure.
func IsSystemError(err error) bool {
	var se systemErrorMarker
	return errors.As(err, &se)
}
