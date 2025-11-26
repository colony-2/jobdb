package swf

import "errors"

var (
	ErrWorkflowNotDeterministic = errors.New("workflow was not deterministic")
	ErrMissingInputHash         = errors.New("workflow deterministic metadata missing input hash")
)
