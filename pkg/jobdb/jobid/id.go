// Package jobid allocates recognizable IDs for new jobs. Existing and
// caller-supplied job IDs remain opaque and need not use this format.
package jobid

import "github.com/segmentio/ksuid"

// Prefix precedes the full 27-character base62 suffix of a generated job ID.
const Prefix = "C2"

// New returns a new job ID: C2 followed by the unchanged KSUID text.
func New() string {
	return format(ksuid.New())
}

func format(id ksuid.KSUID) string { return Prefix + id.String() }
