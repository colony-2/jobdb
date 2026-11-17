package jobdb

import (
	"encoding/json"
	"errors"
	"time"
)

// StartOrAppendJob appends to the newest unstarted generation or creates its successor.
// Delivery is at least once; the first creator chooses the template for each generation.
type StartOrAppendJob struct {
	AppendKey string
	MaxInputs int
	Job       SubmitJob
	Append    json.RawMessage
	// AppendArtifacts references persisted artifacts in the same tenant. Workers see
	// these under appends/<input index>/<name>, independently of start artifacts.
	AppendArtifacts []ArtifactKey
}
type StartOrAppendJobRequest struct {
	Job         StartOrAppendJob
	WorkerID    string
	RequestTime time.Time
}

const DefaultMaxAppendInputs = 50
const MaxAppendInputs = 1000

var ErrInvalidAppendRequest = errors.New("invalid append request")
var ErrAppendBusy = errors.New("append busy")
