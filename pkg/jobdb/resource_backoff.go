package jobdb

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

const ResourceBackoffVersion = 1
const ResourceBackoffReason = "resource_temporarily_unavailable"

var ErrResourceBackoffUnsupported = errors.New("resource backoff unsupported")
var ErrInvalidResourceBackoff = errors.New("invalid resource backoff")

// ResourceBackoffUnsupportedError is returned when a required optional port or
// installed protocol is absent. Ordinary Reschedule is never a fallback.
type ResourceBackoffUnsupportedError struct{ Feature string }

func (e *ResourceBackoffUnsupportedError) Error() string {
	return "resource backoff unsupported: " + e.Feature
}
func (e *ResourceBackoffUnsupportedError) Unwrap() error { return ErrResourceBackoffUnsupported }

// ResourceBackoffPolicy is normalized before crossing a backend boundary.
// A nil allowance selects the backend default at the first accepted wait.
type ResourceBackoffPolicy struct {
	Version        int           `json:"version"`
	Initial        time.Duration `json:"initial"`
	Growth         float64       `json:"growth"`
	Maximum        time.Duration `json:"maximum"`
	Jitter         float64       `json:"jitter"`
	CrashAllowance *int64        `json:"crashAllowance,omitempty"`
}

func DefaultResourceBackoffPolicy() ResourceBackoffPolicy {
	return ResourceBackoffPolicy{Version: 1, Initial: 2 * time.Second, Growth: 2, Maximum: 30 * time.Second, Jitter: .2}
}
func (p ResourceBackoffPolicy) Validate() error {
	bad := func() error { return fmt.Errorf("%w: policy", ErrInvalidResourceBackoff) }
	if p.Version != 1 || p.Initial < time.Millisecond || p.Maximum > 30*time.Second || p.Maximum < p.Initial || p.Initial%time.Millisecond != 0 || p.Maximum%time.Millisecond != 0 || math.IsNaN(p.Growth) || math.IsInf(p.Growth, 0) || p.Growth < 1 || math.IsNaN(p.Jitter) || math.IsInf(p.Jitter, 0) || p.Jitter < 0 || p.Jitter > .2 || p.CrashAllowance != nil && *p.CrashAllowance <= 0 {
		return bad()
	}
	return nil
}

// ResourceBackoffDelay uses a caller-injected uniform sample in [0,1]. It
// saturates before multiplication and truncates fractional nanoseconds.
func ResourceBackoffDelay(p ResourceBackoffPolicy, count int64, sample float64) (time.Duration, error) {
	if err := p.Validate(); err != nil {
		return 0, err
	}
	if count < 1 || math.IsNaN(sample) || sample < 0 || sample > 1 {
		return 0, ErrInvalidResourceBackoff
	}
	nominal := float64(p.Initial)
	maximum := float64(p.Maximum)
	if p.Growth > 1 && count > 1 {
		exponent := float64(count - 1)
		if exponent >= math.Log(maximum/nominal)/math.Log(p.Growth) {
			nominal = maximum
		} else {
			nominal *= math.Pow(p.Growth, exponent)
		}
	}
	delay := math.Min(maximum, nominal*(1-p.Jitter+2*p.Jitter*sample))
	return time.Duration(delay), nil
}

type ResourceBackoffCapability struct {
	ProtocolVersion   int `json:"protocolVersion"`
	DurabilityVersion int `json:"durabilityVersion"`
}

func (c *ResourceBackoffCapability) Validate(durable bool) error {
	if c == nil || c.ProtocolVersion != 1 || c.DurabilityVersion < 0 || c.DurabilityVersion > 1 || durable && c.DurabilityVersion != 1 {
		return &ResourceBackoffUnsupportedError{Feature: "installed protocol/durability"}
	}
	return nil
}

// ResourceResume selects an append-only checkpoint. Only a scheduler-selected
// checkpoint authorizes resource recovery; prepared records alone do not.
type ResourceResume struct {
	Version           int       `json:"version"`
	CheckpointOrdinal int64     `json:"checkpointOrdinal"`
	CheckpointHash    string    `json:"checkpointHash"`
	Invocation        string    `json:"invocation"`
	Attempt           int       `json:"attempt"`
	Route             Route     `json:"route"`
	TaskWait          *TaskWait `json:"taskWait,omitempty"`
	PayloadRevision   int64     `json:"payloadRevision"`
}
type ResourceBackoffRequest struct {
	// AuthorityBinding is set by an authenticated transport, never read from JSON.
	AuthorityBinding    string                `json:"-"`
	Version             int                   `json:"version"`
	RequestID           string                `json:"requestId"`
	Invocation          string                `json:"invocation"`
	Attempt             int                   `json:"attempt"`
	Reason              string                `json:"reason"`
	Policy              ResourceBackoffPolicy `json:"policy"`
	Resume              ResourceResume        `json:"resume"`
	ClientPayloadUpdate *ClientPayloadUpdate  `json:"clientPayloadUpdate,omitempty"`
}

func (r ResourceResume) Validate() error {
	if r.Version != 1 || r.CheckpointOrdinal < 0 || len(r.CheckpointHash) != 64 || r.Invocation == "" || r.Attempt < 1 || r.PayloadRevision < 0 {
		return ErrInvalidResourceBackoff
	}
	if _, err := hex.DecodeString(r.CheckpointHash); err != nil {
		return ErrInvalidResourceBackoff
	}
	if err := r.Route.Validate(); err != nil {
		return err
	}
	_, err := RescheduleTaskWait(r.Route, r.TaskWait)
	return err
}
func (r ResourceBackoffRequest) Validate() error {
	if r.Version != 1 || r.RequestID == "" || len(r.RequestID) > 256 || r.Invocation == "" || len(r.Invocation) > 16384 || r.Attempt < 1 || r.Reason != ResourceBackoffReason || r.Resume.Invocation != r.Invocation || r.Resume.Attempt != r.Attempt {
		return ErrInvalidResourceBackoff
	}
	if err := r.Resume.Validate(); err != nil {
		return err
	}
	return r.Policy.Validate()
}

type ResourceBackoffReceipt struct {
	Version                int                   `json:"version"`
	RequestID              string                `json:"requestId"`
	JobKey                 JobKey                `json:"jobKey"`
	LeaseID                string                `json:"leaseId"`
	WorkerID               string                `json:"workerId"`
	Count                  int64                 `json:"count"`
	WakeAt                 time.Time             `json:"wakeAt"`
	Reason                 string                `json:"reason"`
	Policy                 ResourceBackoffPolicy `json:"policy"`
	ResolvedCrashAllowance int64                 `json:"resolvedCrashAllowance"`
	Resume                 ResourceResume        `json:"resume"`
}
type ResourceBackoffLease interface {
	RescheduleResourceBackoff(context.Context, ResourceBackoffRequest) (ResourceBackoffReceipt, error)
}

// ResourceBackoffTransport also supports exact authenticated receipt replay.
// Implementations must check retained receipts before checking a current lease.
type ResourceBackoffTransport interface {
	RescheduleResourceBackoffByID(context.Context, JobKey, string, string, ResourceBackoffRequest) (ResourceBackoffReceipt, error)
}

// CanonicalResourceBackoffRequest retains exact int64 values and symbolic
// allowance selection. Transport retries must retain this body and RequestID.
func CanonicalResourceBackoffRequest(r ResourceBackoffRequest) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	r.Policy = normalizedResourcePolicy(r.Policy)
	return json.Marshal(struct {
		Request   ResourceBackoffRequest `json:"request"`
		Authority string                 `json:"authority"`
	}{r, r.AuthorityBinding})
}

// ResourceBackoffReceiptReader grants no new mutation authority. HTTP uses it
// only after authenticating an expired credential's exact original binding.
type ResourceBackoffReceiptReader interface {
	ReplayResourceBackoffReceiptByID(context.Context, JobKey, string, string, ResourceBackoffRequest) (ResourceBackoffReceipt, error)
}

// ResourceBackoffWake saturates the absolute backend wake at the signed
// nanosecond storage boundary shared by built-in runtimes.
func ResourceBackoffWake(at time.Time, delay time.Duration) time.Time {
	ns := at.UnixNano()
	if ns > math.MaxInt64-int64(delay) {
		return time.Unix(0, math.MaxInt64).UTC()
	}
	return time.Unix(0, ns+int64(delay)).UTC()
}

func normalizedResourcePolicy(p ResourceBackoffPolicy) ResourceBackoffPolicy {
	if p.Jitter == 0 {
		p.Jitter = 0
	}
	if p.CrashAllowance != nil {
		v := *p.CrashAllowance
		p.CrashAllowance = &v
	}
	return p
}

// CloneResourceBackoffRequest freezes caller-owned pointers and payload bytes
// before an asynchronous transport or backend transaction uses the mutation.
func CloneResourceBackoffRequest(r ResourceBackoffRequest) (ResourceBackoffRequest, error) {
	if err := r.Validate(); err != nil {
		return ResourceBackoffRequest{}, err
	}
	r.Policy = normalizedResourcePolicy(r.Policy)
	raw, err := json.Marshal(r)
	if err != nil {
		return ResourceBackoffRequest{}, err
	}
	var copy ResourceBackoffRequest
	if err = json.Unmarshal(raw, &copy); err != nil {
		return copy, err
	}
	copy.AuthorityBinding = r.AuthorityBinding
	return copy, nil
}
