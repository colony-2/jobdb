package workflow

import (
	"encoding/json"
	"fmt"
	"sync"
)

// YieldRequest describes an explicit handoff. Metadata is optional consumer JSON
// returned in JobRunOutcome.Handoff; it is not sent to storage or recorded in
// task history. Persist anything needed for recovery through Reschedule itself.
type YieldRequest struct {
	Reschedule RescheduleExecutionRequest
	Metadata   json.RawMessage
}

// Handoff is this Run's acknowledged explicit Yield, not the job's current
// scheduler state. It is captured before suspension and never inferred from
// another worker's actions. It does not represent notification delivery.
type Handoff struct {
	NextRoute Route
	Metadata  json.RawMessage
}

func prepareHandoff(req YieldRequest) (*Handoff, error) {
	if len(req.Metadata) > 0 && !json.Valid(req.Metadata) {
		return nil, fmt.Errorf("yield metadata must be valid JSON")
	}
	return &Handoff{NextRoute: req.Reschedule.NextRoute, Metadata: append(json.RawMessage(nil), req.Metadata...)}, nil
}

func cloneHandoff(h *Handoff) *Handoff {
	if h == nil {
		return nil
	}
	copy := *h
	copy.Metadata = append(json.RawMessage(nil), h.Metadata...)
	return &copy
}

// A run can finish while abandoned user code is still executing. Closing this
// state freezes the outcome and prevents a late response from changing it.
type handoffState struct {
	mu        sync.Mutex
	closed    bool
	confirmed *Handoff
}

func (s *handoffState) confirm(h *Handoff) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.confirmed != nil {
		return false
	}
	s.confirmed = h
	return true
}

func (s *handoffState) close() *Handoff {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return cloneHandoff(s.confirmed)
}
