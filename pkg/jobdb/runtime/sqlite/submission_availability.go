package sqlite

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
)

var (
	submissionAvailabilityMin = time.Date(1677, 9, 21, 0, 12, 43, 145224192, time.UTC)
	submissionAvailabilityMax = time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC)
)

// Compare instants before UnixNano: conversion outside this interval wraps.
// Nil means omitted; an explicit Go zero time is outside SQLite's interval.
func validateSubmissionAvailability(at *time.Time) error {
	if at != nil && (at.Before(submissionAvailabilityMin) || at.After(submissionAvailabilityMax)) {
		return fmt.Errorf("sqlite runtime: submission availableAt must be between %s and %s (inclusive)", submissionAvailabilityMin.Format(time.RFC3339Nano), submissionAvailabilityMax.Format(time.RFC3339Nano))
	}
	return nil
}

// Availability uses every signed nanosecond, including zero (the Unix epoch).
// Other timestamp fields retain their existing zero-as-unset contract.
func availabilityFromNS(ns int64) time.Time { return time.Unix(0, ns).UTC() }

// Preparation is also a scheduler port: validate before identity reservation or
// receipt reuse. Keep the descriptor encoding and restart contract unchanged.
func normalizeSubmissionAvailability(req goalstate.Preparation) (goalstate.Preparation, error) {
	var descriptor struct {
		Kind string
		Job  json.RawMessage
	}
	// The preparation port also accepts opaque descriptors. Only the known
	// submission encoding carries an availability contract; leave other
	// reservations and restart descriptors under their existing rules.
	if err := json.Unmarshal([]byte(req.Descriptor), &descriptor); err != nil || descriptor.Kind != "submit" {
		return req, nil
	}
	var submission struct{ AvailableAt *time.Time }
	if err := json.Unmarshal(descriptor.Job, &submission); err != nil {
		return req, err
	}
	at := submission.AvailableAt
	if err := validateSubmissionAvailability(at); err != nil {
		return req, err
	}
	if at == nil {
		return req, nil
	}
	// Equivalent timezone representations must share the same receipt identity.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(req.Descriptor), &fields); err != nil {
		return req, err
	}
	var job map[string]json.RawMessage
	if err := json.Unmarshal(fields["Job"], &job); err != nil {
		return req, err
	}
	raw, err := json.Marshal(at.UTC())
	if err != nil {
		return req, err
	}
	job["AvailableAt"] = raw
	fields["Job"], err = json.Marshal(job)
	if err != nil {
		return req, err
	}
	raw, err = json.Marshal(fields)
	req.Descriptor = string(raw)
	return req, err
}

// Existing preparations and receipts may predate UTC normalization. Compare
// their availability by instant as well, without rewriting durable receipts.
func submissionDescriptorsEqual(a, b string) bool {
	if a == b {
		return true
	}
	left, err := normalizeSubmissionAvailability(goalstate.Preparation{Descriptor: a})
	if err != nil {
		return false
	}
	right, err := normalizeSubmissionAvailability(goalstate.Preparation{Descriptor: b})
	return err == nil && left.Descriptor == right.Descriptor
}
