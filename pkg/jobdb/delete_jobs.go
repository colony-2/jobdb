package jobdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidJobDeletion     = errors.New("invalid job deletion request")
	ErrJobDeletionUnsupported = errors.New("job deletion is not supported by this backend")
)

// JobDeletionRuntime is the optional administration capability for deleting jobs.
// Deletion hides jobs from ListJobs and ListScheduleRuns, revokes leases, and
// cancels unfinished jobs. Completed outcomes, direct reads, history, and
// artifacts remain available, preserving thin clones and other references.
// This does not reclaim storage or implicitly delete children. IDs continue
// to identify the same jobs.
type JobDeletionRuntime interface {
	DeleteJobs(context.Context, DeleteJobsRequest) (DeleteJobsResponse, error)
}

// DeleteJobsRequest selects jobs in exactly one tenant. Selectors intersect;
// JobIDs are ORed. CreatedBefore is an exclusive creation-time cutoff.
// All explicitly selects the tenant's entire job set and cannot have selectors.
// DryRun counts matches without mutation; it does not reserve that set.
type DeleteJobsRequest struct {
	TenantID       string
	JobIDs         []string
	CreatedBefore  *time.Time
	MetadataFilter MetadataFilter
	All            bool
	DryRun         bool
}

type DeleteJobsResponse struct {
	MatchedCount int64 `json:"matchedCount"`
	DeletedCount int64 `json:"deletedCount"`
}

func (r DeleteJobsRequest) Validate() error {
	invalid := func(message string) error { return fmt.Errorf("%w: %s", ErrInvalidJobDeletion, message) }
	if strings.TrimSpace(r.TenantID) == "" {
		return invalid("tenant is required")
	}
	for _, id := range r.JobIDs {
		if strings.TrimSpace(id) == "" {
			return invalid("job IDs must not be empty")
		}
	}
	if r.CreatedBefore != nil {
		// Built-in storage uses nanosecond timestamps. Reject dates that would
		// overflow instead of selecting an unexpected set of jobs.
		if r.CreatedBefore.IsZero() || !time.Unix(0, r.CreatedBefore.UnixNano()).Equal(*r.CreatedBefore) {
			return invalid("createdBefore must be a nonzero timestamp representable in Unix nanoseconds")
		}
	}
	predicates, err := MetadataPredicates(r.MetadataFilter)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJobDeletion, err)
	}
	for _, predicate := range predicates {
		for _, value := range predicate.Values {
			if _, err := json.Marshal(value); err != nil {
				return invalid("metadata predicate values must be valid JSON")
			}
		}
	}
	filtered := len(r.JobIDs) > 0 || r.CreatedBefore != nil || len(predicates) > 0
	if !filtered && !r.All {
		return invalid("provide job IDs, createdBefore, metadata predicates, or explicitly select all")
	}
	if filtered && r.All {
		return invalid("all cannot be combined with selectors")
	}
	return nil
}
