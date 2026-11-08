package jobdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type LeaseEventType string

const (
	LeaseEventAcquired LeaseEventType = "lease_acquired"
	LeaseEventRenewed  LeaseEventType = "lease_renewed"
)

type LeaseAcquisitionSource string

const (
	LeaseAcquisitionPoll           LeaseAcquisitionSource = "poll"
	LeaseAcquisitionTargeted       LeaseAcquisitionSource = "targeted"
	LeaseAcquisitionTaskCompletion LeaseAcquisitionSource = "task_completion"
)

type LeaseEvent struct {
	EventID                string                 `json:"eventId"`
	EventType              LeaseEventType         `json:"eventType"`
	OccurredAt             time.Time              `json:"occurredAt"`
	JobKey                 JobKey                 `json:"jobKey"`
	Cell                   string                 `json:"cell"`
	CellID                 string                 `json:"cellId"`
	CellRepo               string                 `json:"cellRepo"`
	GoalID                 string                 `json:"goalId"`
	JobType                string                 `json:"jobType"`
	Route                  Route                  `json:"route"`
	LeaseID                string                 `json:"leaseId"`
	WorkerID               string                 `json:"workerId"`
	LeaseExpiresAt         time.Time              `json:"leaseExpiresAt"`
	PreviousLeaseExpiresAt *time.Time             `json:"previousLeaseExpiresAt,omitempty"`
	AcquisitionSource      LeaseAcquisitionSource `json:"acquisitionSource,omitempty"`
	PreviousLeaseID        string                 `json:"previousLeaseId,omitempty"`
}
type ListLeaseEventsRequest struct {
	TenantID       string           `json:"-"`
	CellNames      []string         `json:"cellNames,omitempty"`
	CellIDs        []string         `json:"cellIds,omitempty"`
	JobIDs         []string         `json:"jobIds,omitempty"`
	GoalIDs        []string         `json:"goalIds,omitempty"`
	WorkerIDs      []string         `json:"workerIds,omitempty"`
	LeaseIDs       []string         `json:"leaseIds,omitempty"`
	EventTypes     []LeaseEventType `json:"eventTypes,omitempty"`
	JobTypes       []string         `json:"jobTypes,omitempty"`
	Routes         []Route          `json:"routes,omitempty"`
	OccurredAfter  *time.Time       `json:"occurredAfter,omitempty"`
	OccurredBefore *time.Time       `json:"occurredBefore,omitempty"`
	IncludeHidden  bool             `json:"includeHidden,omitempty"`
	PageSize       int              `json:"pageSize,omitempty"`
	PageToken      string           `json:"pageToken,omitempty"`
}
type ListLeaseEventsResponse struct {
	Events        []LeaseEvent `json:"events"`
	NextPageToken string       `json:"nextPageToken,omitempty"`
}

var (
	ErrInvalidLeaseEventRequest   = &CellError{"invalid_request", 400}
	ErrInvalidLeaseEventPageToken = &CellError{"invalid_lease_event_page_token", 400}
)

// UnmarshalJSON preserves the distinction between absent and explicitly null arrays.
func (r *ListLeaseEventsRequest) UnmarshalJSON(raw []byte) error {
	type plain ListLeaseEventsRequest
	var v plain
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidLeaseEventRequest, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return ErrInvalidLeaseEventRequest
	}
	if fields == nil {
		return ErrInvalidLeaseEventRequest
	}
	for _, v := range fields {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return ErrInvalidLeaseEventRequest
		}
	}
	for _, k := range []string{"cellNames", "cellIds", "jobIds", "goalIds", "workerIds", "leaseIds", "eventTypes", "jobTypes", "routes"} {
		if b, ok := fields[k]; ok && (bytes.Equal(bytes.TrimSpace(b), []byte("null")) || bytes.Equal(bytes.TrimSpace(b), []byte("[]"))) {
			return ErrInvalidLeaseEventRequest
		}
	}
	if b, ok := fields["pageSize"]; ok && (string(b) == "null" || v.PageSize < 1 || v.PageSize > 200) {
		return ErrInvalidLeaseEventRequest
	}
	*r = ListLeaseEventsRequest(v)
	return nil
}

// NormalizeLeaseEventRequest validates and canonicalizes filter sets without changing callers' slices.
func NormalizeLeaseEventRequest(r ListLeaseEventsRequest) (ListLeaseEventsRequest, error) {
	bad := func() (ListLeaseEventsRequest, error) { return ListLeaseEventsRequest{}, ErrInvalidLeaseEventRequest }
	if strings.TrimSpace(r.TenantID) == "" || r.PageSize < 0 || r.PageSize > 200 || (r.CellNames != nil && r.CellIDs != nil) {
		return bad()
	}
	for _, p := range []*[]string{&r.CellNames, &r.CellIDs, &r.JobIDs, &r.GoalIDs, &r.WorkerIDs, &r.LeaseIDs, &r.JobTypes} {
		if *p == nil {
			continue
		}
		if len(*p) == 0 || len(*p) > 100 {
			return bad()
		}
		set := map[string]bool{}
		for _, v := range *p {
			if strings.TrimSpace(v) == "" {
				return bad()
			}
			set[v] = true
		}
		out := make([]string, 0, len(set))
		for v := range set {
			out = append(out, v)
		}
		sort.Strings(out)
		*p = out
	}
	for _, name := range r.CellNames {
		if ValidateCellName(name) != nil {
			return bad()
		}
	}
	if r.EventTypes != nil {
		if len(r.EventTypes) == 0 || len(r.EventTypes) > 100 {
			return bad()
		}
		set := map[LeaseEventType]bool{}
		for _, v := range r.EventTypes {
			if v != LeaseEventAcquired && v != LeaseEventRenewed {
				return bad()
			}
			set[v] = true
		}
		r.EventTypes = []LeaseEventType{}
		for v := range set {
			r.EventTypes = append(r.EventTypes, v)
		}
		sort.Slice(r.EventTypes, func(i, j int) bool { return r.EventTypes[i] < r.EventTypes[j] })
	}
	if r.Routes != nil {
		if len(r.Routes) == 0 || len(r.Routes) > 100 {
			return bad()
		}
		set := map[Route]bool{}
		for _, v := range r.Routes {
			if v.Validate() != nil {
				return bad()
			}
			set[v] = true
		}
		r.Routes = []Route{}
		for v := range set {
			r.Routes = append(r.Routes, v)
		}
		sort.Slice(r.Routes, func(i, j int) bool {
			a, b := r.Routes[i], r.Routes[j]
			if a.JobType == b.JobType {
				return a.TaskType < b.TaskType
			}
			return a.JobType < b.JobType
		})
	}
	for _, p := range []**time.Time{&r.OccurredAfter, &r.OccurredBefore} {
		if *p != nil {
			t := (*p).UTC()
			if t.Year() < 0 || t.Year() > 9999 {
				return bad()
			}
			*p = &t
		}
	}
	if r.OccurredAfter != nil && r.OccurredBefore != nil && !r.OccurredAfter.Before(*r.OccurredBefore) {
		return bad()
	}
	if r.PageSize == 0 {
		r.PageSize = 100
	}
	return r, nil
}
