package jobdb

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
)

// GoalSummary describes committed goal state and counts computed at read time.
// Counts cover all memberships, including archived and hidden candidates.
// Candidate changes do not advance GoalInfo.Revision or UpdatedAt.
type GoalSummary struct {
	GoalInfo
	CandidateCount            int64 `json:"candidateCount"`
	NonTerminalCandidateCount int64 `json:"nonTerminalCandidateCount"`
	TerminalCandidateCount    int64 `json:"terminalCandidateCount"`
	UnavailableCandidateCount int64 `json:"unavailableCandidateCount"`
}

// GoalCandidateFilter matches at least one candidate, including hidden/archived jobs.
// MetadataEquals uses paths relative to application metadata and non-null JSON scalars.
type GoalCandidateFilter struct {
	JobIDs         []string            `json:"jobIds,omitempty"`
	JobTypes       []string            `json:"jobTypes,omitempty"`
	MetadataEquals []MetadataPredicate `json:"metadataEquals,omitempty"`
}

// ListGoalsRequest lists goals in ascending opaque goal-ID order. Candidate
// filters mean EXISTS a matching candidate; counts always cover the whole goal.
// Time bounds are inclusive. Pages are live reads, not a cross-page snapshot.
type ListGoalsRequest struct {
	TenantId      string               `json:"tenantId"`
	GoalIDs       []string             `json:"goalIds,omitempty"`
	States        []GoalState          `json:"states,omitempty"`
	CreatedAfter  *time.Time           `json:"createdAfter,omitempty"`
	CreatedBefore *time.Time           `json:"createdBefore,omitempty"`
	UpdatedAfter  *time.Time           `json:"updatedAfter,omitempty"`
	UpdatedBefore *time.Time           `json:"updatedBefore,omitempty"`
	Candidate     *GoalCandidateFilter `json:"candidate,omitempty"`
	PageSize      int                  `json:"pageSize,omitempty"`
	PageToken     string               `json:"pageToken,omitempty"`
}
type ListGoalsResponse struct {
	Goals         []GoalSummary `json:"goals"`
	NextPageToken string        `json:"nextPageToken,omitempty"`
}

// ListDependencyWaitsRequest requires a consumer, a target, or both. Consumer
// lookup includes retained history. Target lookup includes only currently
// unresolved dependencies in pending waits; it does not provide reverse history.
type ListDependencyWaitsRequest struct {
	TenantId      string            `json:"tenantId"`
	ConsumerJobID string            `json:"consumerJobId,omitempty"`
	Target        *DependencyTarget `json:"target,omitempty"`
	States        []DependencyState `json:"states,omitempty"`
	PageSize      int               `json:"pageSize,omitempty"`
	PageToken     string            `json:"pageToken,omitempty"`
}
type ListDependencyWaitsResponse struct {
	Waits         []DependencyWaitInfo `json:"waits"`
	NextPageToken string               `json:"nextPageToken,omitempty"`
}

func (r ListGoalsRequest) Validate() error {
	if strings.TrimSpace(r.TenantId) == "" {
		return fmt.Errorf("%w: tenant is required", ErrInvalidDependency)
	}
	for _, s := range r.States {
		if s != GoalOpen && s != GoalSucceeded && s != GoalFailed {
			return fmt.Errorf("%w: unknown goal state", ErrInvalidDependency)
		}
	}
	for _, pair := range [][2]*time.Time{{r.CreatedAfter, r.CreatedBefore}, {r.UpdatedAfter, r.UpdatedBefore}} {
		if pair[0] != nil && pair[1] != nil && pair[0].After(*pair[1]) {
			return fmt.Errorf("%w: reversed time bounds", ErrInvalidDependency)
		}
	}
	if r.Candidate != nil {
		for _, p := range r.Candidate.MetadataEquals {
			if len(p.Path) == 0 || len(p.Values) == 0 {
				return fmt.Errorf("%w: metadata path and values required", ErrInvalidDependency)
			}
			for _, value := range p.Values {
				if _, bytes := value.([]byte); bytes {
					return fmt.Errorf("%w: metadata values must be JSON scalars", ErrInvalidDependency)
				}
				raw, err := json.Marshal(value)
				if err != nil {
					return fmt.Errorf("%w: invalid metadata value", ErrInvalidDependency)
				}
				var scalar any
				if json.Unmarshal(raw, &scalar) != nil {
					return fmt.Errorf("%w: invalid metadata value", ErrInvalidDependency)
				}
				switch scalar.(type) {
				case string, bool, float64:
				default:
					return fmt.Errorf("%w: metadata values must be non-null scalars", ErrInvalidDependency)
				}
			}
			for _, part := range p.Path {
				if part == "" {
					return fmt.Errorf("%w: empty metadata path", ErrInvalidDependency)
				}
			}
		}
	}
	return nil
}
func (r ListDependencyWaitsRequest) Validate() error {
	if strings.TrimSpace(r.TenantId) == "" || (r.ConsumerJobID == "" && r.Target == nil) {
		return fmt.Errorf("%w: tenant and consumer or target required", ErrInvalidDependency)
	}
	if r.Target != nil {
		if err := ValidateDependencies([]Dependency{{Target: *r.Target, Condition: JobPrereqSuccess}}); err != nil {
			return err
		}
		if len(r.States) > 0 && (len(r.States) != 1 || r.States[0] != DependencyPending) {
			return fmt.Errorf("%w: reverse lookup supports pending waits only", ErrInvalidDependency)
		}
	}
	for _, s := range r.States {
		if !slices.Contains([]DependencyState{DependencyPending, DependencySatisfied, DependencyFailed, DependencyTimedOut, DependencyCancelled}, s) {
			return fmt.Errorf("%w: unknown dependency state", ErrInvalidDependency)
		}
	}
	return nil
}

// DiscoveryPageSize bounds page allocation consistently across adapters.
func DiscoveryPageSize(n int) int {
	if n <= 0 {
		return DefaultListJobsPageSize
	}
	return min(n, MaxListJobsPageSize)
}

// discoveryCursor binds an opaque continuation to the operation, tenant and
// filters. It is not an authorization credential; every page must be authorized.
type discoveryCursor struct {
	Scope string   `json:"scope"`
	After []string `json:"after"`
}

func DiscoveryPageScope(request any) (string, error) {
	// Callers pass the request with PageToken empty and PageSize zero.
	b, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("%w: invalid filters", ErrInvalidDependency)
	}
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%T:%x", request, sum), nil
}
func DecodeDiscoveryPage(token, scope string, dimensions int) ([]string, error) {
	out := make([]string, dimensions)
	if token == "" {
		return out, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid page token", ErrInvalidDependency)
	}
	var c discoveryCursor
	if json.Unmarshal(b, &c) != nil || c.Scope != scope || len(c.After) != dimensions {
		return nil, fmt.Errorf("%w: page token does not match query", ErrInvalidDependency)
	}
	return c.After, nil
}
func EncodeDiscoveryPage(scope string, after ...string) string {
	b, _ := json.Marshal(discoveryCursor{Scope: scope, After: after})
	return base64.RawURLEncoding.EncodeToString(b)
}
