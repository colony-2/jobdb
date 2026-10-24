package jobdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrGoalNotFound           = errors.New("goal not found")
	ErrGoalConflict           = errors.New("goal conflict")
	ErrGoalFailed             = errors.New("goal failed")
	ErrInvalidDependency      = errors.New("invalid goal or dependency request")
	ErrDependencyWaitNotFound = errors.New("dependency wait not found")
	ErrDependencyWaitConflict = errors.New("dependency wait conflict")
	ErrDependencyCycle        = errors.New("dependency cycle")
)

type GoalKey struct {
	TenantId string `json:"tenantId"`
	GoalID   string `json:"goalId"`
}

func (k GoalKey) Validate() error {
	if strings.TrimSpace(k.TenantId) == "" || strings.TrimSpace(k.GoalID) == "" {
		return fmt.Errorf("%w: tenant and goal IDs are required", ErrInvalidDependency)
	}
	return nil
}

type GoalState string

const (
	GoalOpen      GoalState = "OPEN"
	GoalSucceeded GoalState = "SUCCEEDED"
	GoalFailed    GoalState = "FAILED"
)

type GoalFailure struct {
	Code              string    `json:"code"`
	GoalID            string    `json:"goalId,omitempty"`
	JobID             string    `json:"jobId,omitempty"`
	Message           string    `json:"message"`
	TerminalJobIDs    []string  `json:"terminalJobIds"`
	NonTerminalJobIDs []string  `json:"nonTerminalJobIds"`
	FailedAt          time.Time `json:"failedAt"`
}

type GoalInfo struct {
	GoalKey         GoalKey      `json:"goalKey"`
	State           GoalState    `json:"state"`
	Revision        int64        `json:"revision,string"`
	SuccessfulJobID string       `json:"successfulJobId,omitempty"`
	Failure         *GoalFailure `json:"failure,omitempty"`
	CreatedAt       time.Time    `json:"createdAt"`
	UpdatedAt       time.Time    `json:"updatedAt"`
	ClosedAt        *time.Time   `json:"closedAt,omitempty"`
}

type AddGoalCandidateRequest struct {
	GoalKey GoalKey `json:"goalKey"`
	JobID   string  `json:"jobId"`
}
type GoalCandidateInfo struct {
	GoalKey         GoalKey   `json:"goalKey"`
	JobID           string    `json:"jobId"`
	AddedAt         time.Time `json:"addedAt"`
	GoalState       GoalState `json:"goalState"`
	SuccessfulJobID string    `json:"successfulJobId,omitempty"`
}
type FailGoalRequest struct {
	GoalKey GoalKey `json:"-"`
	Message string  `json:"message"`
}

func (r FailGoalRequest) Validate() error {
	if err := r.GoalKey.Validate(); err != nil {
		return err
	}
	if !utf8.ValidString(r.Message) || strings.TrimSpace(r.Message) == "" || len(r.Message) > 16<<10 {
		return fmt.Errorf("%w: nonblank message of at most 16 KiB required", ErrInvalidDependency)
	}
	return nil
}

type GoalFailedError struct{ Failure GoalFailure }

func (e *GoalFailedError) Error() string {
	return fmt.Sprintf("goal %s failed: %s", e.Failure.GoalID, e.Failure.Message)
}
func (e *GoalFailedError) Is(target error) bool { return target == ErrGoalFailed }
func (e *GoalFailedError) NonRetryable() bool   { return true }

type DependencyTarget struct {
	JobID  string `json:"jobId,omitempty"`
	GoalID string `json:"goalId,omitempty"`
}
type Dependency struct {
	Target    DependencyTarget   `json:"target"`
	Condition JobPrereqCondition `json:"condition"`
}
type DependencyWait struct {
	WaitID       string       `json:"waitId"`
	Dependencies []Dependency `json:"dependencies"`
	Deadline     *time.Time   `json:"deadline,omitempty"`
}
type DependencyWaitKey struct {
	JobKey JobKey `json:"jobKey"`
	WaitID string `json:"waitId"`
}
type DependencyState string

const (
	DependencyPending   DependencyState = "PENDING"
	DependencySatisfied DependencyState = "SATISFIED"
	DependencyFailed    DependencyState = "FAILED"
	DependencyTimedOut  DependencyState = "TIMED_OUT"
	DependencyCancelled DependencyState = "CANCELLED"
)

type DependencyResolution struct {
	Target           DependencyTarget   `json:"target"`
	Condition        JobPrereqCondition `json:"condition"`
	State            DependencyState    `json:"state"`
	SatisfiedByJobID string             `json:"satisfiedByJobId,omitempty"`
	BlockedReason    string             `json:"blockedReason,omitempty"`
	Failure          *GoalFailure       `json:"failure,omitempty"`
}
type DependencyWaitInfo struct {
	JobKey                JobKey                 `json:"jobKey"`
	WaitID                string                 `json:"waitId"`
	State                 DependencyState        `json:"state"`
	Deadline              *time.Time             `json:"deadline,omitempty"`
	Dependencies          []DependencyResolution `json:"dependencies"`
	FailedDependencyIndex *int                   `json:"failedDependencyIndex,omitempty"`
	Failure               *GoalFailure           `json:"failure,omitempty"`
}

func (w DependencyWaitInfo) Err() error {
	switch w.State {
	case DependencySatisfied, DependencyPending:
		return nil
	case DependencyTimedOut:
		return context.DeadlineExceeded
	case DependencyCancelled:
		return context.Canceled
	case DependencyFailed:
		if w.Failure != nil && w.Failure.Code == "goal_failed" {
			return &GoalFailedError{Failure: *w.Failure}
		}
		if w.Failure != nil {
			return &dependencyFailedError{failure: *w.Failure}
		}
	}
	return fmt.Errorf("invalid dependency outcome %s", w.State)
}

type dependencyFailedError struct{ failure GoalFailure }

func (e *dependencyFailedError) Error() string      { return e.failure.Message }
func (e *dependencyFailedError) NonRetryable() bool { return true }
func (e *dependencyFailedError) Unwrap() error {
	return &AppError{Payload: AppErrorPayload{Message: e.Error(), Level: "error", Attrs: map[string]interface{}{"_jobdb_dependency_failure": e.failure}}}
}

func ValidateDependencies(deps []Dependency) error {
	seen := map[DependencyTarget]bool{}
	for _, d := range deps {
		if (strings.TrimSpace(d.Target.JobID) == "") == (strings.TrimSpace(d.Target.GoalID) == "") {
			return fmt.Errorf("%w: exactly one jobId or goalId required", ErrInvalidDependency)
		}
		if d.Condition != JobPrereqSuccess && (d.Condition != JobPrereqComplete || d.Target.GoalID != "") {
			return fmt.Errorf("%w: invalid target condition", ErrInvalidDependency)
		}
		if seen[d.Target] {
			return fmt.Errorf("%w: duplicate dependency target", ErrInvalidDependency)
		}
		seen[d.Target] = true
	}
	return nil
}
func ValidateDependencyWait(w *DependencyWait, submission bool) error {
	if w == nil {
		return nil
	}
	if w.WaitID == "" || (!submission && w.WaitID == "submission") || strings.ContainsAny(w.WaitID, "/\\?#% ") || len(w.Dependencies) == 0 {
		return fmt.Errorf("%w: valid waitId and nonempty dependencies required", ErrInvalidDependency)
	}
	return ValidateDependencies(w.Dependencies)
}
func ValidateDependencyReschedule(r RescheduleExecutionRequest) error {
	if r.DependencyWait == nil {
		return nil
	}
	if len(r.WaitForJobIDs) > 0 || r.WaitUntil != nil || r.AlternateRoute != nil || r.AlternateAfter != nil {
		return fmt.Errorf("%w: structured waits cannot combine legacy timing or dependencies", ErrInvalidDependency)
	}
	return ValidateDependencyWait(r.DependencyWait, false)
}

// GoalRuntime is the durable, tenant-scoped goal and dependency read/mutation API.
type GoalRuntime interface {
	GetGoal(context.Context, GoalKey) (GoalInfo, error)
	AddGoalCandidate(context.Context, AddGoalCandidateRequest) (GoalCandidateInfo, error)
	FailGoal(context.Context, FailGoalRequest) (GoalInfo, error)
	GetDependencyWait(context.Context, DependencyWaitKey) (DependencyWaitInfo, error)
}

// Unwrap preserves the typed failure through the existing application-error wire format.
func (e *GoalFailedError) Unwrap() error {
	return &AppError{Payload: AppErrorPayload{Message: e.Error(), Level: "error", Attrs: map[string]interface{}{"_jobdb_goal_failure": e.Failure}}}
}

// GoalFailureFromAppError restores a goal failure from a persisted application error.
func GoalFailureFromAppError(payload AppErrorPayload) (error, bool) {
	value, ok := payload.Attrs["_jobdb_goal_failure"]
	if !ok {
		value, ok = payload.Attrs["_jobdb_dependency_failure"]
		if ok {
			raw, err := json.Marshal(value)
			if err != nil {
				return nil, false
			}
			var failure GoalFailure
			if json.Unmarshal(raw, &failure) == nil && failure.Code == "job_dependency_failed" && failure.JobID != "" {
				return &dependencyFailedError{failure: failure}, true
			}
		}
	}
	if !ok {
		return nil, false
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	var failure GoalFailure
	if json.Unmarshal(raw, &failure) != nil || failure.Code != "goal_failed" || failure.GoalID == "" {
		return nil, false
	}
	return &GoalFailedError{Failure: failure}, true
}

// MarshalJSON keeps the exact-job failure variant distinct from a goal snapshot.
func (f GoalFailure) MarshalJSON() ([]byte, error) {
	if f.GoalID == "" && f.JobID != "" {
		return json.Marshal(struct {
			Code    string `json:"code"`
			JobID   string `json:"jobId"`
			Message string `json:"message"`
		}{f.Code, f.JobID, f.Message})
	}
	type snapshot GoalFailure
	return json.Marshal(snapshot(f))
}
