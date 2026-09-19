package jobdb

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Route identifies job work, or task work when TaskType is nonempty.
// Identifiers are opaque; punctuation, including colons, has no special meaning.
type Route struct {
	JobType  string `json:"jobType"`
	TaskType string `json:"taskType,omitempty"`
}

// ValidateIdentifier checks an opaque job or task type.
func ValidateIdentifier(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return fmt.Errorf("identifier must be nonempty UTF-8 without U+0000")
	}
	return nil
}

func (r Route) Validate() error {
	if err := ValidateIdentifier(r.JobType); err != nil {
		return fmt.Errorf("job type: %w", err)
	}
	if r.TaskType != "" {
		if err := ValidateIdentifier(r.TaskType); err != nil {
			return fmt.Errorf("task type: %w", err)
		}
	}
	return nil
}

// CloneRoute copies an optional route.
func CloneRoute(route *Route) *Route {
	if route == nil {
		return nil
	}
	copy := *route
	return &copy
}

// ValidateAlternateRoute checks the optional timeout route accompanying a handoff.
func ValidateAlternateRoute(route *Route, after *time.Duration, task *TaskWait) error {
	if route == nil {
		if after != nil {
			return fmt.Errorf("alternate route required with alternate delay")
		}
		return nil
	}
	if err := route.Validate(); err != nil {
		return err
	}
	if after == nil || *after < 0 {
		return fmt.Errorf("alternate route requires a nonnegative delay")
	}
	if route.TaskType != "" && task == nil {
		return fmt.Errorf("alternate task route requires task coordinates")
	}
	return nil
}

// ValidateRoutes checks the identifiers used by listing filters.
func (req ListJobsRequest) ValidateRoutes() error {
	for _, name := range req.JobTypes {
		if err := ValidateIdentifier(name); err != nil {
			return err
		}
	}
	for _, task := range req.JobTasks {
		if err := (Route{JobType: task.JobType, TaskType: task.TaskType}).Validate(); err != nil {
			return err
		}
		if task.TaskType == "" {
			return fmt.Errorf("task filter requires a task type")
		}
	}
	return nil
}
