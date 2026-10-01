package workflow

import "time"

// TaskOptions controls dispatch independently of timeout and retry policy.
// Reconstruct these options from durable workflow state on every replay.
type TaskOptions struct {
	Alternate *TaskAlternate `json:"alternate,omitempty"`
}

// TaskAlternate declares another implementation of the same logical task.
// At is an absolute eligibility time, not a deadline for external completion.
type TaskAlternate struct {
	TaskType string    `json:"taskType"`
	At       time.Time `json:"at"`
}

type taskDataWithOptions struct {
	TaskData
	options TaskOptions
}

func (d taskDataWithOptions) TaskOptions() TaskOptions { return cloneTaskOptions(d.options) }

// WithTaskOptions attaches invocation options without changing the input bytes,
// artifacts, logical task identity, or input hash. It also works through existing
// JobContext wrappers. Wrappers replacing TaskData must preserve TaskOptionsFor.
func WithTaskOptions(data TaskData, options TaskOptions) TaskData {
	return taskDataWithOptions{TaskData: data, options: cloneTaskOptions(options)}
}

func TaskOptionsFor(data TaskData) TaskOptions {
	if source, ok := data.(interface{ TaskOptions() TaskOptions }); ok {
		return cloneTaskOptions(source.TaskOptions())
	}
	return TaskOptions{}
}

func cloneTaskOptions(options TaskOptions) TaskOptions {
	if options.Alternate != nil {
		alternate := *options.Alternate
		options.Alternate = &alternate
	}
	return options
}
