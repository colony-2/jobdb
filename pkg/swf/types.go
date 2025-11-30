package swf

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strata "github.com/colony-2/strata/strata-go/pkg/client/artifact"
	"github.com/invopop/jsonschema"
)

type dataImpl struct {
	serialized   []byte
	deserialized map[string]interface{}
}

type Data = json.RawMessage

// Duration wraps time.Duration to provide custom YAML marshaling/unmarshaling
// It serializes to/from human-readable strings like "1s", "500ms", "2m"
type Duration time.Duration

func (d Duration) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Type:        "string",
		Title:       "Duration",
		Description: "Human friendly duration string",
	}
}

func AsDuration(t time.Duration) *Duration {
	d := Duration(t)
	return &d
}

// MarshalYAML converts Duration to a YAML string
func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

// UnmarshalYAML parses a YAML string into a Duration
func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	duration, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration: %w", err)
	}

	*d = Duration(duration)
	return nil
}

// ToDuration converts to standard time.Duration
func (d Duration) ToDuration() time.Duration {
	return time.Duration(d)
}

// String implements the Stringer interface
func (d Duration) String() string {
	return time.Duration(d).String()
}

type RetryPolicy struct {
	InitialInterval        Duration `yaml:"initial_interval,omitempty"`
	BackoffCoefficient     float64  `yaml:"backoff_coefficient,omitempty"`
	MaximumInterval        Duration `yaml:"maximum_interval,omitempty"`
	MaximumAttempts        int32    `yaml:"maximum_attempts,omitempty"`
	NonRetryableErrorTypes []string `yaml:"non_retryable_error_types,omitempty"`
}

// RunPolicy bundles runtime directives for jobs/tasks.
// Future extensions may add fields like affinity or max duration.
type RunPolicy struct {
	Retry             RetryPolicy `yaml:"retry,omitempty"`
	InvocationTimeout *Duration   `yaml:"invocation_timeout,omitempty"`
	TotalTimeout      *Duration   `yaml:"total_timeout,omitempty"`
}

func DefaultRunPolicy() RunPolicy {
	return RunPolicy{
		InvocationTimeout: AsDuration(30 * time.Second),
		TotalTimeout:      AsDuration(30 * time.Minute),
		Retry: RetryPolicy{
			InitialInterval:        Duration(100 * time.Millisecond),
			BackoffCoefficient:     2.0,
			MaximumInterval:        Duration(30 * time.Second),
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"SystemError"},
		},
	}
}

// InputReference points to an input chapter for error payloads/metadata.
type InputReference struct {
	Ordinal int64  `json:"ordinal"`
	Hash    string `json:"hash,omitempty"`
}

type Lease = pgwf.Lease
type Artifact = strata.Artifact
type Dependencies = pgwf.JobDependencies

type JobId string

type SimpleTaskData struct {
	Data      Data
	Artifacts []Artifact
}

func (s *SimpleTaskData) GetDataOrPanic() Data {
	data, err := s.GetData()
	if err != nil {
		panic(err)
	}
	return data
}

func NewTaskData(data any, artifacts ...Artifact) (TaskData, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &SimpleTaskData{Data: bytes, Artifacts: artifacts}, nil
}

func NewTaskDataOrPanic(data any, artifacts ...Artifact) TaskData {
	td, err := NewTaskData(data, artifacts...)
	if err != nil {
		panic(err)
	}
	return td
}

// EnvelopedTaskData preserves payload kind metadata for round-tripping through envelopes.
type EnvelopedTaskData struct {
	SimpleTaskData
	Kind string
}

func (s *SimpleTaskData) GetData() (Data, error) {
	return s.Data, nil
}

func (s *SimpleTaskData) GetArtifacts() ([]Artifact, error) {
	return s.Artifacts, nil
}

type TaskData interface {
	GetData() (Data, error)
	GetDataOrPanic() Data
	GetArtifacts() ([]Artifact, error)
}

type TaskWorker interface {
	Name() string
	Run(context TaskContext, input TaskData) (TaskData, error)
}
