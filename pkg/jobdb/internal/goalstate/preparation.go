package goalstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// Preparation reserves stable identities before history is written. The owning
// scheduler publishes the reserved goal and job together only after preparation.
type Preparation struct {
	Key          jobdb.JobKey
	RequestID    string
	PriorJobID   string
	ShareGoal    bool
	Descriptor   string
	Dependencies []jobdb.Dependency
}
type Prepared struct {
	Key       jobdb.JobKey
	GoalID    string
	Published bool
}

func SubmitPreparation(ctx context.Context, req jobdb.SubmitJobRequest, parent string) (Preparation, error) {
	if len(req.Job.Prerequisites) > 0 && len(req.Job.Dependencies) > 0 {
		return Preparation{}, jobdb.ErrInvalidDependency
	}
	if err := jobdb.ValidateDependencies(req.Job.Dependencies); err != nil {
		return Preparation{}, err
	}
	data, err := taskFingerprint(ctx, req.Job.Data)
	if err != nil {
		return Preparation{}, err
	}
	job := req.Job
	job.Data = nil
	job.JobID = ""
	if len(job.Dependencies) == 0 {
		job.Dependencies = nil
	}
	if len(job.Prerequisites) == 0 {
		job.Prerequisites = nil
	}
	descriptor, err := canonicalJSON(struct {
		Kind, Parent string
		Job          jobdb.SubmitJob
		Data         string
	}{"submit", parent, job, data})
	return Preparation{Key: jobdb.JobKey{TenantId: req.Job.TenantId, JobId: req.Job.JobID}, Descriptor: descriptor, Dependencies: req.Job.Dependencies}, err
}
func RestartPreparation(ctx context.Context, req jobdb.SubmitRestartJobRequest, parent string) (Preparation, error) {
	if req.Job.ShareGoal && req.Job.JobID == "" && req.RequestID == "" {
		return Preparation{}, fmt.Errorf("%w: shareGoal with generated job ID requires requestId", jobdb.ErrInvalidDependency)
	}
	if len(req.Job.Prerequisites) > 0 && len(req.Job.Dependencies) > 0 {
		return Preparation{}, jobdb.ErrInvalidDependency
	}
	if err := jobdb.ValidateDependencies(req.Job.Dependencies); err != nil {
		return Preparation{}, err
	}
	in, err := taskFingerprint(ctx, req.Job.ExtraTaskInput)
	if err != nil {
		return Preparation{}, err
	}
	out, err := taskFingerprint(ctx, req.Job.ExtraTaskOutput)
	if err != nil {
		return Preparation{}, err
	}
	job := req.Job
	job.ExtraTaskInput = nil
	job.ExtraTaskOutput = nil
	job.JobID = ""
	if len(job.Dependencies) == 0 {
		job.Dependencies = nil
	}
	if len(job.Prerequisites) == 0 {
		job.Prerequisites = nil
	}
	descriptor, err := canonicalJSON(struct {
		Kind, Parent  string
		Job           jobdb.SubmitRestartJob
		Input, Output string
	}{"restart", parent, job, in, out})
	return Preparation{Key: jobdb.JobKey{TenantId: req.Job.PriorJobKey.TenantId, JobId: req.Job.JobID}, RequestID: req.RequestID, PriorJobID: req.Job.PriorJobKey.JobId, ShareGoal: req.Job.ShareGoal, Descriptor: descriptor, Dependencies: req.Job.Dependencies}, err
}
func canonicalJSON(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	var canonical any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&canonical); err != nil {
		return "", err
	}
	raw, err = json.Marshal(canonical)
	return string(raw), err
}
func fingerprint(v any) (string, error) {
	canonical, err := canonicalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:]), nil
}

// PreparationMismatch preserves useful explicit-job conflict diagnostics without
// including user input or metadata values in error messages.
func PreparationMismatch(old, new string) error {
	var a, b struct {
		Data string
		Job  map[string]json.RawMessage
	}
	if json.Unmarshal([]byte(old), &a) == nil && json.Unmarshal([]byte(new), &b) == nil {
		if a.Data != b.Data {
			return jobdb.NewExistingJobMismatchError("different input")
		}
		for _, f := range []struct{ key, label string }{{"JobType", "job type"}, {"Metadata", "metadata"}, {"RunPolicy", "run policy"}, {"Prerequisites", "prerequisites"}, {"Dependencies", "dependencies"}, {"ShareGoal", "goal sharing"}, {"ClientPayloadUpdate", "initial client payload"}, {"Schema", "schema"}} {
			if !bytes.Equal(a.Job[f.key], b.Job[f.key]) {
				return jobdb.NewExistingJobMismatchError("different " + f.label)
			}
		}
	}
	return jobdb.NewExistingJobMismatchError("different job preparation")
}
func taskFingerprint(ctx context.Context, data jobdb.TaskData) (string, error) {
	if data == nil {
		return "", nil
	}
	raw, err := data.GetData()
	if err != nil {
		return "", err
	}
	arts, err := data.GetArtifacts()
	if err != nil {
		return "", err
	}
	type artifact struct{ Name, Hash string }
	values := make([]artifact, 0, len(arts))
	for _, a := range arts {
		hash, err := a.Sha256(ctx)
		if err != nil {
			return "", err
		}
		values = append(values, artifact{a.Name(), hash})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Name < values[j].Name })
	return fingerprint(struct {
		Data      json.RawMessage
		Artifacts []artifact
	}{raw, values})
}
