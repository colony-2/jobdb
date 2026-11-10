package goalstate

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// SelectedGoal reads the additive selector without changing legacy descriptor
// encodings. Malformed descriptors fail closed rather than becoming ordinary.
func SelectedGoal(descriptor string) (string, error) {
	var d struct{ Job struct{ GoalID string } }
	err := json.Unmarshal([]byte(descriptor), &d)
	return d.Job.GoalID, err
}

func ValidateSelectedGoal(ctx context.Context, req Preparation, target jobdb.ResolvedCell, goal jobdb.GoalInfo) error {
	if req.SelectedGoalID == "" {
		return nil
	}
	if err := jobdb.ValidatePrimaryGoalSelection(jobdb.SubmitJob{TenantId: req.Key.TenantId, JobID: req.Key.JobId, GoalID: req.SelectedGoalID}); err != nil {
		return err
	}
	if err := jobdb.AuthorizePrimaryGoalSelection(ctx, goal.GoalKey, target); err != nil {
		return err
	}
	if goal.GoalKey != (jobdb.GoalKey{TenantId: req.Key.TenantId, GoalID: req.SelectedGoalID}) {
		return jobdb.ErrGoalNotFound
	}
	if goal.State != jobdb.GoalOpen {
		return jobdb.ErrGoalConflict
	}
	if goal.CellID == "" || target.CellID == "" || goal.CellID != target.CellID {
		return jobdb.ErrCellConflict
	}
	if goal.CellRepo == "" || goal.CellRepo != target.CellRepo {
		return jobdb.ErrCellRepoMismatch
	}
	return nil
}

// AcceptedHandle requires a captured complete receipt for selected publication.
// Legacy ordinary preparations can continue using their historical identity.
func (p Prepared) AcceptedHandle(selected bool) (jobdb.JobHandle, error) {
	if p.Receipt != nil {
		h := *p.Receipt
		if h.JobKey != p.Key || h.GoalID != p.GoalID || h.Cell == "" || h.CellID == "" || h.CellRepo == "" {
			return jobdb.JobHandle{}, jobdb.ErrSubmissionReceiptUnavailable
		}
		return h, nil
	}
	if selected {
		return jobdb.JobHandle{}, jobdb.ErrSubmissionReceiptUnavailable
	}
	return p.Target.Handle(p.Key, p.GoalID), nil
}

// CanonicalSelectedJob keeps equivalent JSON and unordered artifact/dependency
// representations identical through both descriptor and staged chapter writes.
// Ordinary submissions retain their historical encodings and default behavior.
func CanonicalSelectedJob(job jobdb.SubmitJob) (jobdb.SubmitJob, error) {
	if job.GoalID == "" {
		return job, nil
	}
	if job.Data != nil {
		job.Data = canonicalSelectionData{job.Data}
	}
	if len(job.Metadata) == 0 {
		job.Metadata = json.RawMessage(`{}`)
	} else {
		raw, err := canonicalJSON(job.Metadata)
		if err != nil {
			return job, err
		}
		job.Metadata = json.RawMessage(raw)
	}
	if job.RunPolicy.Retry.MaximumAttempts <= 0 {
		job.RunPolicy.Retry.MaximumAttempts = 1
	}
	if job.RunPolicy.Retry.BackoffCoefficient == 0 {
		job.RunPolicy.Retry.BackoffCoefficient = 1
	}
	if job.RunPolicy.InvocationTimeout != nil && *job.RunPolicy.InvocationTimeout < 0 {
		job.RunPolicy.InvocationTimeout = nil
	}
	if job.RunPolicy.TotalTimeout != nil && *job.RunPolicy.TotalTimeout < 0 {
		job.RunPolicy.TotalTimeout = nil
	}
	job.Prerequisites = append([]jobdb.JobPrerequisite(nil), job.Prerequisites...)
	if len(job.Prerequisites) > 0 {
		seen := map[string]bool{}
		normalized := make([]jobdb.JobPrerequisite, 0, len(job.Prerequisites))
		for _, p := range job.Prerequisites {
			if seen[p.JobID] {
				continue
			}
			seen[p.JobID] = true
			if p.Condition == "" {
				p.Condition = jobdb.JobPrereqComplete
			}
			normalized = append(normalized, p)
		}
		sort.Slice(normalized, func(i, j int) bool { return normalized[i].JobID < normalized[j].JobID })
		job.Prerequisites = normalized
	}
	job.Dependencies = append([]jobdb.Dependency(nil), job.Dependencies...)
	sort.Slice(job.Dependencies, func(i, j int) bool {
		a, b := job.Dependencies[i], job.Dependencies[j]
		if a.Target.JobID != b.Target.JobID {
			return a.Target.JobID < b.Target.JobID
		}
		if a.Target.GoalID != b.Target.GoalID {
			return a.Target.GoalID < b.Target.GoalID
		}
		return a.Condition < b.Condition
	})
	if job.AvailableAt != nil {
		utc := job.AvailableAt.UTC()
		job.AvailableAt = &utc
	}
	return job, nil
}

type canonicalSelectionData struct{ jobdb.TaskData }

func (d canonicalSelectionData) GetData() (jobdb.Data, error) {
	raw, err := d.TaskData.GetData()
	if err != nil {
		return nil, err
	}
	value, err := canonicalJSON(json.RawMessage(raw))
	return jobdb.Data(value), err
}
func (d canonicalSelectionData) GetArtifacts() ([]jobdb.Artifact, error) {
	arts, err := d.TaskData.GetArtifacts()
	if err != nil {
		return nil, err
	}
	arts = append([]jobdb.Artifact(nil), arts...)
	for _, a := range arts {
		if a == nil {
			return nil, fmt.Errorf("job artifact is nil")
		}
	}
	sort.Slice(arts, func(i, j int) bool { return arts[i].Name() < arts[j].Name() })
	return arts, nil
}
