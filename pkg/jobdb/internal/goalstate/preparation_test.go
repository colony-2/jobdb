package goalstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func descriptor(t *testing.T, job jobdb.SubmitJob) Preparation {
	t.Helper()
	p, err := SubmitPreparation(context.Background(), jobdb.SubmitJobRequest{Job: job}, "parent")
	require.NoError(t, err)
	return p
}

func TestSubmitDescriptorLegacyCompatibility(t *testing.T) {
	job := jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", JobType: "work", Cell: "alias", CellRepo: "example.test/work"}
	p := descriptor(t, job)
	// Recreate the pre-selector descriptor, which serialized the same job without
	// a GoalID field. Omitted selection must compare byte-for-byte with it.
	raw, err := json.Marshal(job)
	require.NoError(t, err)
	var legacy map[string]any
	require.NoError(t, json.Unmarshal(raw, &legacy))
	delete(legacy, "GoalID")
	legacy["JobID"] = ""
	legacy["Cell"] = ""
	legacy["CellRepo"] = ""
	want, err := canonicalJSON(struct {
		Kind, Parent string
		Job          any
		Data         string
	}{"submit", "parent", legacy, ""})
	require.NoError(t, err)
	require.Equal(t, want, p.Descriptor)
	require.Empty(t, p.SelectedGoalID)
}

func TestSelectedSubmitDescriptorSemanticIdentity(t *testing.T) {
	base := func() jobdb.SubmitJob {
		return jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", Cell: "work", CellRepo: "example.test/work", JobType: "work", Data: jobdb.NewTaskDataOrPanic(map[string]any{"a": 1, "b": 2}, jobdb.NewArtifactFromBytes("input", []byte("original"))), Metadata: json.RawMessage(`{"a":1,"b":2}`)}
	}
	original := descriptor(t, base())
	require.Equal(t, "goal", original.SelectedGoalID)
	for name, change := range map[string]func(*jobdb.SubmitJob){
		"selection": func(j *jobdb.SubmitJob) { j.GoalID = "other" },
		"omission":  func(j *jobdb.SubmitJob) { j.GoalID = "" },
		"type":      func(j *jobdb.SubmitJob) { j.JobType = "other" },
		"tenant":    func(j *jobdb.SubmitJob) { j.TenantId = "other" },
		"metadata":  func(j *jobdb.SubmitJob) { j.Metadata = json.RawMessage(`{"a":3}`) },
		"data":      func(j *jobdb.SubmitJob) { j.Data = jobdb.NewTaskDataOrPanic(3) },
		"artifact": func(j *jobdb.SubmitJob) {
			j.Data = jobdb.NewTaskDataOrPanic(map[string]any{"a": 1, "b": 2}, jobdb.NewArtifactFromBytes("input", []byte("changed")))
		},
		"payload": func(j *jobdb.SubmitJob) {
			j.ClientPayloadUpdate = &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"state":1}`)}
		},
		"policy": func(j *jobdb.SubmitJob) { j.RunPolicy.Retry.MaximumAttempts = 4 },
		"dependency": func(j *jobdb.SubmitJob) {
			j.Dependencies = []jobdb.Dependency{{Target: jobdb.DependencyTarget{GoalID: "upstream"}, Condition: jobdb.JobPrereqSuccess}}
		},
		"prerequisite": func(j *jobdb.SubmitJob) {
			j.Prerequisites = []jobdb.JobPrerequisite{{JobID: "prior", Condition: jobdb.JobPrereqComplete}}
		},
		"schema":       func(j *jobdb.SubmitJob) { j.Schema = &jobdb.JobSchemaSelector{Hash: "schema"} },
		"availability": func(j *jobdb.SubmitJob) { v := time.Unix(123, 0); j.AvailableAt = &v },
	} {
		t.Run(name, func(t *testing.T) {
			j := base()
			change(&j)
			p := descriptor(t, j)
			require.NotEqual(t, original.Descriptor, p.Descriptor)
			require.Error(t, PreparationMismatch(original.Descriptor, p.Descriptor))
		})
	}
	equivalent := base()
	equivalent.Metadata = json.RawMessage(`{ "b":2, "a":1 }`)
	equivalent.Data = jobdb.NewTaskDataOrPanic(json.RawMessage(`{ "b":2, "a":1 }`), jobdb.NewArtifactFromBytes("input", []byte("original")))
	require.Equal(t, original.Descriptor, descriptor(t, equivalent).Descriptor)
	request := jobdb.SubmitJobRequest{Job: base(), WorkerID: "different", RequestTime: time.Now()}
	p, err := SubmitPreparation(context.Background(), request, "parent")
	require.NoError(t, err)
	require.Equal(t, original.Descriptor, p.Descriptor)
	p, err = SubmitPreparation(context.Background(), request, "other-parent")
	require.NoError(t, err)
	require.NotEqual(t, original.Descriptor, p.Descriptor)
}

func TestSelectedSubmitRequiresExplicitIdentity(t *testing.T) {
	for _, job := range []jobdb.SubmitJob{{GoalID: "goal"}, {TenantId: "tenant", GoalID: "goal"}, {TenantId: "tenant", JobID: "dest", GoalID: " "}} {
		_, err := SubmitPreparation(context.Background(), jobdb.SubmitJobRequest{Job: job}, "")
		require.ErrorIs(t, err, jobdb.ErrInvalidDependency)
	}
}

func TestSelectedCanonicalStagingMatchesDescriptor(t *testing.T) {
	a := jobdb.NewArtifactFromBytes("a", []byte("first"))
	b := jobdb.NewArtifactFromBytes("b", []byte("second"))
	base := jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", JobType: "work", Data: jobdb.NewTaskDataOrPanic(json.RawMessage(`{"b":2,"a":1}`), b, a), Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: "b"}, Condition: jobdb.JobPrereqComplete}, {Target: jobdb.DependencyTarget{GoalID: "a"}, Condition: jobdb.JobPrereqSuccess}}}
	same := base
	same.Data = jobdb.NewTaskDataOrPanic(json.RawMessage(`{ "a":1, "b":2 }`), a, b)
	same.Dependencies = []jobdb.Dependency{base.Dependencies[1], base.Dependencies[0]}
	same.Metadata = json.RawMessage(`{}`)
	same.RunPolicy = jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 1, BackoffCoefficient: 1}}
	require.Equal(t, descriptor(t, base).Descriptor, descriptor(t, same).Descriptor)
	normalized, err := CanonicalSelectedJob(base)
	require.NoError(t, err)
	raw, err := normalized.Data.GetData()
	require.NoError(t, err)
	require.Equal(t, `{"a":1,"b":2}`, string(raw))
	arts, err := normalized.Data.GetArtifacts()
	require.NoError(t, err)
	require.Equal(t, "a", arts[0].Name())
	require.Equal(t, "b", arts[1].Name())
}

func TestSelectedPrerequisiteDefaultsCanonicalize(t *testing.T) {
	base := jobdb.SubmitJob{TenantId: "tenant", JobID: "dest", GoalID: "goal", Prerequisites: []jobdb.JobPrerequisite{{JobID: "b"}, {JobID: "a", Condition: jobdb.JobPrereqSuccess}, {JobID: "b"}}}
	same := base
	same.Prerequisites = []jobdb.JobPrerequisite{{JobID: "a", Condition: jobdb.JobPrereqSuccess}, {JobID: "b", Condition: jobdb.JobPrereqComplete}}
	require.Equal(t, descriptor(t, base).Descriptor, descriptor(t, same).Descriptor)
}
