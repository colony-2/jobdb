package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// assembleAppendInput uses the same history snapshot as replay. The prefix is
// immutable after the first worker chapter, including on restarted jobs.
func (r *workerRunner) assembleAppendInput(input JobData, chapters []Chapter) (JobData, int, error) {
	if len(chapters) == 0 {
		return input, 1, nil
	}
	start, ok := chapters[0].Body.(jobdb.JobStartChapter)
	if !ok || start.Append == nil {
		return input, 1, nil
	}
	appends := []json.RawMessage{start.Append.Data}
	artifacts, err := input.GetArtifacts()
	if err != nil {
		return nil, 0, err
	}
	// Start artifacts have a separate namespace so even a start artifact named
	// appends/0/file cannot collide with an appended artifact.
	renamed := make([]Artifact, 0, len(artifacts))
	for _, a := range artifacts {
		renamed = append(renamed, &appendNamedArtifact{Artifact: a, name: "start/" + a.Name()})
	}
	add := func(index int, refs []jobdb.ArtifactKey) {
		for _, ref := range refs {
			a := jobdb.ExecutionArtifact{Name: fmt.Sprintf("appends/%d/%s", index, ref.Name), Key: &ref}
			renamed = append(renamed, a.ToArtifact(executionArtifactGetter{r.runtime}, r.jobKey.TenantId))
		}
	}
	add(0, start.AppendArtifacts)
	end := 1
	for end < len(chapters) {
		b, ok := chapters[end].Body.(jobdb.AdditionalInputChapter)
		if !ok {
			break
		}
		appends = append(appends, b.Input.Data)
		add(end, b.Artifacts)
		end++
	}
	raw, err := json.Marshal(struct {
		Start   json.RawMessage   `json:"start"`
		Appends []json.RawMessage `json:"appends"`
	}{start.Input.Data, appends})
	if err != nil {
		return nil, 0, err
	}
	return &SimpleTaskData{Data: raw, Artifacts: renamed}, end, nil
}

type appendNamedArtifact struct {
	Artifact
	name string
}

func (a *appendNamedArtifact) Name() string { return a.name }
