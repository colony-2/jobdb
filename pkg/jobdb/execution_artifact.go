package jobdb

import (
	"context"
	"fmt"
	"io"
)

// CaptureExecutionArtifact separates transient artifact ownership from the wire
// representation. Existing durable references never read artifact content.
func CaptureExecutionArtifact(ctx context.Context, a Artifact) (ExecutionArtifact, error) {
	if a == nil || a.Name() == "" {
		return ExecutionArtifact{}, fmt.Errorf("artifact name is required")
	}
	out := ExecutionArtifact{Name: a.Name()}
	if key, err := a.ArtifactKey(); err == nil && key.Validate() == nil && key.SizeBytes >= 0 {
		out.Key = &key
		return out, nil
	}
	// Own a stable copy before the caller cleans up a temporary artifact.
	data, err := a.Bytes(ctx)
	if err != nil {
		return out, err
	}
	out.source = NewArtifactFromBytes(a.Name(), data)
	return out, nil
}

// Stage assigns a deterministic storage reference to a transient artifact.
// The source remains available until the referencing chapter is committed.
func (a *ExecutionArtifact) Stage(key ArtifactKey) (*ArtifactUpload, error) {
	if a.Key != nil {
		return nil, nil
	}
	if a.source == nil {
		return nil, fmt.Errorf("execution artifact %q has no source", a.Name)
	}
	key.SizeBytes = a.source.Size()
	if err := key.Validate(); err != nil {
		return nil, err
	}
	a.Key = &key
	AssignArtifactKey(a.source, key)
	return &ArtifactUpload{Name: key.Name, Size: key.SizeBytes, Open: a.source.Open}, nil
}

// ToArtifact reconstructs an artifact without fetching persisted content.
func (a ExecutionArtifact) ToArtifact(getter ArtifactGetter, tenant string) Artifact {
	if a.source != nil {
		return a.source
	}
	if a.Key == nil {
		return NewArtifact(a.Name, func() (io.ReadCloser, int64, error) {
			return nil, 0, fmt.Errorf("execution artifact %q has no durable key", a.Name)
		}, nil)
	}
	return &namedExecutionArtifact{Artifact: a.Key.ToLazyArtifact(getter, tenant), name: a.Name}
}

type namedExecutionArtifact struct {
	Artifact
	name string
}

func (a *namedExecutionArtifact) Name() string { return a.name }
