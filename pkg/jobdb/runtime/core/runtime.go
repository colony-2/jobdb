package runtimecore

import (
	"context"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// Runtime owns JobDB workflow semantics and delegates durable state to its
// backend ports. Its methods are being moved here from concrete runtimes.
type Runtime struct {
	scheduler Scheduler
	chapters  ChapterLog
	schemas   *SchemaRegistry
	now       func() time.Time
}

// NewRuntime wires a JobDB runtime core to backend storage.
func NewRuntime(cfg Config) (*Runtime, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	schemas, err := NewSchemaRegistry(SchemaRegistryConfig{Store: cfg.Schemas, Now: cfg.Now})
	if err != nil {
		return nil, err
	}
	return &Runtime{
		scheduler: cfg.Scheduler,
		chapters:  cfg.Chapters,
		schemas:   schemas,
		now:       nowFunc(cfg.Now),
	}, nil
}

// GetChapter returns a stored visible chapter.
func (r *Runtime) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	if err := r.validate(); err != nil {
		return jobdb.Chapter{}, err
	}
	if err := ref.JobKey.Validate(); err != nil {
		return jobdb.Chapter{}, err
	}
	if ref.Ordinal < 0 {
		return jobdb.Chapter{}, fmt.Errorf("chapter ordinal must be nonnegative")
	}
	stored, err := r.chapters.Get(ctx, ChapterLogKey{JobKey: ref.JobKey}, ref.Ordinal)
	if err != nil {
		return jobdb.Chapter{}, err
	}
	return DecodeChapter(stored)
}

// ListChapters returns visible chapters in ascending ordinal order.
func (r *Runtime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if err := req.JobKey.Validate(); err != nil {
		return nil, err
	}
	if req.StartOrdinal < 0 || (req.EndOrdinal != nil && *req.EndOrdinal < req.StartOrdinal) {
		return nil, fmt.Errorf("invalid chapter ordinal range")
	}
	stored, err := r.chapters.List(ctx, ChapterLogKey{JobKey: req.JobKey}, ChapterRange{
		StartOrdinal: req.StartOrdinal, EndOrdinal: req.EndOrdinal,
	})
	if err != nil {
		return nil, err
	}
	chapters := make([]jobdb.Chapter, 0, len(stored))
	for _, encoded := range stored {
		chapter, err := DecodeChapter(encoded)
		if err != nil {
			return nil, err
		}
		chapters = append(chapters, chapter)
	}
	return chapters, nil
}

// OpenArtifact opens a committed chapter artifact.
func (r *Runtime) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if err := ref.JobKey.Validate(); err != nil {
		return nil, err
	}
	if ref.Ordinal < 0 || ref.Name == "" {
		return nil, fmt.Errorf("artifact ordinal and name are required")
	}
	return r.chapters.OpenArtifact(ctx, ArtifactLookup{
		JobKey: ref.JobKey, Ordinal: ref.Ordinal, Name: ref.Name, Digest: ref.Digest,
	})
}

func (r *Runtime) validate() error {
	if r == nil || r.scheduler == nil || r.chapters == nil || r.schemas == nil {
		return fmt.Errorf("runtime core is not configured")
	}
	return nil
}
