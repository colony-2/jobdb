package directimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterartifact "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/artifact"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
)

func storyKeyForJob(jobKey jobdb.JobKey) story.Key {
	return story.Key{
		AnthologyID: jobKey.TenantId,
		StoryID:     jobKey.JobId,
	}
}

func metadataPredicatesToPgwf(filter jobdb.MetadataFilter) ([]pgwf.MetadataPredicate, error) {
	preds, err := jobdb.MetadataPredicates(filter)
	if err != nil {
		return nil, err
	}
	out := make([]pgwf.MetadataPredicate, 0, len(preds))
	for _, pred := range preds {
		values, err := jsonEncodedMetadataValues(pred.Values)
		if err != nil {
			return nil, err
		}
		out = append(out, pgwf.MetadataPredicate{
			Path:   append([]string{"app"}, pred.Path...),
			Values: values,
		})
	}
	return out, nil
}

func jsonEncodedMetadataValues(values []any) ([]any, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]any, 0, len(values))
	for _, value := range values {
		switch v := value.(type) {
		case json.RawMessage:
			if !json.Valid(v) {
				return nil, fmt.Errorf("metadata predicate value must be valid JSON")
			}
			out = append(out, v)
		case []byte:
			if !json.Valid(v) {
				return nil, fmt.Errorf("metadata predicate value must be valid JSON")
			}
			out = append(out, json.RawMessage(v))
		default:
			encoded, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("metadata predicate value must be JSON-serializable: %w", err)
			}
			out = append(out, json.RawMessage(encoded))
		}
	}
	return out, nil
}

func concreteMetadataPredicatesToPgwf(predicates []jobdb.MetadataPredicate) []pgwf.MetadataPredicate {
	if len(predicates) == 0 {
		return nil
	}
	out := make([]pgwf.MetadataPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		values := append([]any(nil), predicate.Values...)
		out = append(out, pgwf.MetadataPredicate{
			Path:   append([]string{"app"}, predicate.Path...),
			Values: values,
		})
	}
	return out
}

func durationToLeaseSeconds(d time.Duration) int {
	if d == 0 {
		return 0
	}
	if d < 0 {
		return -1
	}
	seconds := int((d + time.Second - 1) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}

func fromChapterArtifact(chapterArt chapterartifact.Artifact) jobdb.Artifact {
	return &chapterArtifactAdapter{art: chapterArt}
}

func toChapterArtifact(art jobdb.Artifact) chapterartifact.Artifact {
	if adapter, ok := art.(*chapterArtifactAdapter); ok {
		return adapter.art
	}
	return &jobdbToChapterAdapter{art: art}
}

// FromChapterArtifactForRuntime exposes the direct artifact adapter to runtime packages.
func FromChapterArtifactForRuntime(chapterArt chapterartifact.Artifact) jobdb.Artifact {
	return fromChapterArtifact(chapterArt)
}

// ToChapterArtifactForRuntime exposes the reverse artifact adapter to runtime packages.
func ToChapterArtifactForRuntime(art jobdb.Artifact) chapterartifact.Artifact {
	return toChapterArtifact(art)
}

type chapterArtifactAdapter struct {
	art chapterartifact.Artifact
	key atomic.Pointer[jobdb.ArtifactKey]
}

func (a *chapterArtifactAdapter) Name() string { return a.art.Name() }
func (a *chapterArtifactAdapter) Size() int64  { return a.art.SizeBytes() }
func (a *chapterArtifactAdapter) Sha256(ctx context.Context) (string, error) {
	return a.art.Sha256(ctx)
}
func (a *chapterArtifactAdapter) WriteTo(ctx context.Context, w io.Writer) error {
	return a.art.WriteTo(ctx, w)
}
func (a *chapterArtifactAdapter) SaveToFile(ctx context.Context, path string) error {
	return a.art.SaveToFile(ctx, path)
}
func (a *chapterArtifactAdapter) Bytes(ctx context.Context) ([]byte, error) {
	return a.art.Bytes(ctx)
}
func (a *chapterArtifactAdapter) Open() (io.ReadCloser, error) {
	_, rc, err := a.art.ToInput(context.Background())
	return rc, err
}
func (a *chapterArtifactAdapter) ArtifactKey() (jobdb.ArtifactKey, error) {
	if value := a.key.Load(); value != nil {
		return *value, nil
	}
	return jobdb.ArtifactKey{}, jobdb.ErrArtifactKeyUnavailable
}
func (a *chapterArtifactAdapter) setArtifactKey(key jobdb.ArtifactKey) { a.key.Store(&key) }
func (a *chapterArtifactAdapter) Cleanup() error {
	if cleanup, ok := a.art.(interface{ Cleanup() error }); ok {
		return cleanup.Cleanup()
	}
	return nil
}

type jobdbToChapterAdapter struct {
	art jobdb.Artifact
}

func (a *jobdbToChapterAdapter) ID() string                                 { return "" }
func (a *jobdbToChapterAdapter) Name() string                               { return a.art.Name() }
func (a *jobdbToChapterAdapter) ContentType() string                        { return "application/octet-stream" }
func (a *jobdbToChapterAdapter) SizeBytes() int64                           { return a.art.Size() }
func (a *jobdbToChapterAdapter) Sha256(ctx context.Context) (string, error) { return a.art.Sha256(ctx) }
func (a *jobdbToChapterAdapter) WriteTo(ctx context.Context, w io.Writer) error {
	return a.art.WriteTo(ctx, w)
}
func (a *jobdbToChapterAdapter) SaveToFile(ctx context.Context, path string) error {
	return a.art.SaveToFile(ctx, path)
}
func (a *jobdbToChapterAdapter) Bytes(ctx context.Context) ([]byte, error) {
	return a.art.Bytes(ctx)
}
func (a *jobdbToChapterAdapter) ToInput(ctx context.Context) (chapterartifact.Descriptor, io.ReadCloser, error) {
	rc, err := a.art.Open()
	if err != nil {
		return chapterartifact.Descriptor{}, nil, err
	}
	return chapterartifact.Descriptor{
		Name:        a.art.Name(),
		ContentType: "application/octet-stream",
		SizeBytes:   a.art.Size(),
	}, rc, nil
}
