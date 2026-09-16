// Package postgres adapts JobDB's Postgres chapter and artifact storage to the
// public runtime core ChapterLog port.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/lib/pq"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/artifact"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/blobstore"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/pagination"
	postgresrows "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/postgres"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

// Config selects artifact storage for a Postgres-backed chapter log. Providers
// for non-local BlobStoreURI schemes are registered through optional imports.
type Config struct {
	BlobStoreURI           string
	MaxInlineArtifactBytes int64
	Logger                 *slog.Logger
}

// Store implements runtimecore.ChapterLog without exposing chapterstore
// internals to external runtime packages.
type Store struct {
	inner *chapterstore.Store
}

var _ runtimecore.ChapterLog = (*Store)(nil)

// NewSQLDB wraps a caller-owned database and migrates the chapter tables.
func NewSQLDB(ctx context.Context, db *sql.DB, cfg Config) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("postgres chapter log: db is required")
	}
	rows, err := postgresrows.NewSQLDB(db)
	if err != nil {
		return nil, err
	}
	return newStore(ctx, rows, cfg)
}

// OpenDSN opens and owns a database connection and migrates the chapter tables.
func OpenDSN(ctx context.Context, dsn string, cfg Config) (*Store, error) {
	rows, err := postgresrows.OpenDSN(ctx, dsn)
	if err != nil {
		return nil, err
	}
	store, err := newStore(ctx, rows, cfg)
	if err != nil {
		_ = rows.Close()
		return nil, err
	}
	return store, nil
}

func newStore(ctx context.Context, rows *postgresrows.Store, cfg Config) (*Store, error) {
	if cfg.BlobStoreURI == "" {
		return nil, fmt.Errorf("postgres chapter log: blob store URI is required")
	}
	blobs, err := blobstore.OpenURIContext(ctx, cfg.BlobStoreURI)
	if err != nil {
		return nil, err
	}
	inner, err := chapterstore.New(rows, blobs, chapterstore.Config{
		MaxInlineArtifactBytes: cfg.MaxInlineArtifactBytes,
		Logger:                 cfg.Logger,
	})
	if err != nil {
		if closer, ok := blobs.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return nil, err
	}
	return &Store{inner: inner}, nil
}

// Close releases artifact storage and any database connection opened by OpenDSN.
func (s *Store) Close(ctx context.Context) error {
	if s == nil || s.inner == nil {
		return nil
	}
	return s.inner.Close(ctx)
}

func (s *Store) Create(ctx context.Context, key runtimecore.ChapterLogKey, initial runtimecore.EncodedChapter) error {
	if initial.Ordinal != 0 {
		return fmt.Errorf("postgres chapter log: initial chapter ordinal must be zero")
	}
	chapter, err := toStoryChapter(initial)
	if err != nil {
		return err
	}
	_, err = s.inner.CreateStory(ctx, storyKey(key), story.CreateOptions{InitialChapter: chapter})
	return translateError(err, jobdb.ErrJobNotFound)
}

func (s *Store) ClonePrefix(ctx context.Context, req runtimecore.ClonePrefixRequest) error {
	opts := story.CloneOptions{DestinationKey: storyKey(req.DestinationKey), LastOrdinal: req.LastOrdinal}
	if req.Append != nil {
		if req.Append.Ordinal != req.LastOrdinal+1 {
			return fmt.Errorf("postgres chapter log: appended chapter ordinal must follow cloned prefix")
		}
		chapter, err := toStoryChapter(*req.Append)
		if err != nil {
			return err
		}
		opts.CreateOptions.InitialChapter = chapter
	}
	_, err := s.inner.CloneStory(ctx, storyKey(req.SourceKey), opts)
	return translateError(err, jobdb.ErrJobNotFound)
}

func (s *Store) Append(ctx context.Context, key runtimecore.ChapterLogKey, chapter runtimecore.EncodedChapter) error {
	stored, err := toStoryChapter(chapter)
	if err != nil {
		return err
	}
	return translateError(s.inner.SaveChapter(ctx, storyKey(key), stored), jobdb.ErrJobNotFound)
}

func (s *Store) Get(ctx context.Context, key runtimecore.ChapterLogKey, ordinal int64) (runtimecore.EncodedChapter, error) {
	chapter, err := s.inner.Chapter(ctx, storyKey(key), ordinal)
	if err != nil {
		return runtimecore.EncodedChapter{}, translateError(err, jobdb.ErrChapterNotFound)
	}
	return fromStoryChapter(ctx, chapter)
}

func (s *Store) List(ctx context.Context, key runtimecore.ChapterLogKey, rng runtimecore.ChapterRange) ([]runtimecore.EncodedChapter, error) {
	if rng.StartOrdinal < 0 {
		return nil, fmt.Errorf("postgres chapter log: start ordinal must be nonnegative")
	}
	st, err := s.inner.Story(ctx, storyKey(key))
	if err != nil {
		return nil, translateError(err, jobdb.ErrJobNotFound)
	}
	iter, err := st.Chapters(ctx, story.ChaptersOptions{PageSize: 100, Direction: story.DirectionForward})
	if err != nil {
		return nil, translateError(err, jobdb.ErrJobNotFound)
	}
	out := make([]runtimecore.EncodedChapter, 0)
	for iter.HasNext() {
		chapter, err := iter.Next(ctx)
		if errors.Is(err, pagination.ErrNoMoreItems) {
			break
		}
		if err != nil {
			return nil, translateError(err, jobdb.ErrChapterNotFound)
		}
		if chapter.Ordinal() < rng.StartOrdinal {
			continue
		}
		if rng.EndOrdinal != nil && chapter.Ordinal() > *rng.EndOrdinal {
			break
		}
		encoded, err := fromStoryChapter(ctx, chapter)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded)
	}
	return out, nil
}

func (s *Store) Count(ctx context.Context, key runtimecore.ChapterLogKey) (int64, error) {
	st, err := s.inner.Story(ctx, storyKey(key))
	if err != nil {
		return 0, translateError(err, jobdb.ErrJobNotFound)
	}
	return st.ChapterCount(), nil
}

func (s *Store) OpenArtifact(ctx context.Context, lookup runtimecore.ArtifactLookup) (jobdb.ArtifactReader, error) {
	chapter, err := s.inner.Chapter(ctx, storyKey(runtimecore.ChapterLogKey{JobKey: lookup.JobKey}), lookup.Ordinal)
	if err != nil {
		return nil, translateError(err, jobdb.ErrChapterNotFound)
	}
	for _, art := range chapter.Artifacts() {
		if art == nil || art.Name() != lookup.Name {
			continue
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		if lookup.Digest == "" || lookup.Digest == digest {
			return artifactReader{art: art}, nil
		}
	}
	return nil, fmt.Errorf("postgres chapter log: artifact %q not found for job %s ordinal %d", lookup.Name, lookup.JobKey.JobId, lookup.Ordinal)
}

func storyKey(key runtimecore.ChapterLogKey) story.Key {
	return story.Key{AnthologyID: key.JobKey.TenantId, StoryID: key.JobKey.JobId}
}

func toStoryChapter(encoded runtimecore.EncodedChapter) (story.Chapter, error) {
	if encoded.Ordinal < 0 {
		return nil, fmt.Errorf("postgres chapter log: chapter ordinal must be nonnegative")
	}
	chapter := story.NewChapter().WithOrdinal(encoded.Ordinal).WithBytes(encoded.Payload)
	for _, upload := range encoded.ArtifactUploads {
		if upload.Open == nil {
			return nil, fmt.Errorf("postgres chapter log: artifact %q is missing opener", upload.Name)
		}
		opener := upload.Open
		chapter.AddArtifact(artifact.FromReader(upload.Name, "application/octet-stream", upload.Size,
			func(context.Context) (io.ReadCloser, error) { return opener() }))
	}
	return chapter, nil
}

func fromStoryChapter(ctx context.Context, chapter story.Chapter) (runtimecore.EncodedChapter, error) {
	out := runtimecore.EncodedChapter{
		Ordinal: chapter.Ordinal(),
		Payload: append([]byte(nil), chapter.Body()...),
	}
	for _, art := range chapter.Artifacts() {
		if art == nil {
			continue
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return runtimecore.EncodedChapter{}, err
		}
		out.Artifacts = append(out.Artifacts, jobdb.StoredArtifact{
			Name: art.Name(), Digest: digest, Size: art.SizeBytes(),
		})
	}
	return out, nil
}

func translateError(err error, notFound error) error {
	var pqError *pq.Error
	switch {
	case errors.Is(err, core.ErrConflict):
		return fmt.Errorf("%w: %v", jobdb.ErrConflict, err)
	case errors.As(err, &pqError) && pqError.Code == "23505":
		return fmt.Errorf("%w: %v", jobdb.ErrConflict, err)
	case errors.Is(err, core.ErrNotFound):
		return fmt.Errorf("%w: %v", notFound, err)
	default:
		return err
	}
}

type artifactReader struct{ art artifact.Artifact }

func (r artifactReader) Open() (io.ReadCloser, error) {
	_, reader, err := r.art.ToInput(context.Background())
	return reader, err
}

func (r artifactReader) Size() int64  { return r.art.SizeBytes() }
func (r artifactReader) Name() string { return r.art.Name() }
