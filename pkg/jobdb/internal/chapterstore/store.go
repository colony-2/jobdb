package chapterstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/artifact"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/pagination"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
)

const DefaultMaxInlineArtifactBytes int64 = 256

type Config struct {
	MaxInlineArtifactBytes int64
	Logger                 *slog.Logger
}

type Store struct {
	rows      storage.RowStore
	blobs     storage.BlobStore
	maxInline int64
	logger    *slog.Logger
}

func New(rows storage.RowStore, blobs storage.BlobStore, cfg Config) (*Store, error) {
	if rows == nil {
		return nil, fmt.Errorf("chapterstore: rowstore is required")
	}
	if blobs == nil {
		return nil, fmt.Errorf("chapterstore: blobstore is required")
	}
	maxInline := cfg.MaxInlineArtifactBytes
	if maxInline < 0 {
		maxInline = 0
	}
	if maxInline == 0 {
		maxInline = DefaultMaxInlineArtifactBytes
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Store{rows: rows, blobs: blobs, maxInline: maxInline, logger: logger}, nil
}

func (s *Store) Close(context.Context) error {
	var errs []error
	if closer, ok := s.rows.(interface{ Close() error }); ok {
		errs = append(errs, closer.Close())
	}
	return errors.Join(errs...)
}

func (s *Store) CreateStory(ctx context.Context, key story.Key, opts story.CreateOptions) (story.Story, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	rec, err := s.rows.CreateStory(ctx, storageKey(key), now)
	if err != nil {
		return nil, translateRowError(err)
	}
	if opts.InitialChapter != nil {
		chapter, err := s.chapterRecordFromStoryChapter(ctx, key, 0, opts.InitialChapter)
		if err != nil {
			return nil, err
		}
		rec, err = s.rows.AppendChapter(ctx, chapter, now)
		if err != nil {
			return nil, translateRowError(err)
		}
	}
	return s.storyFromRecord(rec), nil
}

func (s *Store) Story(ctx context.Context, key story.Key) (story.Story, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}
	rec, err := s.rows.GetStory(ctx, storageKey(key))
	if err != nil {
		return nil, translateRowError(err)
	}
	return s.storyFromRecord(rec), nil
}

func (s *Store) Chapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error) {
	if err := s.validateKey(key); err != nil {
		return nil, err
	}
	resolvedKey, resolvedOrdinal, err := s.resolveChapterRef(ctx, storageKey(key), ordinal)
	if err != nil {
		return nil, translateRowError(err)
	}
	rec, err := s.rows.ReadChapterIncludingDeleted(ctx, resolvedKey, resolvedOrdinal)
	if err != nil {
		return nil, translateRowError(err)
	}
	return s.chapterFromRecord(rec)
}

func (s *Store) SaveChapter(ctx context.Context, key story.Key, chap story.Chapter) error {
	if err := s.validateKey(key); err != nil {
		return err
	}
	if chap == nil {
		return fmt.Errorf("chapterstore: chapter is required")
	}
	ordinal := chap.Ordinal()
	if ordinal < 0 {
		meta, err := s.rows.GetStory(ctx, storageKey(key))
		if err != nil {
			return translateRowError(err)
		}
		ordinal = meta.ChapterCount
	}
	record, err := s.chapterRecordFromStoryChapter(ctx, key, ordinal, chap)
	if err != nil {
		return err
	}
	if _, err := s.rows.AppendChapter(ctx, record, time.Now().UTC()); err != nil {
		return translateRowError(err)
	}
	return nil
}

func (s *Store) CloneStory(ctx context.Context, source story.Key, opts story.CloneOptions) (story.Story, error) {
	if err := s.validateKey(source); err != nil {
		return nil, err
	}
	dest := opts.DestinationKey
	if strings.TrimSpace(dest.AnthologyID) == "" {
		dest.AnthologyID = source.AnthologyID
	}
	if err := s.validateKey(dest); err != nil {
		return nil, fmt.Errorf("story: destination key is required: %w", err)
	}
	srcKey := storageKey(source)
	srcMeta, err := s.rows.GetStory(ctx, srcKey)
	if err != nil {
		return nil, translateRowError(err)
	}
	lastOrdinal := opts.LastOrdinal
	if lastOrdinal < 0 {
		lastOrdinal = srcMeta.LatestOrdinal
	}
	if lastOrdinal < 0 || lastOrdinal > srcMeta.LatestOrdinal {
		return nil, fmt.Errorf("story: last ordinal exceeds source range")
	}
	rec, err := s.rows.CreateLinkedStory(ctx, storageKey(dest), storage.StoryLink{Key: srcKey, ForkOrdinal: lastOrdinal}, time.Now().UTC())
	if err != nil {
		return nil, translateRowError(err)
	}
	out := s.storyFromRecord(rec)
	if opts.CreateOptions.InitialChapter != nil {
		if err := out.AppendChapter(ctx, opts.CreateOptions.InitialChapter); err != nil {
			return nil, fmt.Errorf("story: append next chapter: %w", err)
		}
	}
	return out, nil
}

func (s *Store) validateKey(key story.Key) error {
	if s == nil || s.rows == nil || s.blobs == nil {
		return fmt.Errorf("chapterstore: store is required")
	}
	return key.Validate()
}

func (s *Store) storyFromRecord(rec storage.StoryRecord) story.Story {
	return &storyHandle{
		store: s,
		key:   storyKey(rec.Key),
		state: summaryFromRecord(rec),
	}
}

func (s *Store) chapterRecordFromStoryChapter(ctx context.Context, key story.Key, ordinal int64, chap story.Chapter) (storage.ChapterRecord, error) {
	body := chap.Body()
	if len(body) == 0 {
		return storage.ChapterRecord{}, fmt.Errorf("chapterstore: chapter body missing")
	}
	if !json.Valid(body) {
		return storage.ChapterRecord{}, fmt.Errorf("chapterstore: chapter body must be valid JSON")
	}
	rec := storage.ChapterRecord{
		Key:       storageKey(key),
		Ordinal:   ordinal,
		Body:      append(json.RawMessage(nil), body...),
		Artifacts: make([]storage.ArtifactRecord, 0, len(chap.Artifacts())),
	}
	for _, art := range chap.Artifacts() {
		if art == nil {
			continue
		}
		record, err := s.artifactRecord(ctx, art)
		if err != nil {
			return storage.ChapterRecord{}, err
		}
		rec.Artifacts = append(rec.Artifacts, record)
	}
	return rec, nil
}

func (s *Store) artifactRecord(ctx context.Context, art artifact.Artifact) (storage.ArtifactRecord, error) {
	desc, reader, err := art.ToInput(ctx)
	if err != nil {
		return storage.ArtifactRecord{}, err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		return storage.ArtifactRecord{}, err
	}
	name := strings.TrimSpace(desc.Name)
	if name == "" {
		name = art.Name()
	}
	if name == "" {
		return storage.ArtifactRecord{}, fmt.Errorf("chapterstore: artifact name is required")
	}
	size := int64(len(data))
	if desc.SizeBytes > 0 && desc.SizeBytes != size {
		return storage.ArtifactRecord{}, fmt.Errorf("chapterstore: artifact %s size mismatch", name)
	}
	digest := sha256Hex(data)
	if desc.Sha256 != "" && desc.Sha256 != digest {
		return storage.ArtifactRecord{}, fmt.Errorf("chapterstore: artifact %s sha256 mismatch", name)
	}
	record := storage.ArtifactRecord{
		ID:          fallback(descID(art), uuid.NewString()),
		Name:        name,
		ContentType: fallback(desc.ContentType, "application/octet-stream"),
		SizeBytes:   size,
		Sha256:      digest,
	}
	if size <= s.maxInline {
		record.InlineData = append([]byte(nil), data...)
		return record, nil
	}
	path, err := s.blobs.Save(ctx, bytes.NewReader(data))
	if err != nil {
		return storage.ArtifactRecord{}, err
	}
	record.BlobPath = path
	return record, nil
}

func (s *Store) chapterFromRecord(rec storage.ChapterRecord) (story.Chapter, error) {
	arts := make([]artifact.Artifact, 0, len(rec.Artifacts))
	for _, raw := range rec.Artifacts {
		art := s.artifactFromRecord(raw)
		arts = append(arts, art)
	}
	return story.NewPersistedChapter(rec.Ordinal, rec.Body, arts), nil
}

func (s *Store) artifactFromRecord(raw storage.ArtifactRecord) artifact.Artifact {
	opts := []artifact.Option{artifact.WithID(raw.ID), artifact.WithSha256(raw.Sha256)}
	if raw.InlineData != nil {
		return artifact.FromBytes(raw.Name, raw.ContentType, raw.InlineData, opts...)
	}
	if raw.BlobPath != "" {
		path := raw.BlobPath
		return artifact.FromReader(raw.Name, raw.ContentType, raw.SizeBytes, func(context.Context) (io.ReadCloser, error) {
			return s.blobs.Open(path)
		}, opts...)
	}
	return artifact.FromBytes(raw.Name, raw.ContentType, nil, opts...)
}

func (s *Store) resolveChapterRef(ctx context.Context, key storage.StoryKey, selector int64) (storage.StoryKey, int64, error) {
	meta, err := s.rows.GetStory(ctx, key)
	if err != nil {
		return storage.StoryKey{}, 0, err
	}
	target := selector
	if selector < 0 {
		if meta.LatestOrdinal < 0 {
			return storage.StoryKey{}, 0, storage.ErrChapterNotFound
		}
		target = meta.LatestOrdinal + selector + 1
	}
	return s.resolveLinkedOrdinal(ctx, meta, target)
}

func (s *Store) resolveLinkedOrdinal(ctx context.Context, meta storage.StoryRecord, ordinal int64) (storage.StoryKey, int64, error) {
	const maxLinkDepth = 1024
	for depth := 0; depth < maxLinkDepth; depth++ {
		if ordinal < 0 || ordinal > meta.LatestOrdinal {
			return storage.StoryKey{}, 0, storage.ErrChapterNotFound
		}
		if meta.Link == nil || ordinal > meta.Link.ForkOrdinal {
			return meta.Key, ordinal, nil
		}
		base, err := s.rows.GetStoryIncludingDeleted(ctx, meta.Link.Key)
		if err != nil {
			return storage.StoryKey{}, 0, err
		}
		meta = base
	}
	return storage.StoryKey{}, 0, fmt.Errorf("chapterstore: story link chain exceeds %d levels", maxLinkDepth)
}

type storyHandle struct {
	store *Store
	key   story.Key

	mu    sync.RWMutex
	state story.Summary
}

func (h *storyHandle) Key() story.Key { return h.key }

func (h *storyHandle) State() story.Summary {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state
}

func (h *storyHandle) Refresh(ctx context.Context) error {
	rec, err := h.store.rows.GetStory(ctx, storageKey(h.key))
	if err != nil {
		return translateRowError(err)
	}
	h.setState(summaryFromRecord(rec))
	return nil
}

func (h *storyHandle) NewChapter() *story.ChapterBuilder {
	return story.NewChapter()
}

func (h *storyHandle) AppendChapter(ctx context.Context, chap story.Chapter) error {
	if chap == nil {
		return fmt.Errorf("chapterstore: chapter is required")
	}
	ordinal := chap.Ordinal()
	if ordinal < 0 {
		ordinal = h.ChapterCount()
	}
	record, err := h.store.chapterRecordFromStoryChapter(ctx, h.key, ordinal, chap)
	if err != nil {
		return err
	}
	rec, err := h.store.rows.AppendChapter(ctx, record, time.Now().UTC())
	if err != nil {
		return translateRowError(err)
	}
	h.setState(summaryFromRecord(rec))
	return nil
}

func (h *storyHandle) Chapter(ctx context.Context, ordinal int64) (story.Chapter, error) {
	return h.store.Chapter(ctx, h.key, ordinal)
}

func (h *storyHandle) GetLastChapter(ctx context.Context) (story.Chapter, error) {
	state := h.State()
	if state.ChapterCount == 0 {
		if err := h.Refresh(ctx); err != nil {
			return nil, err
		}
		state = h.State()
		if state.ChapterCount == 0 {
			return nil, core.ErrNotFound
		}
	}
	return h.Chapter(ctx, state.ChapterCount-1)
}

func (h *storyHandle) Chapters(ctx context.Context, opts story.ChaptersOptions) (pagination.Iterator[story.Chapter], error) {
	if err := h.Refresh(ctx); err != nil {
		return nil, err
	}
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	seed := int64(0)
	if opts.PageToken != "" {
		val, err := strconv.ParseInt(opts.PageToken, 10, 64)
		if err != nil || val < 0 {
			return nil, fmt.Errorf("chapterstore: invalid page token")
		}
		seed = val
	}
	fetch := func(ctx context.Context, token *string) ([]story.Chapter, *string, error) {
		start := seed
		if token != nil {
			val, err := strconv.ParseInt(*token, 10, 64)
			if err != nil || val < 0 {
				return nil, nil, fmt.Errorf("chapterstore: invalid page token")
			}
			start = val
		}
		state := h.State()
		var ordinals []int64
		for ord := start; ord <= state.LatestOrdinal && len(ordinals) < pageSize; ord++ {
			ordinals = append(ordinals, ord)
		}
		if opts.Direction == story.DirectionBackward {
			sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] > ordinals[j] })
		}
		items := make([]story.Chapter, 0, len(ordinals))
		for _, ordinal := range ordinals {
			ch, err := h.Chapter(ctx, ordinal)
			if err != nil {
				return nil, nil, err
			}
			items = append(items, ch)
		}
		if len(ordinals) == 0 || ordinals[len(ordinals)-1] >= state.LatestOrdinal {
			return items, nil, nil
		}
		next := strconv.FormatInt(ordinals[len(ordinals)-1]+1, 10)
		return items, &next, nil
	}
	return pagination.NewIterator(fetch), nil
}

func (h *storyHandle) ChapterCount() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state.ChapterCount
}

func (h *storyHandle) Finalize(ctx context.Context, _ story.FinalizeOptions) error {
	rec, err := h.store.rows.Publish(ctx, storageKey(h.key), time.Now().UTC())
	if err != nil {
		return translateRowError(err)
	}
	h.setState(summaryFromRecord(rec))
	return nil
}

func (h *storyHandle) setState(state story.Summary) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state = state
}

func storageKey(key story.Key) storage.StoryKey {
	return storage.StoryKey{AnthologyID: key.AnthologyID, StoryID: key.StoryID}
}

func storyKey(key storage.StoryKey) story.Key {
	return story.Key{AnthologyID: key.AnthologyID, StoryID: key.StoryID}
}

func summaryFromRecord(rec storage.StoryRecord) story.Summary {
	return story.Summary{
		Key:           storyKey(rec.Key),
		CreatedAt:     rec.CreatedAt,
		UpdatedAt:     rec.UpdatedAt,
		Finalized:     rec.Finalized,
		ChapterCount:  rec.ChapterCount,
		LatestOrdinal: rec.LatestOrdinal,
	}
}

func translateRowError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrStoryNotFound), errors.Is(err, storage.ErrChapterNotFound):
		return fmt.Errorf("%w: %v", core.ErrNotFound, err)
	case errors.Is(err, storage.ErrStoryExists), errors.Is(err, storage.ErrChapterExists), errors.Is(err, storage.ErrStoryFinalized), errors.Is(err, storage.ErrInvalidRecord):
		return fmt.Errorf("%w: %v", core.ErrConflict, err)
	default:
		return err
	}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

func descID(art artifact.Artifact) string {
	if art == nil {
		return ""
	}
	return art.ID()
}

func fallback(value, def string) string {
	if strings.TrimSpace(value) == "" {
		return def
	}
	return value
}
