package chapterstore

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
)

// StagedClone holds a linked clone and an optional pre-encoded new chapter.
// Inherited blobs are never copied or added to the discard set.
type StagedClone struct {
	store   Store
	source  story.Key
	opts    story.CloneOptions
	initial *storage.ChapterRecord
	blobs   *stagedBlobs
}

// StageClone reads new artifacts before borrowing the SQLite writer, since an
// artifact reader can itself need the database. It writes no destination rows.
// The caller must Discard on publication failure, including commit failure.
func (s *Store) StageClone(ctx context.Context, source story.Key, opts story.CloneOptions) (*StagedClone, error) {
	blobs := &stagedBlobs{BlobStore: s.blobs}
	staged := &StagedClone{store: *s, source: source, opts: opts, blobs: blobs}
	staged.store.blobs = blobs
	if chap := opts.CreateOptions.InitialChapter; chap != nil {
		dest := opts.DestinationKey
		if strings.TrimSpace(dest.AnthologyID) == "" {
			dest.AnthologyID = source.AnthologyID
		}
		record, err := staged.store.chapterRecordFromStoryChapter(ctx, dest, chap.Ordinal(), chap)
		if err != nil {
			return nil, errors.Join(err, staged.Discard())
		}
		staged.initial = &record
		staged.opts.CreateOptions.InitialChapter = nil
	}
	return staged, nil
}

// Publish borrows transactional rows; the caller owns commit and rollback.
func (s *StagedClone) Publish(ctx context.Context, rows storage.RowStore) error {
	store := s.store
	store.rows = rows
	clone, err := store.CloneStory(ctx, s.source, s.opts)
	if err != nil {
		return err
	}
	if s.initial != nil {
		record := *s.initial
		if record.Ordinal < 0 {
			record.Ordinal = clone.ChapterCount()
		}
		_, err := rows.AppendChapter(ctx, record, time.Now().UTC())
		return translateRowError(err)
	}
	return nil
}

// Discard removes only blobs saved while preparing this clone.
func (s *StagedClone) Discard() error {
	return s.blobs.discard()
}

type stagedBlobs struct {
	storage.BlobStore
	paths []string
}

func (b *stagedBlobs) Save(ctx context.Context, body io.Reader) (string, error) {
	path, err := b.BlobStore.Save(ctx, body)
	if err == nil {
		b.paths = append(b.paths, path)
	}
	return path, err
}

func (b *stagedBlobs) discard() error {
	var errs []error
	for _, path := range b.paths {
		errs = append(errs, b.BlobStore.Delete(path))
	}
	return errors.Join(errs...)
}
