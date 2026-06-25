package storage

import (
	"context"
	"io"
	"time"
)

// RowStore persists story metadata, chapter bodies, and artifact descriptors.
type RowStore interface {
	Health(ctx context.Context) error

	CreateStory(ctx context.Context, key StoryKey, now time.Time) (StoryRecord, error)
	CreateLinkedStory(ctx context.Context, key StoryKey, link StoryLink, now time.Time) (StoryRecord, error)
	GetStory(ctx context.Context, key StoryKey) (StoryRecord, error)
	GetStoryIncludingDeleted(ctx context.Context, key StoryKey) (StoryRecord, error)
	Publish(ctx context.Context, key StoryKey, now time.Time) (StoryRecord, error)
	TombstoneStory(ctx context.Context, key StoryKey, now time.Time) (StoryRecord, error)

	AppendChapter(ctx context.Context, rec ChapterRecord, now time.Time) (StoryRecord, error)
	ReadChapter(ctx context.Context, key StoryKey, ordinal int64) (ChapterRecord, error)
	ReadChapterIncludingDeleted(ctx context.Context, key StoryKey, ordinal int64) (ChapterRecord, error)
	ListChapterOrdinals(ctx context.Context, key StoryKey, startOrdinal int64, limit int) ([]int64, bool, error)
	ListStories(ctx context.Context, anthologyID, startAfter string, limit int) ([]StoryRecord, bool, error)
}

// BlobStore stores large artifact bytes outside the rowstore.
type BlobStore interface {
	Save(ctx context.Context, body io.Reader) (string, error)
	Open(blobPath string) (io.ReadCloser, error)
	Delete(blobPath string) error
}
