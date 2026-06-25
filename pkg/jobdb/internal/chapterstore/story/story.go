package story

import (
	"context"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/pagination"
)

type Direction string

const (
	DirectionForward  Direction = "forward"
	DirectionBackward Direction = "backward"
)

type ChaptersOptions struct {
	PageSize  int
	PageToken string
	Direction Direction
}

type FinalizeOptions struct {
	Wait         bool
	PollInterval time.Duration
}

type CreateOptions struct {
	RequestID      string
	InitialChapter Chapter
}

type CloneOptions struct {
	DestinationKey Key
	LastOrdinal    int64
	CreateOptions  CreateOptions
}

type Summary struct {
	Key           Key
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Finalized     bool
	ChapterCount  int64
	LatestOrdinal int64
}

type Story interface {
	Key() Key
	State() Summary
	Refresh(ctx context.Context) error

	NewChapter() *ChapterBuilder
	AppendChapter(ctx context.Context, chapter Chapter) error

	Chapter(ctx context.Context, ordinal int64) (Chapter, error)
	GetLastChapter(ctx context.Context) (Chapter, error)
	Chapters(ctx context.Context, opts ChaptersOptions) (pagination.Iterator[Chapter], error)

	ChapterCount() int64
	Finalize(ctx context.Context, opts FinalizeOptions) error
}
