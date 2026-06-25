package storage

import (
	"encoding/json"
	"time"
)

// StoryKey uniquely identifies a story within an anthology.
type StoryKey struct {
	AnthologyID string
	StoryID     string
}

// StoryRecord persists story-level metadata.
type StoryRecord struct {
	Key           StoryKey
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Finalized     bool
	Deleted       bool
	DeletedAt     time.Time
	Link          *StoryLink
	ChapterCount  int64
	LatestOrdinal int64
}

// StoryLink points to the base story and logical fork ordinal for a linked clone.
type StoryLink struct {
	Key         StoryKey
	ForkOrdinal int64
}

// ChapterRecord captures a committed chapter payload and its artifacts.
type ChapterRecord struct {
	Key       StoryKey
	Ordinal   int64
	Body      json.RawMessage
	Artifacts []ArtifactRecord
	CreatedAt time.Time
}

// ArtifactRecord retains metadata required to materialize attachments.
type ArtifactRecord struct {
	ID          string
	Name        string
	ContentType string
	SizeBytes   int64
	Sha256      string

	InlineData []byte
	BlobPath   string
}

// LatestOrdinalKnown reports whether a story has at least one chapter.
func (s StoryRecord) LatestOrdinalKnown() bool {
	return s.LatestOrdinal >= 0
}

// Linked reports whether the story inherits pre-fork chapters from another story.
func (s StoryRecord) Linked() bool {
	return s.Link != nil
}

// StoryKeyFromParts normalizes identifier components.
func StoryKeyFromParts(anthologyID, storyID string) StoryKey {
	return StoryKey{
		AnthologyID: anthologyID,
		StoryID:     storyID,
	}
}
