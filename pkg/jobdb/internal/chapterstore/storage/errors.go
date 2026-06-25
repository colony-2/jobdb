package storage

import "errors"

var (
	// ErrStoryExists surfaces when attempting to create a duplicate story.
	ErrStoryExists = errors.New("chapterstore: story already exists")
	// ErrStoryNotFound indicates the requested story is absent.
	ErrStoryNotFound = errors.New("chapterstore: story not found")
	// ErrChapterExists surfaces when an ordinal already contains data.
	ErrChapterExists = errors.New("chapterstore: chapter already exists")
	// ErrChapterNotFound indicates the requested chapter is missing.
	ErrChapterNotFound = errors.New("chapterstore: chapter not found")
	// ErrStoryFinalized indicates the story no longer accepts mutations.
	ErrStoryFinalized = errors.New("chapterstore: story finalized")
	// ErrInvalidRecord indicates a storage mutation violates rowstore invariants.
	ErrInvalidRecord = errors.New("chapterstore: invalid record")
)
