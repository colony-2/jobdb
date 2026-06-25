package storage

import (
	"encoding/hex"
	"fmt"
)

// ValidateStoryRecord verifies metadata invariants that every rowstore must preserve.
func ValidateStoryRecord(rec StoryRecord) error {
	if rec.ChapterCount < 0 {
		return fmt.Errorf("%w: chapter count must be non-negative", ErrInvalidRecord)
	}
	if rec.LatestOrdinal < -1 {
		return fmt.Errorf("%w: latest ordinal must be at least -1", ErrInvalidRecord)
	}
	if rec.LatestOrdinal == -1 && rec.ChapterCount != 0 {
		return fmt.Errorf("%w: empty story chapter count must be zero", ErrInvalidRecord)
	}
	if rec.LatestOrdinal >= 0 && rec.ChapterCount != rec.LatestOrdinal+1 {
		return fmt.Errorf("%w: chapter count must match latest ordinal", ErrInvalidRecord)
	}
	if rec.Link != nil {
		if err := ValidateStoryLink(rec.Key, *rec.Link); err != nil {
			return err
		}
		if rec.LatestOrdinal < rec.Link.ForkOrdinal {
			return fmt.Errorf("%w: linked story latest ordinal must be at least fork ordinal", ErrInvalidRecord)
		}
	}
	return nil
}

// ValidateStoryLink verifies a linked clone base reference.
func ValidateStoryLink(key StoryKey, link StoryLink) error {
	if link.Key.AnthologyID == "" || link.Key.StoryID == "" {
		return fmt.Errorf("%w: linked story base key is required", ErrInvalidRecord)
	}
	if link.Key == key {
		return fmt.Errorf("%w: story cannot link to itself", ErrInvalidRecord)
	}
	if link.ForkOrdinal < 0 {
		return fmt.Errorf("%w: fork ordinal must be non-negative", ErrInvalidRecord)
	}
	return nil
}

// ValidateStoryTombstone verifies metadata before a store records a tombstone.
func ValidateStoryTombstone(rec StoryRecord) error {
	if !rec.Finalized {
		return fmt.Errorf("%w: story must be finalized before tombstone", ErrInvalidRecord)
	}
	if !rec.Deleted {
		return fmt.Errorf("%w: tombstone must mark story deleted", ErrInvalidRecord)
	}
	if rec.DeletedAt.IsZero() {
		return fmt.Errorf("%w: tombstone deleted timestamp is required", ErrInvalidRecord)
	}
	return ValidateStoryRecord(rec)
}

// ValidateChapterAppend verifies rowstore-level invariants before a chapter is persisted.
func ValidateChapterAppend(meta StoryRecord, rec ChapterRecord) error {
	if err := ValidateStoryRecord(meta); err != nil {
		return err
	}
	if rec.Key != meta.Key {
		return fmt.Errorf("%w: chapter key does not match story", ErrInvalidRecord)
	}
	if rec.Ordinal < 0 {
		return fmt.Errorf("%w: ordinal must be non-negative", ErrInvalidRecord)
	}
	if expected := meta.LatestOrdinal + 1; rec.Ordinal != expected {
		return fmt.Errorf("%w: ordinal must be %d", ErrInvalidRecord, expected)
	}
	if err := validateArtifacts(rec.Artifacts); err != nil {
		return err
	}
	return nil
}

func validateArtifacts(artifacts []ArtifactRecord) error {
	seen := make(map[string]struct{}, len(artifacts))
	for _, art := range artifacts {
		if art.Name == "" {
			return fmt.Errorf("%w: artifact name is required", ErrInvalidRecord)
		}
		if _, ok := seen[art.Name]; ok {
			return fmt.Errorf("%w: duplicate artifact name %q", ErrInvalidRecord, art.Name)
		}
		seen[art.Name] = struct{}{}

		if art.SizeBytes < 0 {
			return fmt.Errorf("%w: artifact %q size must be non-negative", ErrInvalidRecord, art.Name)
		}
		if err := validateArtifactSHA256(art); err != nil {
			return err
		}
		if art.InlineData != nil && art.BlobPath != "" {
			return fmt.Errorf("%w: artifact %q cannot be both inline and blob-backed", ErrInvalidRecord, art.Name)
		}
		if art.InlineData != nil && int64(len(art.InlineData)) != art.SizeBytes {
			return fmt.Errorf("%w: artifact %q inline size mismatch", ErrInvalidRecord, art.Name)
		}
		if art.SizeBytes > 0 && art.InlineData == nil && art.BlobPath == "" {
			return fmt.Errorf("%w: artifact %q missing blob path", ErrInvalidRecord, art.Name)
		}
	}
	return nil
}

func validateArtifactSHA256(art ArtifactRecord) error {
	if art.Sha256 == "" {
		return fmt.Errorf("%w: artifact %q sha256 is required", ErrInvalidRecord, art.Name)
	}
	if len(art.Sha256) != 64 {
		return fmt.Errorf("%w: artifact %q sha256 must be 64 lowercase hex characters", ErrInvalidRecord, art.Name)
	}
	if _, err := hex.DecodeString(art.Sha256); err != nil {
		return fmt.Errorf("%w: artifact %q sha256 must be lowercase hex", ErrInvalidRecord, art.Name)
	}
	for _, ch := range art.Sha256 {
		if ch >= 'A' && ch <= 'F' {
			return fmt.Errorf("%w: artifact %q sha256 must be lowercase hex", ErrInvalidRecord, art.Name)
		}
	}
	return nil
}
