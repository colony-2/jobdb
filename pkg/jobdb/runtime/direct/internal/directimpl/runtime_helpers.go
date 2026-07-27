package directimpl

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

// StoryKeyForJob exposes the direct-runtime story-key mapping.
func StoryKeyForJob(jobKey jobdb.JobKey) story.Key {
	return storyKeyForJob(jobKey)
}

// EncodeChapter converts the backend-agnostic chapter representation into
// the on-disk chapter envelope used by the direct runtime.
func EncodeChapter(chapter jobdb.Chapter) ([]byte, error) {
	encoded, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: chapter})
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), encoded.Payload...), nil
}

// ChapterFromStoryChapter converts a direct-runtime chapter into the
// backend-agnostic representation.
func ChapterFromStoryChapter(chapter story.Chapter) (jobdb.Chapter, error) {
	artifacts := make([]jobdb.StoredArtifact, 0, len(chapter.Artifacts()))
	for _, art := range chapter.Artifacts() {
		if art == nil {
			continue
		}
		digest, _ := art.Sha256(context.Background())
		artifacts = append(artifacts, jobdb.StoredArtifact{
			Name:   art.Name(),
			Digest: digest,
			Size:   art.SizeBytes(),
		})
	}
	return runtimecore.DecodeChapter(runtimecore.EncodedChapter{
		Ordinal:   chapter.Ordinal(),
		Payload:   append([]byte(nil), chapter.Body()...),
		Artifacts: artifacts,
	})
}
