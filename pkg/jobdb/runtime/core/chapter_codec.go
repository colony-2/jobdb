package runtimecore

import (
	"encoding/json"
	"fmt"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

// EncodeChapterRequest describes a typed chapter to encode for a ChapterLog.
type EncodeChapterRequest struct {
	Chapter         jobdb.Chapter
	ArtifactUploads []jobdb.ArtifactUpload
}

// EncodeChapter converts a typed public chapter into an opaque chapter-log
// record.
func EncodeChapter(req EncodeChapterRequest) (EncodedChapter, error) {
	chapter := req.Chapter
	meta := runtimecodec.ChapterMeta{
		Version:   runtimecodec.EnvelopeVersion,
		Ordinal:   chapter.Ordinal,
		TaskType:  chapter.TaskType,
		CreatedAt: chapter.CreatedAt,
		InputHash: chapter.InputHash,
	}
	rawMetadata, err := runtimecodec.ChapterMetadataToJSON(chapter.Metadata)
	if err != nil {
		return EncodedChapter{}, fmt.Errorf("encode chapter metadata: %w", err)
	}
	if len(rawMetadata) > 0 {
		if err := json.Unmarshal(rawMetadata, &meta); err != nil {
			return EncodedChapter{}, fmt.Errorf("decode chapter metadata: %w", err)
		}
		if meta.Ordinal == 0 {
			meta.Ordinal = chapter.Ordinal
		}
		if meta.TaskType == "" {
			meta.TaskType = chapter.TaskType
		}
		if meta.CreatedAt.IsZero() {
			meta.CreatedAt = chapter.CreatedAt
		}
		if meta.InputHash == "" {
			meta.InputHash = chapter.InputHash
		}
		if meta.Version == 0 {
			meta.Version = runtimecodec.EnvelopeVersion
		}
	}
	chapterType, payloadKind, payload, err := runtimecodec.ChapterBodyToWire(chapter.Body)
	if err != nil {
		return EncodedChapter{}, err
	}
	encoded, err := runtimecodec.EncodeChapter(meta, chapterType, payloadKind, payload)
	if err != nil {
		return EncodedChapter{}, err
	}
	return EncodedChapter{
		Ordinal:         chapter.Ordinal,
		Payload:         cloneBytes(encoded),
		ArtifactUploads: cloneArtifactUploads(req.ArtifactUploads),
		Artifacts:       cloneStoredArtifacts(chapter.Artifacts),
	}, nil
}

// DecodeChapter converts an opaque chapter-log record into a typed public
// chapter.
func DecodeChapter(encoded EncodedChapter) (jobdb.Chapter, error) {
	env, err := runtimecodec.DecodeChapter(encoded.Payload)
	if err != nil {
		return jobdb.Chapter{}, err
	}
	rawMetadata, err := json.Marshal(env.Meta)
	if err != nil {
		return jobdb.Chapter{}, fmt.Errorf("encode chapter metadata: %w", err)
	}
	metadata, err := runtimecodec.ChapterMetadataFromJSON(rawMetadata)
	if err != nil {
		return jobdb.Chapter{}, fmt.Errorf("decode chapter metadata: %w", err)
	}
	body, err := runtimecodec.ChapterBodyFromWire(env.ChapterType, env.PayloadKind, env.Payload)
	if err != nil {
		return jobdb.Chapter{}, err
	}
	return jobdb.Chapter{
		Ordinal:   encoded.Ordinal,
		TaskType:  env.Meta.TaskType,
		Body:      body,
		InputHash: env.Meta.InputHash,
		CreatedAt: env.Meta.CreatedAt,
		Metadata:  metadata,
		Artifacts: cloneStoredArtifacts(encoded.Artifacts),
	}, nil
}

func cloneBytes(raw []byte) []byte {
	if raw == nil {
		return nil
	}
	return append([]byte(nil), raw...)
}

func cloneArtifactUploads(in []jobdb.ArtifactUpload) []jobdb.ArtifactUpload {
	if len(in) == 0 {
		return nil
	}
	return append([]jobdb.ArtifactUpload(nil), in...)
}

func cloneStoredArtifacts(in []jobdb.StoredArtifact) []jobdb.StoredArtifact {
	if len(in) == 0 {
		return nil
	}
	return append([]jobdb.StoredArtifact(nil), in...)
}
