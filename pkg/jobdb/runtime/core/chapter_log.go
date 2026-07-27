package runtimecore

import (
	"context"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ChapterLogKey identifies the chapter log for one JobDB job.
type ChapterLogKey struct {
	JobKey jobdb.JobKey
}

// EncodedChapter is an opaque runtime-core chapter record.
//
// Payload is owned by runtime core. Backend implementations must persist and
// return it byte-for-byte, but must not interpret its format. ArtifactUploads
// is meaningful only on Create, ClonePrefix, and Append calls. Artifacts is
// meaningful on returned records.
type EncodedChapter struct {
	Ordinal         int64
	Payload         []byte
	ArtifactUploads []jobdb.ArtifactUpload
	Artifacts       []jobdb.StoredArtifact
}

// ChapterRange selects committed chapters in ascending ordinal order.
type ChapterRange struct {
	StartOrdinal int64
	EndOrdinal   *int64
}

// ClonePrefixRequest creates a destination log from an existing log prefix.
type ClonePrefixRequest struct {
	SourceKey      ChapterLogKey
	DestinationKey ChapterLogKey
	LastOrdinal    int64
	Append         *EncodedChapter
}

// ArtifactLookup identifies a committed artifact.
type ArtifactLookup struct {
	JobKey  jobdb.JobKey
	Ordinal int64
	Name    string
	Digest  string
}

// ChapterLog stores opaque chapter records and their artifacts.
type ChapterLog interface {
	Create(ctx context.Context, key ChapterLogKey, initial EncodedChapter) error
	ClonePrefix(ctx context.Context, req ClonePrefixRequest) error
	Append(ctx context.Context, key ChapterLogKey, chapter EncodedChapter) error
	Get(ctx context.Context, key ChapterLogKey, ordinal int64) (EncodedChapter, error)
	List(ctx context.Context, key ChapterLogKey, rng ChapterRange) ([]EncodedChapter, error)
	Count(ctx context.Context, key ChapterLogKey) (int64, error)
	OpenArtifact(ctx context.Context, lookup ArtifactLookup) (jobdb.ArtifactReader, error)
}
