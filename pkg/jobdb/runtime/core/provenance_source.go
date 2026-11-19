package runtimecore

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ProvenanceChapterSource is optional. It must determine a physical committed
// ceiling without decoding bodies, bound reads before materialization, resolve
// inherited positions and never enumerate or read artifacts.
type ProvenanceChapterSource interface {
	ExecutionProvenanceCeiling(context.Context, ChapterLogKey) (*int64, error)
	ReadProvenanceChapterPage(context.Context, ProvenanceChapterPageRequest) (ProvenanceChapterPage, error)
}
type ProvenanceChapterPageRequest struct {
	Key            ChapterLogKey
	ThroughOrdinal int64
	StartOrdinal   int64
	Records        int
	Bytes          int64
}
type ProvenanceChapterEntry struct {
	Ordinal int64
	Body    []byte
	Issue   jobdb.ExecutionProvenanceIssueReason
}
type ProvenanceChapterPage struct {
	Entries     []ProvenanceChapterEntry
	NextOrdinal int64
}
