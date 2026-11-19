package chapterstore

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

type provenanceRows interface {
	ReadProvenanceBody(context.Context, storage.StoryKey, int64, int64) ([]byte, int64, bool, error)
}

func (s *Store) ExecutionProvenanceCeiling(ctx context.Context, key runtimecore.ChapterLogKey) (*int64, error) {
	row, err := s.rows.GetStory(ctx, storage.StoryKey{AnthologyID: key.JobKey.TenantId, StoryID: key.JobKey.JobId})
	if err != nil {
		return nil, err
	}
	if row.LatestOrdinal < 0 {
		return nil, nil
	}
	n := row.LatestOrdinal
	return &n, nil
}
func (s *Store) ReadProvenanceChapterPage(ctx context.Context, q runtimecore.ProvenanceChapterPageRequest) (runtimecore.ProvenanceChapterPage, error) {
	out := runtimecore.ProvenanceChapterPage{NextOrdinal: q.StartOrdinal}
	rows, ok := s.rows.(provenanceRows)
	if !ok {
		return out, jobdb.ErrExecutionProvenanceUnsupported
	}
	q.Records = min(q.Records, 256)
	q.Bytes = min(q.Bytes, 1<<20)
	remaining := q.Bytes
	for n := q.StartOrdinal; n <= q.ThroughOrdinal && len(out.Entries) < q.Records; n++ {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		key := storage.StoryKey{AnthologyID: q.Key.JobKey.TenantId, StoryID: q.Key.JobKey.JobId}
		entry := runtimecore.ProvenanceChapterEntry{Ordinal: n}
		visited := map[storage.StoryKey]bool{}
		for depth := 0; ; depth++ {
			if depth >= 64 || visited[key] {
				entry.Issue = jobdb.ExecutionProvenanceDepthLimit
				break
			}
			visited[key] = true
			meta, err := s.rows.GetStoryIncludingDeleted(ctx, key)
			if err != nil {
				entry.Issue = jobdb.ExecutionProvenanceRetainedUnavailable
				break
			}
			if meta.Link != nil && n <= meta.Link.ForkOrdinal {
				key = meta.Link.Key
				continue
			}
			body, size, found, err := rows.ReadProvenanceBody(ctx, key, n, remaining)
			if err != nil {
				return out, err
			}
			if !found {
				entry.Issue = jobdb.ExecutionProvenanceMissingRecord
			} else if size > remaining {
				// Normal page exhaustion retries this ordinal in a fresh page. A body
				// exceeding the fixed page maximum is skipped without loading it.
				if size <= 1<<20 && len(out.Entries) > 0 {
					out.NextOrdinal = n
					return out, nil
				}
				if size > 1<<20 {
					entry.Issue = jobdb.ExecutionProvenanceOversizedRecord
				} else {
					entry.Issue = jobdb.ExecutionProvenanceByteLimit
				}
			} else {
				entry.Body = body
				remaining -= int64(len(body))
			}
			break
		}
		out.Entries = append(out.Entries, entry)
		out.NextOrdinal = n + 1
	}
	return out, nil
}
