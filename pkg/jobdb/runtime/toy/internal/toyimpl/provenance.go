package toyimpl

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

// Cache the committed wire representation at write time. Diagnostic reads do
// not re-encode application data or invoke strict execution conversion.
func (e *ToyEngine) recordProvenanceLocked(key jobdb.JobKey, ch jobdb.Chapter) {
	if e.runtimeProvenanceCeilings == nil {
		e.runtimeProvenanceCeilings = map[jobdb.JobKey]int64{}
	}
	if previous, ok := e.runtimeProvenanceCeilings[key]; !ok || ch.Ordinal > previous {
		e.runtimeProvenanceCeilings[key] = ch.Ordinal
	}
	if e.runtimeProvenance == nil {
		e.runtimeProvenance = map[jobdb.JobKey]map[int64][]byte{}
	}
	if e.runtimeProvenance[key] == nil {
		e.runtimeProvenance[key] = map[int64][]byte{}
	}
	encoded, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: ch})
	if err != nil {
		e.runtimeProvenance[key][ch.Ordinal] = nil
	} else {
		e.runtimeProvenance[key][ch.Ordinal] = encoded.Payload
	}
}

type provenanceSource struct{ r *Runtime }

func (r *Runtime) ReadExecutionProvenance(ctx context.Context, q jobdb.ReadExecutionProvenanceRequest) (jobdb.ExecutionProvenance, error) {
	return runtimecore.ReadExecutionProvenance(ctx, q, func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec := r.engine.getJobRecord(q.JobKey)
		if rec == nil {
			return jobdb.ErrJobNotFound
		}
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if rec.hidden {
			return jobdb.ErrJobNotFound
		}
		return nil
	}, provenanceSource{r})
}
func (s provenanceSource) ExecutionProvenanceCeiling(ctx context.Context, key runtimecore.ChapterLogKey) (*int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.r.engine.mu.Lock()
	defer s.r.engine.mu.Unlock()
	v, ok := s.r.engine.runtimeProvenanceCeilings[key.JobKey]
	if !ok {
		return nil, nil
	}
	return &v, nil
}
func (s provenanceSource) ReadProvenanceChapterPage(ctx context.Context, q runtimecore.ProvenanceChapterPageRequest) (runtimecore.ProvenanceChapterPage, error) {
	s.r.engine.mu.Lock()
	defer s.r.engine.mu.Unlock()
	out := runtimecore.ProvenanceChapterPage{NextOrdinal: q.StartOrdinal}
	remaining := min(q.Bytes, int64(1<<20))
	maxRecords := min(q.Records, 256)
	for n := q.StartOrdinal; n <= q.ThroughOrdinal && len(out.Entries) < maxRecords; n++ {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		entry := runtimecore.ProvenanceChapterEntry{Ordinal: n}
		body, ok := s.r.engine.runtimeProvenance[q.Key.JobKey][n]
		if _, exists := s.r.engine.runtimeChapters[q.Key.JobKey][n]; !exists {
			entry.Issue = jobdb.ExecutionProvenanceMissingRecord
		} else if !ok || body == nil {
			entry.Issue = jobdb.ExecutionProvenanceUnreadableRecord
		} else if int64(len(body)) > remaining {
			if len(body) <= 1<<20 && len(out.Entries) > 0 {
				return out, nil
			}
			if len(body) > 1<<20 {
				entry.Issue = jobdb.ExecutionProvenanceOversizedRecord
			} else {
				entry.Issue = jobdb.ExecutionProvenanceByteLimit
			}
		} else {
			entry.Body = append([]byte(nil), body...)
			remaining -= int64(len(body))
		}
		out.Entries = append(out.Entries, entry)
		out.NextOrdinal = n + 1
	}
	return out, nil
}
