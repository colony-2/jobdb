package runtimecore

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
	"unicode/utf8"

	j "github.com/colony-2/jobdb/pkg/jobdb"
	"google.golang.org/protobuf/encoding/protowire"
)

func (r *Runtime) ReadExecutionProvenance(ctx context.Context, req j.ReadExecutionProvenanceRequest) (j.ExecutionProvenance, error) {
	source, ok := r.chapters.(ProvenanceChapterSource)
	if !ok {
		return j.ExecutionProvenance{}, j.ErrExecutionProvenanceUnsupported
	}
	return ReadExecutionProvenance(ctx, req, func(ctx context.Context) error { return r.requirePublishedJob(ctx, req.JobKey) }, source)
}

// ReadExecutionProvenance shares the bounded reader with concrete runtimes whose
// authorization and native scheduler projections remain backend-owned.
func ReadExecutionProvenance(ctx context.Context, req j.ReadExecutionProvenanceRequest, authorize func(context.Context) error, source ProvenanceChapterSource) (j.ExecutionProvenance, error) {
	return readExecutionProvenance(ctx, req, authorize, source, j.DefaultExecutionProvenanceLimits())
}

type provenanceScan struct {
	ctx              context.Context
	out              j.ExecutionProvenance
	start            time.Time
	ordinal          int64
	stopped          bool
	seen             map[string]bool
	meaningful       bool
	responseEstimate int
	unvisitedUnknown bool
}

func readExecutionProvenance(ctx context.Context, req j.ReadExecutionProvenanceRequest, authorize func(context.Context) error, source ProvenanceChapterSource, limits j.ExecutionProvenanceLimits) (j.ExecutionProvenance, error) {
	start := time.Now()
	if req.JobKey.Validate() != nil || req.Consumer == "" || len(req.Consumer) > 128 || !utf8.ValidString(req.Consumer) {
		return j.ExecutionProvenance{}, j.ErrInvalidExecutionProvenanceRequest
	}
	if ctx == nil {
		ctx = context.Background()
	}
	budget := time.Duration(limits.ElapsedMillis) * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < budget {
		budget = time.Until(deadline)
		if budget < 0 {
			budget = 0
		}
		limits.ElapsedMillis = budget.Milliseconds()
	}
	ctx, cancel := context.WithDeadline(ctx, start.Add(budget))
	defer cancel()
	s := provenanceScan{ctx: ctx, start: start, seen: map[string]bool{}, out: j.ExecutionProvenance{Capability: j.ExecutionProvenanceCapability, Scope: j.ExecutionProvenanceScope, Consumer: req.Consumer, Status: j.ExecutionProvenanceUnavailable, Limits: limits, Pairs: []j.ExecutionProvenancePair{}, Issues: []j.ExecutionProvenanceIssue{}}}
	if err := authorize(ctx); err != nil {
		if errors.Is(err, j.ErrJobNotFound) || errors.Is(err, j.ErrLeasePermissionDenied) || errors.Is(err, j.ErrCellForbidden) {
			return j.ExecutionProvenance{}, err
		}
		if ctx.Err() != nil {
			s.issue(j.ExecutionProvenanceTimeLimit, nil)
		} else {
			s.issue(j.ExecutionProvenanceStorageUnavailable, nil)
		}
		return s.finish(), nil
	}
	if !s.timeOK() {
		return s.finish(), nil
	}
	ceiling, err := source.ExecutionProvenanceCeiling(ctx, ChapterLogKey{JobKey: req.JobKey})
	s.out.ThroughOrdinal = ceiling
	if err != nil {
		if ctx.Err() != nil {
			s.issue(j.ExecutionProvenanceTimeLimit, nil)
		} else {
			s.issue(j.ExecutionProvenanceStorageUnavailable, nil)
		}
		return s.finish(), nil
	}
	if ceiling == nil {
		s.issue(j.ExecutionProvenanceUnknownCeiling, nil)
		return s.finish(), nil
	}
	position := int64(0)
	contiguous := true
	for position <= *ceiling && !s.stopped {
		if !s.timeOK() {
			break
		}
		if s.out.Usage.Pages >= limits.Pages {
			s.stop(j.ExecutionProvenancePageLimit, &position)
			break
		}
		remaining := limits.Records - s.out.Usage.Records
		if remaining <= 0 {
			s.stop(j.ExecutionProvenanceRecordLimit, &position)
			break
		}
		remainingBytes := limits.ScannedBytes - s.out.Usage.ScannedBytes
		if remainingBytes <= 0 {
			s.stop(j.ExecutionProvenanceByteLimit, &position)
			break
		}
		pageRecords := min(limits.PageRecords, remaining)
		pageBytes := min(limits.PageBytes, remainingBytes)
		page, err := source.ReadProvenanceChapterPage(ctx, ProvenanceChapterPageRequest{Key: ChapterLogKey{JobKey: req.JobKey}, ThroughOrdinal: *ceiling, StartOrdinal: position, Records: pageRecords, Bytes: pageBytes})
		s.out.Usage.Pages++
		if err != nil {
			if ctx.Err() != nil {
				s.stop(j.ExecutionProvenanceTimeLimit, &position)
			} else {
				s.stop(j.ExecutionProvenanceStorageUnavailable, &position)
			}
			break
		}
		if len(page.Entries) > pageRecords || page.NextOrdinal <= position || page.NextOrdinal > *ceiling+1 {
			s.stop(j.ExecutionProvenanceStorageUnavailable, &position)
			break
		}
		for _, entry := range page.Entries {
			if s.stopped || !s.timeOK() {
				break
			}
			s.ordinal = entry.Ordinal
			if entry.Ordinal != position {
				s.issue(j.ExecutionProvenanceMissingRecord, &position)
				contiguous = false
				position = entry.Ordinal
			}
			if entry.Ordinal > *ceiling || entry.Ordinal < 0 {
				s.stop(j.ExecutionProvenanceStorageUnavailable, &position)
				break
			}
			if !s.record(true) {
				break
			}
			if entry.Issue != "" {
				// The body was not examined. Later readable entries must not
				// extend the contiguous physical prefix across this gap.
				contiguous = false
				if entry.Issue == j.ExecutionProvenanceByteLimit {
					s.stop(entry.Issue, &entry.Ordinal)
				} else {
					s.issue(entry.Issue, &entry.Ordinal)
				}
			} else if s.bytes(int64(len(entry.Body))) {
				s.chapter(entry.Body)
			}
			if s.stopped {
				break
			}
			if contiguous {
				n := entry.Ordinal
				s.out.ScannedThroughOrdinal = &n
			}
			position = entry.Ordinal + 1
		}
		if !s.stopped && position != page.NextOrdinal {
			s.issue(j.ExecutionProvenanceMissingRecord, &position)
			contiguous = false
			position = page.NextOrdinal
		}
	}
	if position <= *ceiling || s.unvisitedUnknown {
		s.out.UnexaminedRecordCount = nil
	} else {
		n := int64(0)
		s.out.UnexaminedRecordCount = &n
	}
	if !s.meaningful && !s.stopped {
		s.issue(j.ExecutionProvenanceMissingMetadata, nil)
	}
	if s.meaningful && (s.out.ScannedThroughOrdinal != nil || len(s.out.Pairs) > 0) {
		s.out.Status = j.ExecutionProvenancePartial
	}
	if s.meaningful && position > *ceiling && len(s.out.Issues) == 0 && s.out.OmittedIssueCount == 0 && !s.stopped {
		s.out.Status = j.ExecutionProvenanceComplete
	}
	return s.finish(), nil
}
func (s *provenanceScan) issue(reason j.ExecutionProvenanceIssueReason, ordinal *int64) {
	switch reason {
	case j.ExecutionProvenanceUnreadableRecord, j.ExecutionProvenanceUnsupportedEncoding, j.ExecutionProvenanceMissingRecord, j.ExecutionProvenanceUnknownCeiling, j.ExecutionProvenanceRetainedUnavailable, j.ExecutionProvenanceStorageUnavailable, j.ExecutionProvenanceOversizedRecord, j.ExecutionProvenanceDepthLimit:
		// Framing/source gaps can hide nested records, whose number is unknowable.
		s.unvisitedUnknown = true
	}

	if len(s.out.Issues) >= s.out.Limits.Issues {
		s.out.OmittedIssueCount++
		if !s.stopped {
			s.stopped = true
			if len(s.out.Issues) > 0 && s.out.Issues[len(s.out.Issues)-1].Reason != j.ExecutionProvenanceIssueLimit {
				s.out.Issues[len(s.out.Issues)-1] = j.ExecutionProvenanceIssue{Reason: j.ExecutionProvenanceIssueLimit}
				s.out.OmittedIssueCount++
			}
		}
		return
	}
	var copyOrdinal *int64
	if ordinal != nil {
		n := *ordinal
		copyOrdinal = &n
	}
	s.out.Issues = append(s.out.Issues, j.ExecutionProvenanceIssue{Reason: reason, Ordinal: copyOrdinal})
}
func (s *provenanceScan) stop(reason j.ExecutionProvenanceIssueReason, ordinal *int64) {
	if !s.stopped {
		s.issue(reason, ordinal)
		s.stopped = true
	}
}
func (s *provenanceScan) timeOK() bool {
	if s.ctx.Err() != nil {
		s.stop(j.ExecutionProvenanceTimeLimit, &s.ordinal)
		return false
	}
	return !s.stopped
}
func (s *provenanceScan) record(physical bool) bool {
	if s.out.Usage.Records >= s.out.Limits.Records {
		s.stop(j.ExecutionProvenanceRecordLimit, &s.ordinal)
		return false
	}
	s.out.Usage.Records++
	return true
}
func (s *provenanceScan) bytes(n int64) bool {
	if n > s.out.Limits.ScannedBytes-s.out.Usage.ScannedBytes {
		s.stop(j.ExecutionProvenanceByteLimit, &s.ordinal)
		return false
	}
	s.out.Usage.ScannedBytes += n
	return true
}
func (s *provenanceScan) finish() j.ExecutionProvenance {
	if s.out.Status != j.ExecutionProvenanceComplete && (len(s.out.Pairs) > 0 || s.out.ScannedThroughOrdinal != nil) {
		s.out.Status = j.ExecutionProvenancePartial
	}
	s.timeOK()
	if s.stopped && s.out.Status == j.ExecutionProvenanceComplete {
		s.out.Status = j.ExecutionProvenancePartial
	}
	sort.Slice(s.out.Pairs, func(a, b int) bool {
		x, y := s.out.Pairs[a], s.out.Pairs[b]
		if x.ConsumerVersion != y.ConsumerVersion {
			return x.ConsumerVersion < y.ConsumerVersion
		}
		if x.ConsumerCommit == nil || y.ConsumerCommit == nil {
			if x.ConsumerCommit != y.ConsumerCommit {
				return x.ConsumerCommit != nil
			}
		} else if *x.ConsumerCommit != *y.ConsumerCommit {
			return *x.ConsumerCommit < *y.ConsumerCommit
		}
		return x.JobDBVersion < y.JobDBVersion
	})
	s.out.Usage.ElapsedMillis = time.Since(s.start).Milliseconds()
	// Leave room for usage/truncation information. Exact responseBytes includes
	// its own decimal digits and excludes any encoder-added newline.
	for {
		for i := 0; i < 4; i++ {
			raw, _ := json.Marshal(s.out)
			if s.out.Usage.ResponseBytes == len(raw) {
				break
			}
			s.out.Usage.ResponseBytes = len(raw)
		}
		raw, _ := json.Marshal(s.out)
		if len(raw) <= s.out.Limits.ResponseBytes {
			break
		}
		s.out.Status = j.ExecutionProvenancePartial
		if len(s.out.Pairs) > 0 {
			s.out.Pairs = s.out.Pairs[:len(s.out.Pairs)-1]
			s.out.OmittedPairCount++
		} else if len(s.out.Issues) > 1 {
			s.out.Issues = s.out.Issues[:len(s.out.Issues)-1]
			s.out.OmittedIssueCount++
		} else {
			break
		}
		found := false
		for _, v := range s.out.Issues {
			found = found || v.Reason == j.ExecutionProvenanceResponseLimit
		}
		if !found {
			if len(s.out.Issues) >= s.out.Limits.Issues {
				s.out.Issues = s.out.Issues[:len(s.out.Issues)-1]
				s.out.OmittedIssueCount++
			}
			s.issue(j.ExecutionProvenanceResponseLimit, nil)
		}
	}
	if s.ctx.Err() != nil && !s.stopped {
		s.stop(j.ExecutionProvenanceTimeLimit, nil)
		if len(s.out.Pairs) > 0 || s.out.ScannedThroughOrdinal != nil {
			s.out.Status = j.ExecutionProvenancePartial
		} else {
			s.out.Status = j.ExecutionProvenanceUnavailable
		}
		s.out.Usage.ElapsedMillis = time.Since(s.start).Milliseconds()
		// Encoding is bounded by the response cap and reserves room for this issue.
		for i := 0; i < 4; i++ {
			raw, _ := json.Marshal(s.out)
			if len(raw) == s.out.Usage.ResponseBytes {
				break
			}
			s.out.Usage.ResponseBytes = len(raw)
		}
	}
	return s.out
}
func pairKey(p j.ExecutionProvenancePair) string { raw, _ := json.Marshal(p); return string(raw) }
func (s *provenanceScan) pair(consumer, commit, version string, hasCommit bool) {
	for _, v := range []string{consumer, commit, version} {
		if len(v) > s.out.Limits.IdentityBytes || !utf8.ValidString(v) {
			s.issue(j.ExecutionProvenanceIdentityLimit, &s.ordinal)
			return
		}
	}
	if consumer == "" || version == "" {
		s.issue(j.ExecutionProvenanceIncompleteIdentity, &s.ordinal)
		return
	}
	p := j.ExecutionProvenancePair{ConsumerVersion: consumer, JobDBVersion: version}
	if hasCommit {
		p.ConsumerCommit = &commit
	}
	key := pairKey(p)
	if s.seen[key] {
		return
	}
	if len(s.out.Pairs) >= s.out.Limits.Pairs {
		s.out.OmittedPairCount++
		s.stop(j.ExecutionProvenancePairLimit, &s.ordinal)
		return
	}
	if s.responseEstimate+len(key)+32768 > s.out.Limits.ResponseBytes && s.out.Limits.ResponseBytes >= 32768 {
		s.out.OmittedPairCount++
		s.stop(j.ExecutionProvenanceResponseLimit, &s.ordinal)
		return
	}
	s.responseEstimate += len(key)
	s.seen[key] = true
	s.out.Pairs = append(s.out.Pairs, p)
}

// fields visits only independently delimited wire fields. No protobuf object
// graph, application snapshots, or artifact descriptors are materialized.
func (s *provenanceScan) fields(raw []byte, visit func(protowire.Number, protowire.Type, []byte, uint64)) bool {
	for len(raw) > 0 && s.timeOK() {
		num, typ, n := protowire.ConsumeTag(raw)
		if typ == protowire.StartGroupType || typ == protowire.EndGroupType {
			s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
			return false
		}
		if n < 0 {
			s.issue(j.ExecutionProvenanceUnreadableRecord, &s.ordinal)
			return false
		}
		raw = raw[n:]
		var value []byte
		var integer uint64
		if typ == protowire.BytesType {
			value, n = protowire.ConsumeBytes(raw)
		} else if typ == protowire.VarintType {
			integer, n = protowire.ConsumeVarint(raw)
		} else {
			n = protowire.ConsumeFieldValue(num, typ, raw)
		}
		if n < 0 {
			s.issue(j.ExecutionProvenanceUnreadableRecord, &s.ordinal)
			return false
		}
		visit(num, typ, value, integer)
		raw = raw[n:]
	}
	return len(raw) == 0 && !s.stopped
}

func (s *provenanceScan) chapter(raw []byte) {
	format := uint64(0)
	hasChapter := false
	// Inspect the framing/version without constructing an object graph. Only the
	// recognized format permits interpreting its provenance-bearing field numbers.
	for rest := raw; len(rest) > 0 && s.timeOK(); {
		n, t, k := protowire.ConsumeTag(rest)
		if k < 0 || t == protowire.StartGroupType || t == protowire.EndGroupType {
			break
		}
		rest = rest[k:]
		if n == 20 && t == protowire.VarintType {
			v, k := protowire.ConsumeVarint(rest)
			if k < 0 {
				break
			}
			format = v
		}
		k = protowire.ConsumeFieldValue(n, t, rest)
		if k < 0 || t == protowire.StartGroupType || t == protowire.EndGroupType {
			break
		}
		rest = rest[k:]
	}
	if format != 2 {
		s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
		return
	}

	s.fields(raw, func(n protowire.Number, t protowire.Type, v []byte, i uint64) {
		switch n {
		case 20:
			format = i
		case 19:
			if t == protowire.BytesType {
				s.execution(v, 1, false)
			} else {
				s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
			}
		case 34:
			if t != protowire.BytesType {
				s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
				return
			}
			hasChapter = true
			s.fields(v, func(n protowire.Number, t protowire.Type, v []byte, _ uint64) {
				if n == 2 && t == protowire.BytesType {
					s.execution(v, 1, true)
				} else if n == 1 {
					s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
				}
			})
		case 30, 31, 32, 33, 35:
			hasChapter = true
		default:
			if n >= 36 && n < 50 {
				s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
			}
		}
	})
	if format != 2 || !hasChapter {
		s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
	}
}
func (s *provenanceScan) execution(raw []byte, depth int, physical bool) {
	if !s.timeOK() {
		return
	}
	if depth > s.out.Limits.NestingDepth {
		s.issue(j.ExecutionProvenanceDepthLimit, &s.ordinal)
		return
	}
	if !physical && (!s.record(false) || !s.bytes(int64(len(raw)))) {
		return
	}
	kind := ""
	hasVersions := false
	hasRetained := false
	s.fields(raw, func(n protowire.Number, t protowire.Type, v []byte, _ uint64) {
		if (n == 2 || n == 5 || n == 26) && t != protowire.BytesType {
			s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
			return
		}
		switch n {
		case 2:
			kind = string(v)
		case 5:
			hasVersions = true
			s.meaningful = true
			s.versions(v)
		case 26:
			hasRetained = true
			s.execution(v, depth+1, false)
		default:
			if n > 27 && n < 50 {
				s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
			}
		}
	})
	switch kind {
	case "header", "activation", "dependency_binding", "checkpoint", "dispatch", "join_result", "admission", "scope", "task", "attempt", "result", "catch", "handled", "decision", "end", "wait", "restart", "continuation", "sleep", "join":
	default:
		// Keep independently recognizable versions, but an unknown layout
		// cannot establish coverage of all provenance-bearing structures.
		s.issue(j.ExecutionProvenanceUnsupportedEncoding, &s.ordinal)
	}
	if (kind == "header" || kind == "activation") && !hasVersions {
		s.issue(j.ExecutionProvenanceMissingMetadata, &s.ordinal)
	}
	if kind == "restart" && !hasRetained {
		s.issue(j.ExecutionProvenanceRetainedUnavailable, &s.ordinal)
	}
}
func (s *provenanceScan) versions(raw []byte) {
	var consumer, commit, version string
	hasCommit, bad, malformed := false, false, false
	valid := s.fields(raw, func(n protowire.Number, t protowire.Type, v []byte, _ uint64) {
		if (n == 2 || n == 3) && t != protowire.BytesType {
			malformed = true
			return
		}
		if n == 2 {
			if len(v) > s.out.Limits.IdentityBytes {
				bad = true
			} else {
				version = string(v)
			}
		}
		if n == 3 {
			var k, value []byte
			if !s.fields(v, func(n protowire.Number, t protowire.Type, v []byte, _ uint64) {
				if (n == 1 || n == 2) && t != protowire.BytesType {
					malformed = true
					return
				}
				if n == 1 {
					k = v
				}
				if n == 2 {
					value = v
				}
			}) {
				malformed = true
			}
			if string(k) == s.out.Consumer || string(k) == s.out.Consumer+".commit" {
				if len(value) > s.out.Limits.IdentityBytes {
					bad = true
					return
				}
				if string(k) == s.out.Consumer {
					consumer = string(value)
				} else {
					commit = string(value)
					hasCommit = true
				}
			}
		}
	})
	if !valid {
		return
	}
	if malformed {
		s.issue(j.ExecutionProvenanceUnreadableRecord, &s.ordinal)
		return
	}
	if bad {
		s.issue(j.ExecutionProvenanceIdentityLimit, &s.ordinal)
		return
	}
	s.pair(consumer, commit, version, hasCommit)
}
