package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// Catch history states are open strings: consumers must tolerate future values.
const (
	CatchBoundaryScope           = "scope"
	CatchBoundaryTask            = "task"
	CatchSelectionSelected       = "selected"
	CatchSelectionUnmatched      = "unmatched"
	CatchSelectionPredicateError = "predicate_error"
	CatchSelectionUnknown        = "unknown"
	CatchHandlingAccepted        = "accepted"
	CatchHandlingPending         = "pending"
	CatchHandlingNotApplicable   = "not_applicable"
	CatchHandlingUnknown         = "unknown"
	CatchProvenanceHandlerError  = "handler_error"
	CatchProvenanceReplacement   = "replacement"
	CatchProvenancePreserved     = "preserved"
	CatchProvenanceUnknown       = "unknown"
	CatchGapOriginal             = "original"
	CatchGapOriginalOrigin       = "original.origin"
	CatchGapAcceptedProvenance   = "accepted.provenance"
	CatchGapSelection            = "selection"
	CatchGapHandling             = "handling"
	CatchGapRetry                = "retry"
	CatchGapParent               = "parent"
	CatchGapNotRecorded          = "not_recorded"
	CatchGapFrontier             = "frontier"
	CatchGapAmbiguous            = "ambiguous"
	CatchGapOmitted              = "omitted"
)

// CatchHistory describes evidenced failed boundary attempts, not final job status
// or every declared catch rule. Empty Attempts does not prove no application
// exception occurred. Entries are ordered by their first durable evidence.
type CatchHistory struct {
	Attempts []CatchAttempt
	Gaps     []CatchEvidenceGap
}

// CatchBoundaryRef contains opaque equality-only identities. Never parse them.
// BoundaryID identifies a visit; AttemptID includes all enclosing attempts.
// A copied restart prefix belongs to the requested JobKey, not its source run.
type CatchBoundaryRef struct {
	JobKey        jobdb.JobKey
	JobAttemptID  string
	BoundaryID    string
	AttemptID     string
	AttemptNumber int
}

// CatchAttempt separates selection, accepted handling and retry. Pending means
// acceptance is not recorded; it proves neither handler start nor worker liveness.
// ParentID references the enclosing boundary invocation (empty for the root).
type CatchAttempt struct {
	Boundary       CatchBoundaryRef
	BoundaryKind   string
	ParentID       string
	Site           string
	Original       *CatchFailure
	Selection      string
	RuleID         string
	PredicateError *CatchFailure
	Handling       string
	Accepted       *CatchAcceptedResult
	Retry          *CatchRetryDecision
	Gaps           []CatchEvidenceGap
}

// CatchAcceptedResult is an engine-validated, committed handled result. Success
// requires CatchHandled AND Failure == nil. CatchContinue continues engine
// failure/retry processing; it is not a consumer's recipe continue action.
// Provenance is unknown when history cannot distinguish handler error,
// intentional replacement and preservation, even if their payloads match.
type CatchAcceptedResult struct {
	Disposition CatchDisposition
	Output      *jobdb.ExecutionSnapshot
	Failure     *CatchFailure
	Provenance  string
}

// CatchFailure preserves the recorded structured failure without reconstructing
// an application Go error or guessing a causal originating child.
type CatchFailure struct {
	Kind string
	Data json.RawMessage
}

// CatchRetryDecision is present only when recorded. Retry=false is known false;
// a nil decision is unknown. WakeAt is set only for a recorded retry wake time.
type CatchRetryDecision struct {
	Retry  bool
	WakeAt *time.Time
}

// CatchEvidenceGap marks unavailable evidence, independently of proven facts.
type CatchEvidenceGap struct {
	Field  string
	Reason string
}

// ReadCatchHistory projects a coherent durable prefix through existing runtime
// reads, without executing predicates, handlers, workers or replay orchestration.
// The same prefix has the same projection for active and historical inspection.
// req.JobKey selects the run; inclusion flags do not limit required evidence.
// Returned buffers are owned by the caller. Reads do not mutate history.
//
// Only supported v4 execution histories are interpreted. Corruption, inconsistent
// association and missing required identity return errors; unavailable historical
// facts produce gaps. A separately read job summary never establishes acceptance.
func ReadCatchHistory(ctx context.Context, runtime jobdb.WorkflowRuntime, req jobdb.GetJobRunRequest) (CatchHistory, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := req.JobKey.Validate(); err != nil {
		return CatchHistory{}, err
	}
	if runtime == nil {
		return CatchHistory{}, fmt.Errorf("catch history: nil runtime")
	}
	req.IncludeInputs, req.IncludeOutputs, req.IncludeArtifacts, req.IncludeAttemptInputs = true, true, true, true
	info, err := jobdb.GetJobRun(ctx, catchPrefixRuntime{runtime}, req)
	if err != nil {
		return CatchHistory{}, fmt.Errorf("catch history: %w", err)
	}
	h, err := projectCatchHistory(req.JobKey, info.Execution)
	if err != nil {
		return CatchHistory{}, fmt.Errorf("catch history: %w", err)
	}
	return h, nil
}

// GetJobRun obtains chapters exactly once. Validate that snapshot before it can
// be normalized into logical records; equal ordinals are legal only after
// reconstruction, never in the physical chapter list.
type catchPrefixRuntime struct{ jobdb.WorkflowRuntime }

func (r catchPrefixRuntime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	chapters, err := r.WorkflowRuntime.ListChapters(ctx, req)
	if err != nil {
		return nil, err
	}
	ordered := append([]jobdb.Chapter(nil), chapters...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Ordinal < ordered[j].Ordinal })
	for i, ch := range ordered {
		if ch.Ordinal != int64(i) {
			return nil, fmt.Errorf("inconsistent chapter prefix: expected ordinal %d, got %d", i, ch.Ordinal)
		}
	}
	if len(ordered) == 0 {
		return nil, fmt.Errorf("missing job-start chapter")
	}
	return ordered, nil
}

type catchAttemptEvidence struct {
	row                                  CatchAttempt
	result, selection, handled, decision *jobdb.ExecutionRecord
}

// These are internal v4 engine grammar readers, not a public identity format.
// JSON-quoted sites may themselves contain slashes, quotes and engine-like text.
var catchBoundarySuffix = regexp.MustCompile(`/(scope|task):("(?:[^"\\]|\\.)*")#([1-9][0-9]*)$`)

func catchSplitAttempt(id string) (string, int, error) {
	pos := strings.LastIndex(id, "/attempt:")
	if pos < 0 {
		return "", 0, fmt.Errorf("missing attempt identity %q", id)
	}
	n, err := strconv.Atoi(id[pos+len("/attempt:"):])
	if err != nil || n < 1 || strconv.Itoa(n) != id[pos+len("/attempt:"):] {
		return "", 0, fmt.Errorf("invalid attempt identity %q", id)
	}
	return id[:pos], n, nil
}

func catchBoundaryParts(id string) (parent, kind, site string, err error) {
	loc := catchBoundarySuffix.FindStringSubmatchIndex(id)
	if loc == nil {
		return "", "", "", fmt.Errorf("invalid boundary identity %q", id)
	}
	parent = id[:loc[0]]
	kind = id[loc[2]:loc[3]]
	if err = json.Unmarshal([]byte(id[loc[4]:loc[5]]), &site); err != nil {
		return "", "", "", err
	}
	if site == "" {
		return "", "", "", fmt.Errorf("empty boundary site")
	}
	return parent, kind, site, nil
}

// Resolve ancestry from exact occurrence paths, including omitted organizational
// scopes. Handler contexts use exact recorded selections or unambiguous declared rules,
// never splitting user-supplied rule IDs (which can contain arbitrary slashes).
func catchReference(key jobdb.JobKey, id string, handlerOwners map[string]string, seen map[string]bool) (CatchBoundaryRef, string, string, string, error) {
	boundary, n, err := catchSplitAttempt(id)
	if err != nil {
		return CatchBoundaryRef{}, "", "", "", err
	}
	parent, kind, site, err := catchBoundaryParts(boundary)
	if err != nil {
		return CatchBoundaryRef{}, "", "", "", err
	}
	ref := CatchBoundaryRef{JobKey: key, BoundaryID: boundary, AttemptID: id, AttemptNumber: n}
	if parent == "" {
		if kind != CatchBoundaryScope || site != "$job" {
			return ref, "", "", "", fmt.Errorf("missing whole-job ancestry for %q", id)
		}
		// Compact histories have already been expanded into logical records.
		// Unlike organizational scopes, the whole-job attempt must be evidenced.
		if !seen["attempt\x00"+id] {
			return ref, "", "", "", fmt.Errorf("missing recorded whole-job attempt %q", id)
		}
		ref.JobAttemptID = id
		return ref, kind, site, "", nil
	}
	// Exact context matches retain punctuation in rule IDs. A missing selection
	// may still have an unambiguous owner through recorded declarations; this
	// establishes ancestry only, never selection or acceptance.
	contextID := parent
	if owner, ok := handlerOwners[parent]; ok {
		if owner == "" {
			return ref, "", "", "", fmt.Errorf("ambiguous handler ancestry for %q", id)
		}
		contextID = owner
	}
	ancestor, _, _, _, err := catchReference(key, contextID, handlerOwners, seen)
	if err != nil {
		return ref, "", "", "", err
	}
	ref.JobAttemptID = ancestor.JobAttemptID
	return ref, kind, site, ancestor.BoundaryID, nil
}

func projectCatchHistory(key jobdb.JobKey, entries []jobdb.ExecutionHistoryEntry) (CatchHistory, error) {
	history := CatchHistory{Attempts: []CatchAttempt{}}
	declarations := map[string]executionPolicy{}
	selections := map[string]string{}
	evidence := map[string]*catchAttemptEvidence{}
	var order []string
	seen := map[string]bool{}
	header := false
	// A first pass binds handler ancestry independently of missing optional origin
	// evidence, while rejecting contradictory duplicate control records.
	for _, entry := range entries {
		rec := entry.Record
		if rec.Version != jobdb.ExecutionPatternVersion {
			return history, fmt.Errorf("unsupported execution version %d", rec.Version)
		}
		if !header {
			if rec.Kind != "header" {
				return history, fmt.Errorf("execution header missing")
			}
			header = true
		}
		switch rec.Kind {
		case "header":
			if seen["header"] {
				return history, fmt.Errorf("duplicate execution header")
			}
			seen["header"] = true
		case "scope", "task":
			if _, ok := declarations[rec.ID]; ok {
				return history, fmt.Errorf("duplicate boundary %q", rec.ID)
			}
			_, kind, site, err := catchBoundaryParts(rec.ID)
			if err != nil {
				return history, err
			}
			if kind != rec.Kind {
				return history, fmt.Errorf("boundary kind mismatch %q", rec.ID)
			}
			var policy executionPolicy
			if rec.Kind == "task" {
				var descriptor executionTaskDescriptor
				if err := json.Unmarshal(rec.Descriptor, &descriptor); err != nil {
					return history, fmt.Errorf("invalid task policy for %q: %w", rec.ID, err)
				}
				if descriptor.Call.Site != site {
					return history, fmt.Errorf("task site mismatch for %q", rec.ID)
				}
				policy = descriptor.Policy
			} else if err := json.Unmarshal(rec.Descriptor, &policy); err != nil {
				return history, fmt.Errorf("invalid scope policy for %q: %w", rec.ID, err)
			}
			rules := map[string]bool{}
			for _, rule := range policy.Rules {
				if rule == "" || rules[rule] {
					return history, fmt.Errorf("invalid rule declaration for %q", rec.ID)
				}
				rules[rule] = true
			}
			declarations[rec.ID] = policy
		case "attempt", "result", "catch", "handled", "decision":
			k := rec.Kind + "\x00" + rec.ID
			if seen[k] {
				return history, fmt.Errorf("duplicate %s for %q", rec.Kind, rec.ID)
			}
			seen[k] = true
			if _, n, err := catchSplitAttempt(rec.ID); err != nil {
				return history, err
			} else if rec.Kind == "attempt" && n != rec.Attempt {
				return history, fmt.Errorf("attempt number mismatch %q", rec.ID)
			}
			if rec.Kind == "catch" {
				selections[rec.ID] = rec.Rule
			}
		}
	}
	handlerOwners := map[string]string{}
	bindHandler := func(aid, rule string) {
		contextID := aid + "/catch:" + rule
		if owner, exists := handlerOwners[contextID]; exists && owner != aid {
			handlerOwners[contextID] = "" // Required ancestry is ambiguous.
		} else if !exists {
			handlerOwners[contextID] = aid
		}
	}
	for aid, rule := range selections {
		if rule != "" {
			bindHandler(aid, rule)
		}
	}
	for recordKey := range seen {
		kind, aid, ok := strings.Cut(recordKey, "\x00")
		if !ok || (kind != "attempt" && kind != "result") {
			continue
		}
		if _, recorded := selections[aid]; recorded {
			continue
		}
		boundary, _, _ := catchSplitAttempt(aid) // Validated above.
		for _, rule := range declarations[boundary].Rules {
			bindHandler(aid, rule)
		}
	}
	for _, entry := range entries {
		rec := entry.Record
		switch rec.Kind {
		case "result", "catch", "handled", "decision":
		default:
			continue
		}
		e := evidence[rec.ID]
		if rec.Kind != "result" && !seen["attempt\x00"+rec.ID] && !seen["result\x00"+rec.ID] {
			return history, fmt.Errorf("orphan %s for %q", rec.Kind, rec.ID)
		}
		if e == nil {
			ref, kind, site, parent, err := catchReference(key, rec.ID, handlerOwners, seen)
			if err != nil {
				return history, err
			}
			e = &catchAttemptEvidence{row: CatchAttempt{Boundary: ref, BoundaryKind: kind, Site: site, ParentID: parent, Selection: CatchSelectionUnknown, Handling: CatchHandlingUnknown}}
			evidence[rec.ID] = e
			order = append(order, rec.ID)
		}
		switch rec.Kind {
		case "result":
			if e.selection != nil || e.handled != nil || e.decision != nil {
				return history, fmt.Errorf("result follows control for %q", rec.ID)
			}
			e.result = &rec
		case "catch":
			if e.handled != nil || e.decision != nil {
				return history, fmt.Errorf("selection follows handling for %q", rec.ID)
			}
			if policy, ok := declarations[e.row.Boundary.BoundaryID]; ok && rec.Rule != "" {
				found := false
				for _, rule := range policy.Rules {
					if rule == rec.Rule {
						found = true
					}
				}
				if !found {
					return history, fmt.Errorf("selected undeclared rule for %q", rec.ID)
				}
			}
			e.selection = &rec
		case "handled":
			if e.decision != nil {
				return history, fmt.Errorf("handled follows decision for %q", rec.ID)
			}
			if rec.Disposition == "" {
				return history, fmt.Errorf("handled disposition missing for %q", rec.ID)
			}
			if e.selection != nil && (e.selection.Rule == "" || e.selection.ErrorKind != "") {
				return history, fmt.Errorf("handled without selected rule for %q", rec.ID)
			}
			e.handled = &rec
		case "decision":
			if rec.Disposition == "" {
				return history, fmt.Errorf("decision disposition missing for %q", rec.ID)
			}
			if e.handled != nil && e.handled.ErrorKind == "" && rec.Retry {
				return history, fmt.Errorf("retry after successful handling for %q", rec.ID)
			}
			if e.selection != nil && e.selection.ErrorKind != "" && rec.Disposition != string(CatchPropagate) {
				return history, fmt.Errorf("predicate failure disposition mismatch for %q", rec.ID)
			}
			if e.selection != nil && e.selection.Rule != "" && e.handled == nil {
				return history, fmt.Errorf("decision before selected handling for %q", rec.ID)
			}
			if e.handled != nil && rec.Disposition != e.handled.Disposition {
				return history, fmt.Errorf("conflicting disposition for %q", rec.ID)
			}
			if rec.Retry && (rec.Disposition == string(CatchPropagate) || rec.WakeAt.IsZero()) {
				return history, fmt.Errorf("invalid retry decision for %q", rec.ID)
			}
			e.decision = &rec
		}
	}
	for _, id := range order {
		e := evidence[id]
		a := &e.row
		if e.result != nil && e.result.ErrorKind == "" {
			if e.selection != nil || e.handled != nil || e.decision != nil {
				return history, fmt.Errorf("failure control for successful attempt %q", id)
			}
			continue
		}
		if e.result == nil {
			a.Gaps = append(a.Gaps, CatchEvidenceGap{CatchGapOriginal, CatchGapNotRecorded})
		} else {
			a.Original = catchFailure(*e.result)
			a.Gaps = append(a.Gaps, CatchEvidenceGap{CatchGapOriginalOrigin, CatchGapNotRecorded})
		}
		if e.selection != nil {
			switch {
			case e.selection.ErrorKind != "":
				if e.selection.Rule != "" {
					return history, fmt.Errorf("predicate error also selected rule for %q", id)
				}
				a.Selection = CatchSelectionPredicateError
				a.PredicateError = catchFailure(*e.selection)
				a.Handling = CatchHandlingNotApplicable
			case e.selection.Rule != "":
				a.Selection = CatchSelectionSelected
				a.RuleID = e.selection.Rule
				a.Handling = CatchHandlingPending
			default:
				a.Selection = CatchSelectionUnmatched
				a.Handling = CatchHandlingNotApplicable
			}
		} else {
			a.Gaps = append(a.Gaps, CatchEvidenceGap{CatchGapSelection, CatchGapFrontier})
		}
		if e.handled != nil {
			a.Handling = CatchHandlingAccepted
			a.Accepted = &CatchAcceptedResult{Disposition: CatchDisposition(e.handled.Disposition), Output: catchSnapshot(e.handled.Output), Failure: catchFailure(*e.handled), Provenance: CatchProvenanceUnknown}
			a.Gaps = append(a.Gaps, CatchEvidenceGap{CatchGapAcceptedProvenance, CatchGapNotRecorded})
		}
		if e.decision != nil {
			a.Retry = &CatchRetryDecision{Retry: e.decision.Retry}
			if e.decision.Retry && !e.decision.WakeAt.IsZero() {
				wake := e.decision.WakeAt
				a.Retry.WakeAt = &wake
			}
		} else {
			a.Gaps = append(a.Gaps, CatchEvidenceGap{CatchGapRetry, CatchGapFrontier})
		}
		if a.Handling == CatchHandlingUnknown {
			// A declaration can prove the engine's no-catch/no-retry fast path. An
			// omitted organizational declaration cannot prove absence of control work.
			if policy, ok := declarations[a.Boundary.BoundaryID]; ok && len(policy.Rules) == 0 && policy.Retry.MaximumAttempts == 1 {
				a.Handling = CatchHandlingNotApplicable
			}
			if a.Handling == CatchHandlingUnknown {
				a.Gaps = append(a.Gaps, CatchEvidenceGap{CatchGapHandling, CatchGapFrontier})
			}
		}
		history.Attempts = append(history.Attempts, *a)
	}
	return history, nil
}

func catchFailure(rec jobdb.ExecutionRecord) *CatchFailure {
	if rec.ErrorKind == "" {
		return nil
	}
	return &CatchFailure{Kind: rec.ErrorKind, Data: append(json.RawMessage(nil), rec.Error...)}
}
func catchSnapshot(in *jobdb.ExecutionSnapshot) *jobdb.ExecutionSnapshot {
	if in == nil {
		return nil
	}
	out := *in
	out.Data = append(json.RawMessage(nil), in.Data...)
	out.Artifacts = append([]jobdb.ExecutionArtifact(nil), in.Artifacts...)
	for i := range out.Artifacts {
		if in.Artifacts[i].Key != nil {
			key := *in.Artifacts[i].Key
			out.Artifacts[i].Key = &key
		}
	}
	return &out
}
