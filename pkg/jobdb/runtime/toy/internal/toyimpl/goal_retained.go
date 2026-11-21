package toyimpl

import (
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
)

// toyGoalStore projects lifecycle from retained records. Only waits have their
// own persistent map. The caller holds engine.mu, including when a record is
// already locked; store methods must not acquire record locks again.
// A clone stages per-job facts and detached waits for all-or-nothing publication.
type toyGoalStore struct {
	engine   *ToyEngine
	waits    map[jobdb.DependencyWaitKey]jobdb.DependencyWaitInfo
	staged   bool
	closures map[jobdb.JobKey]*toyGoalClosure
	records  map[jobdb.JobKey]*jobRecord
}

var _ goalstate.Store = (*toyGoalStore)(nil)
var _ goalstate.FailureRecordStore = (*toyFailureTransaction)(nil)

func (e *ToyEngine) goalStoreLocked() *toyGoalStore {
	e.initGoalsLocked()
	return &toyGoalStore{engine: e, waits: e.goalWaits}
}
func (s *toyGoalStore) Clone() *toyGoalStore {
	return &toyGoalStore{engine: s.engine, waits: maps.Clone(s.waits), staged: true,
		closures: map[jobdb.JobKey]*toyGoalClosure{}, records: map[jobdb.JobKey]*jobRecord{}}
}
func (s *toyGoalStore) commit() {
	for k, r := range s.records {
		s.engine.jobRecords[k] = r
	}
	for k, c := range s.closures {
		s.engine.jobRecords[k].goalClosure = c
	}
	s.engine.goalWaits = s.waits
}
func (s *toyGoalStore) record(k jobdb.JobKey) *jobRecord {
	if r := s.records[k]; r != nil {
		return r
	}
	return s.engine.jobRecords[k]
}
func (s *toyGoalStore) closure(k jobdb.JobKey, r *jobRecord) *toyGoalClosure {
	if c := s.closures[k]; c != nil {
		return c
	}
	return r.goalClosure
}
func invalidToyGoal(message string) error {
	return fmt.Errorf("%w: %s", jobdb.ErrInvalidJobRecord, message)
}
func (s *toyGoalStore) Job(k jobdb.JobKey) (goalstate.Job, error) {
	r := s.record(k)
	if r == nil {
		return goalstate.Job{}, jobdb.ErrJobNotFound
	}
	if err := r.recordKind.Validate(); err != nil {
		return goalstate.Job{}, err
	}
	return goalJob(k, r), nil
}
func (s *toyGoalStore) Goal(k jobdb.GoalKey) (jobdb.GoalInfo, error) {
	g := jobdb.GoalInfo{GoalKey: k, State: jobdb.GoalOpen, Revision: 1}
	var origin, holder *jobRecord
	var closure *toyGoalClosure
	var holderKey jobdb.JobKey
	visit := func(key jobdb.JobKey, r *jobRecord) error {
		if key.TenantId != k.TenantId || r.goalID != k.GoalID {
			return nil
		}
		if r.goalOrigin {
			if origin != nil {
				return invalidToyGoal("multiple retained origins")
			}
			origin = r
		}
		if c := s.closure(key, r); c != nil {
			if closure != nil {
				return invalidToyGoal("multiple retained closures")
			}
			closure, holder, holderKey = c, r, key
		}
		return nil
	}
	for key, r := range s.engine.jobRecords {
		if err := visit(key, r); err != nil {
			return g, err
		}
	}
	for key, r := range s.records {
		if err := visit(key, r); err != nil {
			return g, err
		}
	}
	if origin == nil {
		return g, jobdb.ErrGoalNotFound
	}
	g.Cell, g.CellID, g.CellRepo = origin.cell.Cell, origin.cell.CellID, origin.cell.CellRepo
	g.CreatedAt, g.UpdatedAt = origin.createdAt, origin.createdAt
	if k.Validate() != nil || g.CreatedAt.IsZero() || strings.TrimSpace(g.CellID) == "" || strings.TrimSpace(g.Cell) == "" {
		return g, invalidToyGoal("invalid retained origin identity")
	}
	if _, err := jobdb.NormalizeCellRepo(g.CellRepo); err != nil {
		return g, invalidToyGoal("invalid retained origin repository")
	}
	if closure == nil {
		return g, nil
	}
	if closure.at.IsZero() {
		return g, invalidToyGoal("invalid closure time")
	}
	at := closure.at
	g.State, g.Revision, g.UpdatedAt, g.ClosedAt = closure.state, 2, at, &at
	switch g.State {
	case jobdb.GoalSucceeded:
		if goalJob(holderKey, holder).RecordKind != jobdb.JobRecordExecution || !goalJob(holderKey, holder).Success || closure.winner != holderKey.JobId {
			return g, invalidToyGoal("invalid successful closure witness")
		}
		g.SuccessfulJobID = closure.winner
	case jobdb.GoalFailed:
		if !holder.recordKind.IsTerminalRecord() || holder.terminalFailure == nil || closure.winner != "" || !holder.terminalFailure.FailedAt.Equal(at) {
			return g, invalidToyGoal("invalid protected failure closure")
		}
		if err := validateProtectedToyRecord(holder); err != nil {
			return g, err
		}
		if err := (jobdb.JobInfo{Cell: g.Cell, CellID: g.CellID, CellRepo: g.CellRepo, GoalID: k.GoalID,
			RecordKind: holder.recordKind, Status: holder.status, CompletionStatus: holder.completionStatus,
			CompletionDetail: holder.completionDetail, TerminalFailure: holder.terminalFailure}).ValidateRecord(); err != nil {
			return g, err
		}
		g.FailureJobID, g.Failure = holderKey.JobId, jobdb.CloneGoalFailure(holder.terminalFailure)
	default:
		return g, invalidToyGoal("unknown closure state")
	}
	return g, nil
}
func (s *toyGoalStore) SaveGoal(g jobdb.GoalInfo) error {
	old, err := s.Goal(g.GoalKey)
	if err != nil {
		return err
	}
	if reflect.DeepEqual(old, g) {
		return nil
	}
	if old.State != jobdb.GoalOpen {
		return jobdb.ErrGoalConflict
	}
	if g.CellID != old.CellID || g.Cell != old.Cell || g.CellRepo != old.CellRepo || !g.CreatedAt.Equal(old.CreatedAt) || g.Revision != 2 || g.ClosedAt == nil || g.ClosedAt.IsZero() || !g.UpdatedAt.Equal(*g.ClosedAt) {
		return invalidToyGoal("closure changes origin or lacks exact timestamps")
	}
	holder := g.SuccessfulJobID
	if g.State == jobdb.GoalFailed {
		holder = g.FailureJobID
	}
	key := jobdb.JobKey{TenantId: g.GoalKey.TenantId, JobId: holder}
	r := s.record(key)
	if r == nil || r.goalID != g.GoalKey.GoalID {
		return invalidToyGoal("closure holder changes primary goal")
	}
	switch g.State {
	case jobdb.GoalSucceeded:
		if g.Failure != nil || g.FailureJobID != "" || goalJob(key, r).RecordKind != jobdb.JobRecordExecution || !goalJob(key, r).Success {
			return invalidToyGoal("success witness is not a successful execution")
		}
	case jobdb.GoalFailed:
		if g.SuccessfulJobID != "" || !r.recordKind.IsTerminalRecord() || !reflect.DeepEqual(g.Failure, r.terminalFailure) || g.Failure == nil || !g.Failure.FailedAt.Equal(*g.ClosedAt) {
			return invalidToyGoal("closure differs from protected failure snapshot")
		}
		if err := validateProtectedToyRecord(r); err != nil {
			return err
		}
	default:
		return invalidToyGoal("unknown closure state")
	}
	c := &toyGoalClosure{state: g.State, at: *g.ClosedAt, winner: g.SuccessfulJobID}
	if s.staged {
		s.closures[key] = c
	} else {
		r.goalClosure = c
	}
	return nil
}
func (s *toyGoalStore) Candidates(k jobdb.GoalKey) ([]goalstate.Job, error) {
	out := []goalstate.Job{}
	for key, r := range s.engine.jobRecords {
		if key.TenantId == k.TenantId && r.goalID == k.GoalID {
			out = append(out, goalJob(key, r))
		}
	}
	for key, r := range s.records {
		if key.TenantId == k.TenantId && r.goalID == k.GoalID {
			out = append(out, goalJob(key, r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key.JobId < out[j].Key.JobId })
	return out, nil
}
func (s *toyGoalStore) Goals(k jobdb.JobKey) ([]jobdb.GoalKey, error) {
	j, err := s.Job(k)
	if err != nil {
		return nil, err
	}
	return []jobdb.GoalKey{{TenantId: k.TenantId, GoalID: j.GoalID}}, nil
}
func (s *toyGoalStore) checkAdmission(k jobdb.GoalKey, provisional jobdb.JobKey) error {
	g, err := s.Goal(k)
	if err != nil {
		return err
	}
	if g.State != jobdb.GoalOpen {
		return jobdb.ErrGoalConflict
	}
	jobs, err := s.Candidates(k)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.Key != provisional && !j.Terminal {
			return jobdb.ErrGoalConflict
		}
	}
	return nil
}
func (s *toyGoalStore) Wait(k jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	w, ok := s.waits[k]
	if !ok {
		return w, jobdb.ErrDependencyWaitNotFound
	}
	return cellstate.Clone(w), nil
}
func (s *toyGoalStore) SaveWait(w jobdb.DependencyWaitInfo) error {
	s.waits[jobdb.DependencyWaitKey{JobKey: w.JobKey, WaitID: w.WaitID}] = cellstate.Clone(w)
	return nil
}
func (s *toyGoalStore) ConsumerWaits(k jobdb.JobKey) ([]jobdb.DependencyWaitInfo, error) {
	out := []jobdb.DependencyWaitInfo{}
	for key, w := range s.waits {
		if key.JobKey == k && w.State == jobdb.DependencyPending {
			out = append(out, cellstate.Clone(w))
		}
	}
	return out, nil
}
func (s *toyGoalStore) TargetWaits(tenant string, t jobdb.DependencyTarget) ([]jobdb.DependencyWaitInfo, error) {
	out := []jobdb.DependencyWaitInfo{}
	for key, w := range s.waits {
		if key.JobKey.TenantId != tenant || w.State != jobdb.DependencyPending {
			continue
		}
		for _, d := range w.Dependencies {
			if d.Target == t && d.State == jobdb.DependencyPending {
				out = append(out, cellstate.Clone(w))
				break
			}
		}
	}
	return out, nil
}
