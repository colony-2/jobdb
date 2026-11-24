package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/colony-2/jobdb/pkg/jobdb/jobid"
)

// retainedGoalStore is the format-13 transition store. The embedded legacy
// store supplies only the unchanged wait/subscription operations. Every goal
// and membership operation below uses retained jobs, never the removed tables.
// Mutating callers own a scheduler write transaction (including writer
// reservation), and must roll back the entire operation on any error.
type retainedGoalStore struct {
	goalStore
	newFailureID func() (string, error)
}

var _ goalstate.FailureRecordStore = retainedGoalStore{}

func retainedStore(ctx context.Context, tx *sql.Tx) retainedGoalStore {
	return retainedQueryStore(ctx, tx)
}

func retainedQueryStore(ctx context.Context, q goalQuery) retainedGoalStore {
	return retainedGoalStore{goalStore: goalStore{ctx, q}}
}

func invalidRetainedGoal(message string) error {
	return fmt.Errorf("%w: %s", jobdb.ErrInvalidJobRecord, message)
}

func (s retainedGoalStore) Job(key jobdb.JobKey) (goalstate.Job, error) {
	j := goalstate.Job{Key: key}
	var archived sql.NullInt64
	var status sql.NullString
	var created int64
	err := s.q.QueryRowContext(s.ctx, `SELECT goal_id,record_kind,archived_at_ns,completion_status,created_at_ns FROM jobdb_jobs WHERE tenant_id=? AND job_id=?`, key.TenantId, key.JobId).Scan(&j.GoalID, &j.RecordKind, &archived, &status, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return j, jobdb.ErrJobNotFound
	}
	if err != nil {
		return j, err
	}
	if err = j.RecordKind.Validate(); err != nil {
		return j, err
	}
	j.Terminal = archived.Valid
	j.Success = j.Terminal && (status.String == "success" || status.String == "succeeded")
	j.CreatedAt = timeFromNS(created)
	return j, nil
}

// Goal projects only the origin and first closure. Later member outcomes and
// hidden flags cannot reopen a goal or replace its selected producer.
func (s retainedGoalStore) Goal(key jobdb.GoalKey) (jobdb.GoalInfo, error) {
	g := jobdb.GoalInfo{GoalKey: key, State: jobdb.GoalOpen, Revision: 1}
	var created int64
	var holder, state, provenance, winner, kind, detail, winnerGoal, winnerStatus sql.NullString
	var closed, archived, winnerArchived sql.NullInt64
	var failure []byte
	err := s.q.QueryRowContext(s.ctx, `SELECT o.cell_id,o.cell_name,o.cell_repo,o.created_at_ns,
 c.job_id,c.goal_closure_state,c.goal_closed_at_ns,c.goal_winner_job_id,c.goal_closure_origin,
 c.record_kind,c.goal_failure,c.archived_at_ns,c.completion_detail,
 w.goal_id,w.archived_at_ns,w.completion_status
 FROM jobdb_jobs o
 LEFT JOIN jobdb_jobs c ON c.tenant_id=o.tenant_id AND c.goal_id=o.goal_id AND c.goal_closure_state IS NOT NULL
 LEFT JOIN jobdb_jobs w ON w.tenant_id=c.tenant_id AND w.job_id=c.goal_winner_job_id
 WHERE o.tenant_id=? AND o.goal_id=? AND o.goal_origin=1`, key.TenantId, key.GoalID).Scan(
		&g.CellID, &g.Cell, &g.CellRepo, &created, &holder, &state, &closed, &winner, &provenance, &kind, &failure, &archived, &detail, &winnerGoal, &winnerArchived, &winnerStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return g, jobdb.ErrGoalNotFound
	}
	if err != nil {
		return g, err
	}
	g.CreatedAt = timeFromNS(created)
	g.UpdatedAt = g.CreatedAt
	if key.Validate() != nil || g.CreatedAt.IsZero() || strings.TrimSpace(g.CellID) == "" || strings.TrimSpace(g.Cell) == "" {
		return g, invalidRetainedGoal("invalid retained origin identity")
	}
	if _, err := jobdb.NormalizeCellRepo(g.CellRepo); err != nil {
		return g, invalidRetainedGoal("invalid retained origin repository")
	}
	if !state.Valid {
		return g, nil
	}
	if !closed.Valid || timeFromNS(closed.Int64).IsZero() || (provenance.String != "native" && provenance.String != "migrated") {
		return g, invalidRetainedGoal("invalid closure time or provenance")
	}
	at := timeFromNS(closed.Int64)
	g.State = jobdb.GoalState(state.String)
	g.Revision = 2
	g.UpdatedAt = at
	g.ClosedAt = &at
	switch g.State {
	case jobdb.GoalSucceeded:
		if kind.String != string(jobdb.JobRecordExecution) || !winner.Valid || !winnerArchived.Valid || (winnerStatus.String != "success" && winnerStatus.String != "succeeded") {
			return g, invalidRetainedGoal("invalid successful closure witness")
		}
		if provenance.String == "native" && (holder.String != winner.String || winnerGoal.String != key.GoalID) {
			return g, invalidRetainedGoal("native closure changes primary producer")
		}
		if winnerGoal.String == key.GoalID && holder.String != winner.String {
			return g, invalidRetainedGoal("primary winner does not hold its closure")
		}
		if winnerGoal.String != key.GoalID {
			var origin bool
			if err = s.q.QueryRowContext(s.ctx, `SELECT goal_origin FROM jobdb_jobs WHERE tenant_id=? AND job_id=?`, key.TenantId, holder.String).Scan(&origin); err != nil {
				return g, err
			}
			if !origin || provenance.String != "migrated" {
				return g, invalidRetainedGoal("cross-primary closure is not on its migrated origin")
			}
		}
		g.SuccessfulJobID = winner.String
	case jobdb.GoalFailed:
		if !jobdb.JobRecordKind(kind.String).IsTerminalRecord() || !archived.Valid || !utf8.Valid(failure) {
			return g, invalidRetainedGoal("invalid terminal failure closure")
		}
		if (provenance.String == "native" && kind.String != string(jobdb.JobRecordGoalFailure)) ||
			(provenance.String == "migrated" && kind.String != string(jobdb.JobRecordMigratedGoalFailure)) {
			return g, invalidRetainedGoal("failure provenance differs from protected kind")
		}
		var f jobdb.GoalFailure
		if err = json.Unmarshal(failure, &f); err != nil {
			return g, fmt.Errorf("%w: invalid saved failure: %v", jobdb.ErrInvalidJobRecord, err)
		}
		info := jobdb.JobInfo{CellID: g.CellID, Cell: g.Cell, CellRepo: g.CellRepo, GoalID: key.GoalID, RecordKind: jobdb.JobRecordKind(kind.String), Status: jobdb.JobStatusCompleted, CompletionStatus: "failed", CompletionDetail: detail.String, TerminalFailure: &f}
		if err = info.ValidateRecord(); err != nil {
			return g, err
		}
		if !f.FailedAt.Equal(at) {
			return g, invalidRetainedGoal("failure and closure times differ")
		}
		g.FailureJobID = holder.String
		g.Failure = jobdb.CloneGoalFailure(&f)
	default:
		return g, invalidRetainedGoal("unknown closure state")
	}
	return g, nil
}

// MarkOrigin is called only for an already inserted primary job in the same
// publication/conversion transaction. Preparation alone cannot publish a goal.
func (s retainedGoalStore) MarkOrigin(key jobdb.JobKey) error {
	result, err := s.q.ExecContext(s.ctx, `UPDATE jobdb_jobs SET goal_origin=1 WHERE tenant_id=? AND job_id=? AND record_kind='EXECUTION'`, key.TenantId, key.JobId)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return jobdb.ErrJobNotFound
	}
	return nil
}

// CheckAdmission includes all retained nonterminal members, including hidden,
// delayed, blocked and cancellation-requested rows. It does not reserve a slot;
// the caller must publish in this same reserved writer transaction.
func (s retainedGoalStore) CheckAdmission(key jobdb.GoalKey) error {
	g, err := s.Goal(key)
	if err != nil {
		return err
	}
	if g.State != jobdb.GoalOpen {
		return jobdb.ErrGoalConflict
	}
	var active bool
	if err = s.q.QueryRowContext(s.ctx, `SELECT EXISTS(SELECT 1 FROM jobdb_jobs WHERE tenant_id=? AND goal_id=? AND archived_at_ns IS NULL)`, key.TenantId, key.GoalID).Scan(&active); err != nil {
		return err
	}
	if active {
		return jobdb.ErrGoalConflict
	}
	return nil
}

func (s retainedGoalStore) SaveGoal(g jobdb.GoalInfo) error {
	holder := g.SuccessfulJobID
	if g.State == jobdb.GoalFailed {
		holder = g.FailureJobID
	}
	return s.saveClosure(g, holder, "native")
}

// saveClosure also serves the locked converter: a migrated cross-primary
// success uses the target origin as holder while retaining the actual producer.
func (s retainedGoalStore) saveClosure(g jobdb.GoalInfo, holder, provenance string) error {
	old, err := s.Goal(g.GoalKey)
	if err != nil {
		return err
	}
	if old.State != jobdb.GoalOpen {
		if reflect.DeepEqual(old, g) {
			return nil
		}
		return jobdb.ErrGoalConflict
	}
	if g.State == jobdb.GoalOpen {
		if reflect.DeepEqual(old, g) {
			return nil
		}
		return invalidRetainedGoal("open goal cannot rewrite its origin")
	}
	if g.CellID != old.CellID || g.Cell != old.Cell || g.CellRepo != old.CellRepo || !g.CreatedAt.Equal(old.CreatedAt) || g.Revision != 2 || g.ClosedAt == nil || g.ClosedAt.IsZero() || !g.UpdatedAt.Equal(*g.ClosedAt) {
		return invalidRetainedGoal("closure changes origin or lacks exact timestamps")
	}
	if provenance != "native" && provenance != "migrated" {
		return invalidRetainedGoal("unknown closure provenance")
	}
	var goal string
	var origin bool
	var kind jobdb.JobRecordKind
	var raw []byte
	err = s.q.QueryRowContext(s.ctx, `SELECT goal_id,goal_origin,record_kind,goal_failure FROM jobdb_jobs WHERE tenant_id=? AND job_id=?`, g.GoalKey.TenantId, holder).Scan(&goal, &origin, &kind, &raw)
	if err != nil {
		return err
	}
	if goal != g.GoalKey.GoalID {
		return invalidRetainedGoal("closure holder changes primary goal")
	}
	var winner any
	switch g.State {
	case jobdb.GoalSucceeded:
		if g.SuccessfulJobID == "" || g.Failure != nil || g.FailureJobID != "" || kind != jobdb.JobRecordExecution {
			return invalidRetainedGoal("inconsistent successful closure")
		}
		w, err := s.Job(jobdb.JobKey{TenantId: g.GoalKey.TenantId, JobId: g.SuccessfulJobID})
		if err != nil {
			return err
		}
		if !w.Success || w.RecordKind != jobdb.JobRecordExecution {
			return invalidRetainedGoal("success witness is not a successful execution")
		}
		if w.GoalID != g.GoalKey.GoalID {
			if provenance != "migrated" || !origin {
				return invalidRetainedGoal("cross-primary winner requires migrated origin closure")
			}
		} else if holder != w.Key.JobId {
			return invalidRetainedGoal("primary winner must hold its own closure")
		}
		winner = g.SuccessfulJobID
	case jobdb.GoalFailed:
		if g.Failure == nil || g.SuccessfulJobID != "" || holder != g.FailureJobID || !kind.IsTerminalRecord() || !utf8.Valid(raw) {
			return invalidRetainedGoal("inconsistent failure closure")
		}
		if (provenance == "native" && kind != jobdb.JobRecordGoalFailure) || (provenance == "migrated" && kind != jobdb.JobRecordMigratedGoalFailure) {
			return invalidRetainedGoal("failure provenance differs from protected kind")
		}
		var saved jobdb.GoalFailure
		if err = json.Unmarshal(raw, &saved); err != nil {
			return err
		}
		if !reflect.DeepEqual(&saved, g.Failure) || !saved.FailedAt.Equal(*g.ClosedAt) {
			return invalidRetainedGoal("closure differs from protected failure snapshot")
		}
	default:
		return invalidRetainedGoal("unknown goal state")
	}
	result, err := s.q.ExecContext(s.ctx, `UPDATE jobdb_jobs SET goal_closure_state=?,goal_closed_at_ns=?,goal_winner_job_id=?,goal_closure_origin=? WHERE tenant_id=? AND job_id=? AND goal_closure_state IS NULL
 AND NOT EXISTS(SELECT 1 FROM jobdb_jobs WHERE tenant_id=? AND goal_id=? AND goal_closure_state IS NOT NULL)`, string(g.State), g.ClosedAt.UnixNano(), winner, provenance, g.GoalKey.TenantId, holder, g.GoalKey.TenantId, g.GoalKey.GoalID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return jobdb.ErrGoalConflict
	}
	return nil
}

func (s retainedGoalStore) Candidates(key jobdb.GoalKey) ([]goalstate.Job, error) {
	rows, err := s.q.QueryContext(s.ctx, `SELECT job_id,record_kind,archived_at_ns,completion_status,created_at_ns FROM jobdb_jobs WHERE tenant_id=? AND goal_id=? ORDER BY job_id`, key.TenantId, key.GoalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []goalstate.Job{}
	for rows.Next() {
		j := goalstate.Job{GoalID: key.GoalID}
		j.Key.TenantId = key.TenantId
		var at int64
		var archived sql.NullInt64
		var status sql.NullString
		if err = rows.Scan(&j.Key.JobId, &j.RecordKind, &archived, &status, &at); err != nil {
			return nil, err
		}
		if err = j.RecordKind.Validate(); err != nil {
			return nil, err
		}
		j.Terminal = archived.Valid
		j.Success = j.Terminal && (status.String == "success" || status.String == "succeeded")
		j.CreatedAt = timeFromNS(at)
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
func (s retainedGoalStore) Goals(key jobdb.JobKey) ([]jobdb.GoalKey, error) {
	j, err := s.Job(key)
	if err != nil {
		return nil, err
	}
	return []jobdb.GoalKey{{TenantId: key.TenantId, GoalID: j.GoalID}}, nil
}

func (s retainedGoalStore) InsertGoalFailure(g jobdb.GoalInfo, f jobdb.GoalFailure) (jobdb.JobKey, error) {
	return s.insertFailure(g, f, jobdb.JobRecordGoalFailure)
}

func (s retainedGoalStore) insertFailure(g jobdb.GoalInfo, f jobdb.GoalFailure, kind jobdb.JobRecordKind) (jobdb.JobKey, error) {
	if !kind.IsTerminalRecord() {
		return jobdb.JobKey{}, invalidRetainedGoal("failure insertion requires protected kind")
	}
	actual, err := s.Goal(g.GoalKey)
	if err != nil {
		return jobdb.JobKey{}, err
	}
	if actual.State != jobdb.GoalOpen {
		return jobdb.JobKey{}, jobdb.ErrGoalConflict
	}
	if actual.CellID != g.CellID || actual.Cell != g.Cell || actual.CellRepo != g.CellRepo {
		return jobdb.JobKey{}, invalidRetainedGoal("failure changes retained ownership")
	}
	info := jobdb.JobInfo{Cell: g.Cell, CellID: g.CellID, CellRepo: g.CellRepo, GoalID: g.GoalKey.GoalID, RecordKind: kind, Status: jobdb.JobStatusCompleted, CompletionStatus: "failed", CompletionDetail: f.Message, TerminalFailure: &f}
	if err = info.ValidateRecord(); err != nil {
		return jobdb.JobKey{}, err
	}
	seen := map[string]bool{}
	for _, ids := range [][]string{f.TerminalJobIDs, f.NonTerminalJobIDs} {
		for _, id := range ids {
			if id == "" || seen[id] {
				return jobdb.JobKey{}, invalidRetainedGoal("failure has duplicate or empty snapshot member")
			}
			seen[id] = true
			if _, err = s.Job(jobdb.JobKey{TenantId: g.GoalKey.TenantId, JobId: id}); err != nil {
				return jobdb.JobKey{}, err
			}
		}
	}
	id := jobid.New()
	if s.newFailureID != nil {
		id, err = s.newFailureID()
		if err != nil {
			return jobdb.JobKey{}, err
		}
	}
	key := jobdb.JobKey{TenantId: g.GoalKey.TenantId, JobId: id}
	if err = key.Validate(); err != nil {
		return jobdb.JobKey{}, err
	}
	var exists bool
	err = s.q.QueryRowContext(s.ctx, `SELECT EXISTS(SELECT 1 FROM jobdb_jobs WHERE tenant_id=? AND job_id=? UNION ALL SELECT 1 FROM jobdb_goal_preparations WHERE tenant_id=? AND job_id=? UNION ALL SELECT 1 FROM jobdb_submission_receipts WHERE tenant_id=? AND job_id=?)`, key.TenantId, id, key.TenantId, id, key.TenantId, id).Scan(&exists)
	if err != nil {
		return jobdb.JobKey{}, err
	}
	if exists {
		return jobdb.JobKey{}, jobdb.NewExistingJobMismatchError("generated failure job collides with retained identity")
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return jobdb.JobKey{}, err
	}
	at := f.FailedAt.UnixNano()
	_, err = s.q.ExecContext(s.ctx, `INSERT INTO jobdb_jobs(tenant_id,job_id,goal_id,cell_id,cell_name,cell_repo,job_type,route_job_type,initial_payload_digest,created_at_ns,updated_at_ns,available_at_ns,archived_at_ns,completion_status,completion_detail,record_kind,goal_failure) VALUES(?,?,?,?,?,?,?,'','',?,?,?,?,'failed',?,?,?)`, key.TenantId, id, g.GoalKey.GoalID, g.CellID, g.Cell, g.CellRepo, jobdb.GoalFailureJobType, at, at, at, at, f.Message, string(kind), raw)
	return key, err
}
