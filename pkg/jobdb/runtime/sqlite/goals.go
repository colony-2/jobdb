package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
	"github.com/segmentio/ksuid"
)

type goalQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}
type goalStore struct {
	ctx context.Context
	q   goalQuery
}

func goalEngine(ctx context.Context, q goalQuery) goalstate.Engine {
	return goalstate.Engine{Store: goalStore{ctx, q}, Now: time.Now().UTC()}
}
func (s goalStore) Job(key jobdb.JobKey) (goalstate.Job, error) {
	var j goalstate.Job
	var archived sql.NullInt64
	var status sql.NullString
	var created int64
	err := s.q.QueryRowContext(s.ctx, `SELECT goal_id,archived_at_ns,completion_status,created_at_ns FROM jobdb_jobs WHERE tenant_id=? AND job_id=?`, key.TenantId, key.JobId).Scan(&j.GoalID, &archived, &status, &created)
	if err == sql.ErrNoRows {
		err = jobdb.ErrJobNotFound
	}
	j.Key = key
	j.Terminal = archived.Valid
	j.Success = j.Terminal && (status.String == "success" || status.String == "succeeded")
	j.CreatedAt = timeFromNS(created)
	return j, err
}
func (s goalStore) Goal(key jobdb.GoalKey) (jobdb.GoalInfo, error) {
	var raw []byte
	var g jobdb.GoalInfo
	err := s.q.QueryRowContext(s.ctx, `SELECT payload FROM jobdb_goals WHERE tenant_id=? AND goal_id=?`, key.TenantId, key.GoalID).Scan(&raw)
	if err == sql.ErrNoRows {
		return g, jobdb.ErrGoalNotFound
	}
	if err != nil {
		return g, err
	}
	err = json.Unmarshal(raw, &g)
	return g, err
}
func (s goalStore) SaveGoal(g jobdb.GoalInfo) error {
	raw, err := json.Marshal(g)
	if err != nil {
		return err
	}
	_, err = s.q.ExecContext(s.ctx, `INSERT INTO jobdb_goals(tenant_id,goal_id,payload) VALUES(?,?,?) ON CONFLICT(tenant_id,goal_id) DO UPDATE SET payload=excluded.payload`, g.GoalKey.TenantId, g.GoalKey.GoalID, raw)
	return err
}
func (s goalStore) Candidates(key jobdb.GoalKey) ([]goalstate.Job, error) {
	rows, err := s.q.QueryContext(s.ctx, `SELECT job_id FROM jobdb_jobs WHERE tenant_id=? AND goal_id=? UNION SELECT job_id FROM jobdb_goal_candidates WHERE tenant_id=? AND goal_id=? ORDER BY job_id`, key.TenantId, key.GoalID, key.TenantId, key.GoalID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := make([]goalstate.Job, 0, len(ids))
	for _, id := range ids {
		j, err := s.Job(jobdb.JobKey{TenantId: key.TenantId, JobId: id})
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}
func (s goalStore) Goals(key jobdb.JobKey) ([]jobdb.GoalKey, error) {
	j, err := s.Job(key)
	if err != nil {
		return nil, err
	}
	ids, err := s.otherGoals(key)
	if err != nil {
		return nil, err
	}
	out := []jobdb.GoalKey{{TenantId: key.TenantId, GoalID: j.GoalID}}
	for _, id := range ids {
		out = append(out, jobdb.GoalKey{TenantId: key.TenantId, GoalID: id})
	}
	return out, nil
}
func (s goalStore) otherGoals(key jobdb.JobKey) ([]string, error) {
	rows, err := s.q.QueryContext(s.ctx, `SELECT goal_id FROM jobdb_goal_candidates WHERE tenant_id=? AND job_id=? ORDER BY goal_id`, key.TenantId, key.JobId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
func (s goalStore) Candidate(key jobdb.GoalKey, id string) (time.Time, bool, error) {
	j, err := s.Job(jobdb.JobKey{TenantId: key.TenantId, JobId: id})
	if err != nil {
		return time.Time{}, false, err
	}
	if j.GoalID == key.GoalID {
		return j.CreatedAt, true, nil
	}
	var at int64
	err = s.q.QueryRowContext(s.ctx, `SELECT added_at_ns FROM jobdb_goal_candidates WHERE tenant_id=? AND goal_id=? AND job_id=?`, key.TenantId, key.GoalID, id).Scan(&at)
	if err == sql.ErrNoRows {
		return time.Time{}, false, nil
	}
	return timeFromNS(at), err == nil, err
}
func (s goalStore) InsertCandidate(key jobdb.GoalKey, id string, at time.Time) error {
	_, err := s.q.ExecContext(s.ctx, `INSERT INTO jobdb_goal_candidates(tenant_id,goal_id,job_id,added_at_ns) VALUES(?,?,?,?)`, key.TenantId, key.GoalID, id, timeToNS(at))
	return err
}
func (s goalStore) Wait(key jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	var raw []byte
	var w jobdb.DependencyWaitInfo
	err := s.q.QueryRowContext(s.ctx, `SELECT payload FROM jobdb_dependency_waits WHERE tenant_id=? AND job_id=? AND wait_id=?`, key.JobKey.TenantId, key.JobKey.JobId, key.WaitID).Scan(&raw)
	if err == sql.ErrNoRows {
		return w, jobdb.ErrDependencyWaitNotFound
	}
	if err != nil {
		return w, err
	}
	err = json.Unmarshal(raw, &w)
	return w, err
}
func (s goalStore) SaveWait(w jobdb.DependencyWaitInfo) error {
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	_, err = s.q.ExecContext(s.ctx, `INSERT INTO jobdb_dependency_waits(tenant_id,job_id,wait_id,state,payload) VALUES(?,?,?,?,?) ON CONFLICT(tenant_id,job_id,wait_id) DO UPDATE SET state=excluded.state,payload=excluded.payload`, w.JobKey.TenantId, w.JobKey.JobId, w.WaitID, string(w.State), raw)
	if err != nil {
		return err
	}
	_, err = s.q.ExecContext(s.ctx, `DELETE FROM jobdb_dependency_targets WHERE tenant_id=? AND job_id=? AND wait_id=?`, w.JobKey.TenantId, w.JobKey.JobId, w.WaitID)
	if err != nil {
		return err
	}
	if w.State != jobdb.DependencyPending {
		return nil
	}
	for _, d := range w.Dependencies {
		if d.State != jobdb.DependencyPending {
			continue
		}
		kind, id := targetParts(d.Target)
		_, err = s.q.ExecContext(s.ctx, `INSERT INTO jobdb_dependency_targets(tenant_id,job_id,wait_id,target_kind,target_id) VALUES(?,?,?,?,?)`, w.JobKey.TenantId, w.JobKey.JobId, w.WaitID, kind, id)
		if err != nil {
			return err
		}
	}
	return nil
}
func targetParts(t jobdb.DependencyTarget) (string, string) {
	if t.GoalID != "" {
		return "goal", t.GoalID
	}
	return "job", t.JobID
}
func (s goalStore) ConsumerWaits(key jobdb.JobKey) ([]jobdb.DependencyWaitInfo, error) {
	return s.waitQuery(`SELECT payload FROM jobdb_dependency_waits WHERE tenant_id=? AND job_id=? AND state='PENDING'`, key.TenantId, key.JobId)
}
func (s goalStore) TargetWaits(tenant string, target jobdb.DependencyTarget) ([]jobdb.DependencyWaitInfo, error) {
	kind, id := targetParts(target)
	return s.waitQuery(`SELECT w.payload FROM jobdb_dependency_targets t JOIN jobdb_dependency_waits w ON w.tenant_id=t.tenant_id AND w.job_id=t.job_id AND w.wait_id=t.wait_id WHERE t.tenant_id=? AND t.target_kind=? AND t.target_id=? ORDER BY w.job_id,w.wait_id`, tenant, kind, id)
}
func (s goalStore) waitQuery(query string, args ...any) ([]jobdb.DependencyWaitInfo, error) {
	rows, err := s.q.QueryContext(s.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []jobdb.DependencyWaitInfo{}
	for rows.Next() {
		var raw []byte
		var w jobdb.DependencyWaitInfo
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &w); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (r *Runtime) GetGoal(ctx context.Context, key jobdb.GoalKey) (jobdb.GoalInfo, error) {
	if err := key.Validate(); err != nil {
		return jobdb.GoalInfo{}, err
	}
	return (goalStore{ctxOrBackground(ctx), r.db}).Goal(key)
}
func (r *Runtime) AddGoalCandidate(ctx context.Context, req jobdb.AddGoalCandidateRequest) (out jobdb.GoalCandidateInfo, err error) {
	ctx = ctxOrBackground(ctx)
	err = r.withTx(ctx, func(tx *sql.Tx) error { var e error; out, e = goalEngine(ctx, tx).Add(req); return e })
	return
}
func (r *Runtime) FailGoal(ctx context.Context, req jobdb.FailGoalRequest) (out jobdb.GoalInfo, err error) {
	ctx = ctxOrBackground(ctx)
	err = r.withTx(ctx, func(tx *sql.Tx) error { var e error; out, e = goalEngine(ctx, tx).Fail(req); return e })
	return
}
func (r *Runtime) GetDependencyWait(ctx context.Context, key jobdb.DependencyWaitKey) (jobdb.DependencyWaitInfo, error) {
	ctx = ctxOrBackground(ctx)
	// Read all target state at one snapshot and use a clock sampled before that
	// snapshot. A concurrent resolution cannot become a contradictory timeout.
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return jobdb.DependencyWaitInfo{}, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	store := goalStore{ctx, tx}
	if _, err := store.Job(key.JobKey); err != nil {
		return jobdb.DependencyWaitInfo{}, err
	}
	wait, err := store.Wait(key)
	if err != nil {
		return wait, err
	}
	return (goalstate.Engine{Store: store, Now: now}).Evaluate(wait)
}
func (r *Runtime) goalHandle(ctx context.Context, key jobdb.JobKey) (jobdb.JobHandle, error) {
	j, err := (goalStore{ctxOrBackground(ctx), r.db}).Job(key)
	return jobdb.JobHandle{JobKey: key, GoalID: j.GoalID}, err
}

func structuredWaitReady(ctx context.Context, q queryer, row jobRow, now time.Time) (bool, error) {
	if row.dependencyWaitID == "" {
		return true, nil
	}
	var raw []byte
	err := q.QueryRowContext(ctx, `SELECT payload FROM jobdb_dependency_waits WHERE tenant_id=? AND job_id=? AND wait_id=?`, row.tenantID, row.jobID, row.dependencyWaitID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, jobdb.ErrDependencyWaitNotFound
	}
	if err != nil {
		return false, err
	}
	var w jobdb.DependencyWaitInfo
	if err = json.Unmarshal(raw, &w); err != nil {
		return false, err
	}
	return w.State != jobdb.DependencyPending || (w.Deadline != nil && !now.Before(*w.Deadline)), nil
}

// PrepareJob reserves identities without publishing a job or goal.
func (r *Runtime) PrepareJob(ctx context.Context, req goalstate.Preparation) (out goalstate.Prepared, err error) {
	ctx = ctxOrBackground(ctx)
	err = r.withTx(ctx, func(tx *sql.Tx) error {
		query := `SELECT job_id,goal_id,descriptor FROM jobdb_goal_preparations WHERE tenant_id=? AND job_id=?`
		args := []any{req.Key.TenantId, req.Key.JobId}
		if req.RequestID != "" {
			query = `SELECT job_id,goal_id,descriptor FROM jobdb_goal_preparations WHERE tenant_id=? AND request_id=?`
			args = []any{req.Key.TenantId, req.RequestID}
		}
		var id, goalID, descriptor string
		e := tx.QueryRowContext(ctx, query, args...).Scan(&id, &goalID, &descriptor)
		if e == nil {
			if descriptor != req.Descriptor || (req.Key.JobId != "" && id != req.Key.JobId) {
				return goalstate.PreparationMismatch(descriptor, req.Descriptor)
			}
			out = goalstate.Prepared{Key: jobdb.JobKey{TenantId: req.Key.TenantId, JobId: id}, GoalID: goalID}
			var count int
			if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM jobdb_jobs WHERE tenant_id=? AND job_id=?`, out.Key.TenantId, id).Scan(&count); e != nil {
				return e
			}
			out.Published = count != 0
			return nil
		}
		if e != sql.ErrNoRows {
			return e
		}
		if req.Key.JobId == "" {
			req.Key.JobId = ksuid.New().String()
		}
		if e = req.Key.Validate(); e != nil {
			return e
		}
		if req.RequestID != "" {
			var previous string
			e = tx.QueryRowContext(ctx, `SELECT descriptor FROM jobdb_goal_preparations WHERE tenant_id=? AND job_id=?`, req.Key.TenantId, req.Key.JobId).Scan(&previous)
			if e == nil {
				return jobdb.NewExistingJobMismatchError("destination belongs to a different operation")
			}
			if e != sql.ErrNoRows {
				return e
			}
		}
		// Jobs predating reservations (migration and scheduled occurrences) still
		// pass through the established history-equivalence reconciliation.
		if existing, lookupErr := (goalStore{ctx, tx}).Job(req.Key); lookupErr == nil {
			if len(req.Dependencies) > 0 || req.ShareGoal {
				return jobdb.NewExistingJobMismatchError("existing job has no matching goal preparation")
			}
			out = goalstate.Prepared{Key: req.Key, GoalID: existing.GoalID}
			return nil
		} else if !errors.Is(lookupErr, jobdb.ErrJobNotFound) {
			return lookupErr
		}
		goalID = ksuid.New().String()
		if req.ShareGoal {
			j, e := (goalStore{ctx, tx}).Job(jobdb.JobKey{TenantId: req.Key.TenantId, JobId: req.PriorJobID})
			if e != nil {
				return e
			}
			goalID = j.GoalID
			g, e := (goalStore{ctx, tx}).Goal(jobdb.GoalKey{TenantId: req.Key.TenantId, GoalID: goalID})
			if e != nil {
				return e
			}
			if g.State != jobdb.GoalOpen {
				return jobdb.ErrGoalConflict
			}
		}
		deps, e := json.Marshal(req.Dependencies)
		if e != nil {
			return e
		}
		var requestID any
		if req.RequestID != "" {
			requestID = req.RequestID
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO jobdb_goal_preparations(tenant_id,job_id,request_id,goal_id,share_goal,descriptor,dependencies) VALUES(?,?,?,?,?,?,?)`, req.Key.TenantId, req.Key.JobId, requestID, goalID, req.ShareGoal, req.Descriptor, deps)
		if e != nil {
			return e
		}
		out = goalstate.Prepared{Key: req.Key, GoalID: goalID}
		return nil
	})
	return
}
func publishGoal(ctx context.Context, tx *sql.Tx, key jobdb.JobKey, now time.Time) (string, []jobdb.Dependency, error) {
	var goalID string
	var shared bool
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT goal_id,share_goal,dependencies FROM jobdb_goal_preparations WHERE tenant_id=? AND job_id=?`, key.TenantId, key.JobId).Scan(&goalID, &shared, &raw)
	if err == sql.ErrNoRows {
		goalID = ksuid.New().String()
	} else if err != nil {
		return "", nil, err
	}
	var deps []jobdb.Dependency
	if len(raw) > 0 {
		if err = json.Unmarshal(raw, &deps); err != nil {
			return "", nil, err
		}
	}
	s := goalStore{ctx, tx}
	gk := jobdb.GoalKey{TenantId: key.TenantId, GoalID: goalID}
	if shared {
		g, err := s.Goal(gk)
		if err != nil {
			return "", nil, err
		}
		if g.State != jobdb.GoalOpen {
			return "", nil, jobdb.ErrGoalConflict
		}
	} else {
		if err = s.SaveGoal(jobdb.GoalInfo{GoalKey: gk, State: jobdb.GoalOpen, Revision: 1, CreatedAt: now, UpdatedAt: now}); err != nil {
			return "", nil, err
		}
	}
	return goalID, deps, nil
}

// Fetch additional associations only for the returned page, through the reverse index.
func (r *Runtime) batchOtherGoals(ctx context.Context, jobs []jobdb.JobSummary) (map[jobdb.JobKey][]string, error) {
	out := map[jobdb.JobKey][]string{}
	tenants := map[string][]string{}
	for _, j := range jobs {
		tenants[j.JobKey.TenantId] = append(tenants[j.JobKey.TenantId], j.JobKey.JobId)
	}
	for tenant, ids := range tenants {
		args := []any{tenant}
		for _, id := range ids {
			args = append(args, id)
		}
		rows, err := r.db.QueryContext(ctx, "SELECT job_id,goal_id FROM jobdb_goal_candidates WHERE tenant_id=? AND job_id IN ("+placeholders(len(ids))+") ORDER BY job_id,goal_id", args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, g string
			if err = rows.Scan(&id, &g); err != nil {
				rows.Close()
				return nil, err
			}
			k := jobdb.JobKey{TenantId: tenant, JobId: id}
			out[k] = append(out[k], g)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
func placeholders(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }
func goalListPredicate(req jobdb.ListJobsRequest) (string, []any) {
	tenants := placeholders(len(req.TenantIds))
	args := []any{}
	for _, id := range req.TenantIds {
		args = append(args, id)
	}
	where := "tenant_id IN (" + tenants + ")"
	if !req.IncludeHidden {
		where += " AND hidden_at_ns IS NULL"
	}
	if len(req.GoalIDs) > 0 {
		goals := placeholders(len(req.GoalIDs))
		where += " AND (tenant_id,job_id) IN (SELECT tenant_id,job_id FROM jobdb_jobs WHERE tenant_id IN (" + tenants + ") AND goal_id IN (" + goals + ") UNION SELECT tenant_id,job_id FROM jobdb_goal_candidates WHERE tenant_id IN (" + tenants + ") AND goal_id IN (" + goals + "))"
		for i := 0; i < 2; i++ {
			for _, id := range req.TenantIds {
				args = append(args, id)
			}
			for _, id := range req.GoalIDs {
				args = append(args, id)
			}
		}
	}
	return where, args
}
