package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type migrationJob struct {
	key                                  jobdb.JobKey
	goal, cell, name, repo, wait, status string
	created                              int64
	archived, hidden                     sql.NullInt64
	appendKey                            string
	appendGeneration                     int64
}
type migrationGoal struct {
	info             jobdb.GoalInfo
	state            string
	created, updated int64
	cell, name, repo string
}
type migrationCandidate struct {
	key   jobdb.GoalKey
	job   string
	added int64
}
type migrationWait struct {
	key   jobdb.DependencyWaitKey
	state string
	info  jobdb.DependencyWaitInfo
}
type migrationTarget struct {
	key      jobdb.DependencyWaitKey
	kind, id string
}
type migrationPreparation struct {
	key                                                        jobdb.JobKey
	goal, request, descriptor, cell, name, repo, parent, prior string
	shared                                                     bool
	dependencies                                               []jobdb.Dependency
}
type migrationReceipt struct {
	key                                           jobdb.JobKey
	goal, descriptor, requested, cell, name, repo string
}
type goalMigrationInput struct {
	jobs         map[jobdb.JobKey]migrationJob
	jobOrder     []jobdb.JobKey
	goals        map[jobdb.GoalKey]migrationGoal
	goalOrder    []jobdb.GoalKey
	cells        map[jobdb.CellKey]bool
	candidates   []migrationCandidate
	waits        []migrationWait
	targets      map[migrationTarget]bool
	preparations []migrationPreparation
	receipts     map[jobdb.JobKey]migrationReceipt
}

func migrationRows(ctx context.Context, tx *sql.Tx, query string, read func(*sql.Rows) error) error {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err = read(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Input loading and semantic validation are shared by read-only inspection and
// the locked converter. No runtime/chapter/blob/configuration store is
// constructed and no stored payload is rewritten by inspection.
func loadGoalMigrationInput(ctx context.Context, tx *sql.Tx, report *GoalMigrationReport) (*goalMigrationInput, error) {
	s := &goalMigrationInput{jobs: map[jobdb.JobKey]migrationJob{}, goals: map[jobdb.GoalKey]migrationGoal{}, cells: map[jobdb.CellKey]bool{}, targets: map[migrationTarget]bool{}, receipts: map[jobdb.JobKey]migrationReceipt{}}
	if err := migrationRows(ctx, tx, "SELECT tenant_id,cell_id FROM jobdb_cells ORDER BY tenant_id,cell_id", func(rows *sql.Rows) error {
		var k jobdb.CellKey
		if err := rows.Scan(&k.TenantID, &k.CellID); err != nil {
			return err
		}
		s.cells[k] = true
		return nil
	}); err != nil {
		return nil, err
	}
	var appendColumns int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('jobdb_jobs') WHERE name IN ('append_key','append_gen')`).Scan(&appendColumns); err != nil {
		return nil, err
	}
	appendProjection := `'',0`
	if appendColumns == 2 {
		appendProjection = `coalesce(append_key,''),coalesce(append_gen,0)`
	}
	if err := migrationRows(ctx, tx, `SELECT tenant_id,job_id,goal_id,cell_id,cell_name,cell_repo,dependency_wait_id,created_at_ns,archived_at_ns,hidden_at_ns,coalesce(completion_status,''),`+appendProjection+` FROM jobdb_jobs ORDER BY tenant_id,job_id`, func(rows *sql.Rows) error {
		var j migrationJob
		if err := rows.Scan(&j.key.TenantId, &j.key.JobId, &j.goal, &j.cell, &j.name, &j.repo, &j.wait, &j.created, &j.archived, &j.hidden, &j.status, &j.appendKey, &j.appendGeneration); err != nil {
			return err
		}
		s.jobs[j.key] = j
		s.jobOrder = append(s.jobOrder, j.key)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := migrationRows(ctx, tx, `SELECT tenant_id,goal_id,state,created_at_ns,updated_at_ns,cell_id,cell_name,cell_repo,payload FROM jobdb_goals ORDER BY tenant_id,goal_id`, func(rows *sql.Rows) error {
		var g migrationGoal
		var k jobdb.GoalKey
		var raw []byte
		if err := rows.Scan(&k.TenantId, &k.GoalID, &g.state, &g.created, &g.updated, &g.cell, &g.name, &g.repo, &raw); err != nil {
			return err
		}
		if !utf8.Valid(raw) || json.Unmarshal(raw, &g.info) != nil {
			report.issue("invalid_goal_payload", k.TenantId, k.GoalID, "", "", "goal payload is not valid GoalInfo JSON")
		}
		if g.info.GoalKey != k || (g.info.CellID != "" && g.info.CellID != g.cell) || (g.info.Cell != "" && g.info.Cell != g.name) || (g.info.CellRepo != "" && g.info.CellRepo != g.repo) {
			report.issue("goal_identity_mismatch", k.TenantId, k.GoalID, "", "", "goal payload identity differs from retained columns")
		}
		g.info.GoalKey = k
		g.info.CellID = g.cell
		g.info.Cell = g.name
		g.info.CellRepo = g.repo
		s.goals[k] = g
		s.goalOrder = append(s.goalOrder, k)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := migrationRows(ctx, tx, `SELECT tenant_id,goal_id,job_id,added_at_ns FROM jobdb_goal_candidates ORDER BY tenant_id,goal_id,job_id`, func(rows *sql.Rows) error {
		var c migrationCandidate
		if err := rows.Scan(&c.key.TenantId, &c.key.GoalID, &c.job, &c.added); err != nil {
			return err
		}
		s.candidates = append(s.candidates, c)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := migrationRows(ctx, tx, `SELECT tenant_id,job_id,wait_id,state,payload FROM jobdb_dependency_waits ORDER BY tenant_id,job_id,wait_id`, func(rows *sql.Rows) error {
		var w migrationWait
		var raw []byte
		if err := rows.Scan(&w.key.JobKey.TenantId, &w.key.JobKey.JobId, &w.key.WaitID, &w.state, &raw); err != nil {
			return err
		}
		if !utf8.Valid(raw) || json.Unmarshal(raw, &w.info) != nil {
			report.issue("invalid_wait_payload", w.key.JobKey.TenantId, "", w.key.JobKey.JobId, w.key.WaitID, "wait payload is not valid DependencyWaitInfo JSON")
		}
		s.waits = append(s.waits, w)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := migrationRows(ctx, tx, `SELECT tenant_id,job_id,wait_id,target_kind,target_id FROM jobdb_dependency_targets ORDER BY tenant_id,job_id,wait_id,target_kind,target_id`, func(rows *sql.Rows) error {
		var t migrationTarget
		if err := rows.Scan(&t.key.JobKey.TenantId, &t.key.JobKey.JobId, &t.key.WaitID, &t.kind, &t.id); err != nil {
			return err
		}
		s.targets[t] = true
		return nil
	}); err != nil {
		return nil, err
	}
	if err := migrationRows(ctx, tx, `SELECT tenant_id,job_id,goal_id,coalesce(request_id,''),descriptor,cell_id,cell_name,cell_repo,parent_job_id,prior_job_id,share_goal,dependencies FROM jobdb_goal_preparations ORDER BY tenant_id,job_id`, func(rows *sql.Rows) error {
		var p migrationPreparation
		var raw []byte
		if err := rows.Scan(&p.key.TenantId, &p.key.JobId, &p.goal, &p.request, &p.descriptor, &p.cell, &p.name, &p.repo, &p.parent, &p.prior, &p.shared, &raw); err != nil {
			return err
		}
		if len(raw) > 0 {
			if !utf8.Valid(raw) || json.Unmarshal(raw, &p.dependencies) != nil {
				report.issue("invalid_preparation_dependencies", p.key.TenantId, p.goal, p.key.JobId, "", "preparation dependencies are not valid JSON")
			}
		}
		s.preparations = append(s.preparations, p)
		return nil
	}); err != nil {
		return nil, err
	}
	var receipts int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='jobdb_submission_receipts'").Scan(&receipts); err != nil {
		return nil, err
	}
	if receipts > 0 {
		if err := migrationRows(ctx, tx, `SELECT tenant_id,job_id,goal_id,descriptor,requested_cell,cell_id,cell_name,cell_repo FROM jobdb_submission_receipts ORDER BY tenant_id,job_id`, func(rows *sql.Rows) error {
			var r migrationReceipt
			if err := rows.Scan(&r.key.TenantId, &r.key.JobId, &r.goal, &r.descriptor, &r.requested, &r.cell, &r.name, &r.repo); err != nil {
				return err
			}
			s.receipts[r.key] = r
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *goalMigrationInput) owner(tenant, cell, name, repo string) bool {
	_, err := jobdb.NormalizeCellRepo(repo)
	return err == nil && utf8.ValidString(tenant) && utf8.ValidString(cell) && utf8.ValidString(name) && utf8.ValidString(repo) && s.cells[jobdb.CellKey{TenantID: tenant, CellID: cell}] && strings.TrimSpace(name) != "" && strings.TrimSpace(repo) != ""
}
func (j migrationJob) success() bool {
	return j.archived.Valid && (j.status == "success" || j.status == "succeeded")
}

func (s *goalMigrationInput) validate(r *GoalMigrationReport) {
	tenants := map[string]*GoalMigrationTenant{}
	tenant := func(id string) *GoalMigrationTenant {
		if tenants[id] == nil {
			tenants[id] = &GoalMigrationTenant{TenantID: id}
		}
		return tenants[id]
	}
	primary := map[jobdb.GoalKey][]migrationJob{}
	members := map[migrationCandidate]bool{} // added time is irrelevant to membership below
	for _, key := range s.jobOrder {
		j := s.jobs[key]
		t := tenant(key.TenantId)
		t.Jobs++
		if j.hidden.Valid {
			t.HiddenJobs++
		}
		gk := jobdb.GoalKey{TenantId: key.TenantId, GoalID: j.goal}
		if key.Validate() != nil || gk.Validate() != nil || !s.owner(key.TenantId, j.cell, j.name, j.repo) {
			r.issue("invalid_job_identity", key.TenantId, j.goal, key.JobId, "", "job has invalid identity or captured ownership")
		}
		if _, ok := s.goals[gk]; !ok {
			r.issue("missing_primary_goal", key.TenantId, j.goal, key.JobId, "", "primary goal has no retained goal row")
		}
		primary[gk] = append(primary[gk], j)
		members[migrationCandidate{key: gk, job: key.JobId}] = true
	}
	lost := map[jobdb.GoalKey][]string{}
	for _, c := range s.candidates {
		j, ok := s.jobs[jobdb.JobKey{TenantId: c.key.TenantId, JobId: c.job}]
		if !ok {
			r.issue("missing_candidate_job", c.key.TenantId, c.key.GoalID, c.job, "", "candidate references a missing retained job")
			continue
		}
		if _, ok := s.goals[c.key]; !ok {
			r.issue("missing_candidate_goal", c.key.TenantId, c.key.GoalID, c.job, "", "candidate references a missing goal")
		}
		members[migrationCandidate{key: c.key, job: c.job}] = true
		if j.goal == c.key.GoalID {
			r.notice("redundant_candidate", c.key.TenantId, c.key.GoalID, c.job, "", "redundant primary candidate timestamp is dropped")
			continue
		}
		lost[c.key] = append(lost[c.key], c.job)
		tenant(c.key.TenantId).DroppedMemberships++
		r.DroppedMemberships = append(r.DroppedMemberships, GoalMigrationMembership{GoalKey: c.key, JobID: c.job, PrimaryGoalID: j.goal, AddedAt: timeFromNS(c.added)})
		r.notice("dropped_secondary_membership", c.key.TenantId, c.key.GoalID, c.job, "", "primary membership is retained; secondary completion effects and candidate timestamp are dropped")
	}
	for _, key := range s.goalOrder {
		g := s.goals[key]
		t := tenant(key.TenantId)
		t.Goals++
		if key.Validate() != nil || !s.owner(key.TenantId, g.cell, g.name, g.repo) {
			r.issue("invalid_goal_ownership", key.TenantId, key.GoalID, "", "", "goal has invalid identity or captured ownership")
		}
		if string(g.info.State) != g.state || g.info.CreatedAt.IsZero() || g.info.CreatedAt.UnixNano() != g.created || g.info.UpdatedAt.IsZero() || g.info.UpdatedAt.UnixNano() != g.updated || g.info.Revision < 1 {
			r.issue("goal_columns_mismatch", key.TenantId, key.GoalID, "", "", "goal state, timestamps or revision disagree with indexed columns")
		}
		js := primary[key]
		sort.Slice(js, func(i, j int) bool {
			if js[i].created != js[j].created {
				return js[i].created < js[j].created
			}
			return js[i].key.JobId < js[j].key.JobId
		})
		plan := GoalMigrationPlan{GoalKey: key, State: g.info.State, SuccessfulJobID: g.info.SuccessfulJobID, ClosedAt: g.info.ClosedAt, OriginalCreatedAt: g.info.CreatedAt, OriginalUpdatedAt: g.info.UpdatedAt, NonTerminalJobIDs: []string{}}
		for _, j := range js {
			if plan.OriginJobID == "" && j.cell == g.cell && j.repo == g.repo {
				plan.OriginJobID = j.key.JobId
				plan.DerivedCreatedAt = timeFromNS(j.created)
			}
			if !j.archived.Valid {
				plan.NonTerminalJobIDs = append(plan.NonTerminalJobIDs, j.key.JobId)
			}
		}
		sort.Strings(plan.NonTerminalJobIDs)
		if plan.OriginJobID == "" {
			r.issue("missing_compatible_origin", key.TenantId, key.GoalID, "", "", "no ownership-compatible primary job can anchor this goal")
		}
		if len(plan.NonTerminalJobIDs) > 1 {
			t.LiveOverlaps++
			r.notice("live_overlap", key.TenantId, key.GoalID, "", "", "existing active members may convert, but every fresh admission must wait until all are terminal")
		}
		plan.DerivedUpdatedAt = plan.DerivedCreatedAt
		if g.info.State == jobdb.GoalOpen {
			if g.info.SuccessfulJobID != "" || g.info.Failure != nil || g.info.ClosedAt != nil || g.info.FailureJobID != "" {
				r.issue("invalid_open_goal", key.TenantId, key.GoalID, "", "", "open goal contains closure facts")
			}
		} else {
			if g.info.ClosedAt == nil || g.info.ClosedAt.IsZero() {
				r.issue("missing_closure_time", key.TenantId, key.GoalID, "", "", "closed goal has no saved closure time")
			} else {
				plan.DerivedUpdatedAt = *g.info.ClosedAt
			}
			switch g.info.State {
			case jobdb.GoalSucceeded:
				winner, ok := s.jobs[jobdb.JobKey{TenantId: key.TenantId, JobId: g.info.SuccessfulJobID}]
				if !ok || !winner.success() || !members[migrationCandidate{key: key, job: g.info.SuccessfulJobID}] || g.info.Failure != nil || g.info.FailureJobID != "" {
					r.issue("invalid_success_witness", key.TenantId, key.GoalID, g.info.SuccessfulJobID, "", "saved success lacks a retained successful member witness")
				}
				plan.ClosureJobID = winner.key.JobId
				if ok && winner.goal != key.GoalID {
					plan.ClosureJobID = plan.OriginJobID
					r.notice("cross_primary_winner", key.TenantId, key.GoalID, winner.key.JobId, "", "retain exact result producer; relocate closure reference to the target origin without moving primary membership")
				}
			case jobdb.GoalFailed:
				plan.CreateFailureRecord = true
				if !s.validFailure(key, g.info.Failure) || g.info.SuccessfulJobID != "" || g.info.FailureJobID != "" || g.info.ClosedAt == nil || (g.info.Failure != nil && !g.info.Failure.FailedAt.Equal(*g.info.ClosedAt)) {
					r.issue("invalid_failure_snapshot", key.TenantId, key.GoalID, "", "", "explicit failure has invalid message, time, identity or historical member references")
				}
				r.notice("migrated_failure_record", key.TenantId, key.GoalID, "", "", "conversion must allocate one migrated terminal record and preserve the saved failure snapshot; checking allocates no ID")
			default:
				r.issue("unknown_goal_state", key.TenantId, key.GoalID, "", "", "unrecognized saved goal state")
			}
		}
		r.notice("derived_discovery_metadata", key.TenantId, key.GoalID, plan.OriginJobID, "", "discovery times derive from retained origin and saved closure; revision becomes 1 open or 2 closed")
		r.Goals = append(r.Goals, plan)
	}
	s.validateWaits(r, lost, tenant)
	s.validatePreparations(r, lost, tenant)
	ids := make([]string, 0, len(tenants))
	for id := range tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r.Tenants = append(r.Tenants, *tenants[id])
	}
}

func (s *goalMigrationInput) validFailure(key jobdb.GoalKey, f *jobdb.GoalFailure) bool {
	if f == nil || f.Code != "goal_failed" || f.GoalID != key.GoalID || f.FailedAt.IsZero() || (jobdb.FailGoalRequest{GoalKey: key, Message: f.Message}).Validate() != nil {
		return false
	}
	seen := map[string]bool{}
	for _, ids := range [][]string{f.TerminalJobIDs, f.NonTerminalJobIDs} {
		for _, id := range ids {
			if strings.TrimSpace(id) == "" || seen[id] {
				return false
			}
			seen[id] = true
			if _, ok := s.jobs[jobdb.JobKey{TenantId: key.TenantId, JobId: id}]; !ok {
				return false
			}
		}
	}
	return true
}

func (s *goalMigrationInput) validateWaits(r *GoalMigrationReport, lost map[jobdb.GoalKey][]string, tenant func(string) *GoalMigrationTenant) {
	expected := map[migrationTarget]bool{}
	waits := map[jobdb.DependencyWaitKey]bool{}
	for _, row := range s.waits {
		w := row.info
		key := row.key
		waits[key] = true
		bad := func(code, message string) {
			r.issue(code, key.JobKey.TenantId, "", key.JobKey.JobId, key.WaitID, message)
		}
		if _, ok := s.jobs[key.JobKey]; !ok {
			bad("missing_wait_consumer", "wait references a missing retained consumer")
		}
		if w.JobKey != key.JobKey || w.WaitID != key.WaitID || string(w.State) != row.state || key.WaitID == "" || len(w.Dependencies) == 0 {
			bad("wait_identity_mismatch", "wait payload identity/state disagrees with retained columns")
		}
		switch w.State {
		case jobdb.DependencyPending, jobdb.DependencySatisfied, jobdb.DependencyFailed, jobdb.DependencyTimedOut, jobdb.DependencyCancelled:
		default:
			bad("invalid_wait_state", "wait has an unrecognized state")
		}
		deps := make([]jobdb.Dependency, 0, len(w.Dependencies))
		affected := false
		pending := 0
		for _, d := range w.Dependencies {
			deps = append(deps, jobdb.Dependency{Target: d.Target, Condition: d.Condition})
			if d.Target.GoalID != "" {
				gk := jobdb.GoalKey{TenantId: key.JobKey.TenantId, GoalID: d.Target.GoalID}
				if _, ok := s.goals[gk]; !ok {
					bad("missing_wait_target", "wait references a missing goal")
				}
				if w.State == jobdb.DependencyPending && d.State == jobdb.DependencyPending && len(lost[gk]) > 0 {
					affected = true
					r.notice("pending_wait_loses_secondary", key.JobKey.TenantId, gk.GoalID, key.JobKey.JobId, key.WaitID, "pending target retains its goal ID; dropped secondary producers no longer complete it")
				}
			} else {
				if _, ok := s.jobs[jobdb.JobKey{TenantId: key.JobKey.TenantId, JobId: d.Target.JobID}]; !ok {
					bad("missing_wait_target", "wait references a missing exact job")
				}
			}
			switch d.State {
			case jobdb.DependencyPending, jobdb.DependencySatisfied, jobdb.DependencyFailed, jobdb.DependencyTimedOut, jobdb.DependencyCancelled:
			default:
				bad("invalid_dependency_state", "dependency has an unrecognized state")
			}
			if d.State == jobdb.DependencySatisfied {
				j, ok := s.jobs[jobdb.JobKey{TenantId: key.JobKey.TenantId, JobId: d.SatisfiedByJobID}]
				if !ok || !j.archived.Valid || (d.Condition == jobdb.JobPrereqSuccess && !j.success()) || (d.Target.JobID != "" && d.SatisfiedByJobID != d.Target.JobID) {
					bad("invalid_settled_producer", "frozen binding lacks its exact retained terminal producer")
				}
				// A historical goal producer need not share its target's primary.
				if d.Target.GoalID != "" {
					g := s.goals[jobdb.GoalKey{TenantId: key.JobKey.TenantId, GoalID: d.Target.GoalID}]
					if g.info.State != jobdb.GoalSucceeded || g.info.SuccessfulJobID != d.SatisfiedByJobID {
						bad("invalid_settled_goal_witness", "frozen goal binding differs from its saved successful witness")
					}
				}
			}
			if d.State == jobdb.DependencyPending {
				pending++
			}
			if (w.State == jobdb.DependencyPending && d.State != jobdb.DependencyPending && d.State != jobdb.DependencySatisfied) || (w.State == jobdb.DependencySatisfied && d.State != jobdb.DependencySatisfied) {
				bad("inconsistent_wait_outcome", "wait state contradicts its frozen dependency states")
			}
			if d.State == jobdb.DependencyFailed && (d.Failure == nil || strings.TrimSpace(d.Failure.Message) == "" || (d.Target.GoalID != "" && !s.validFailure(jobdb.GoalKey{TenantId: key.JobKey.TenantId, GoalID: d.Target.GoalID}, d.Failure)) || (d.Target.JobID != "" && (d.Failure.Code != "job_dependency_failed" || d.Failure.JobID != d.Target.JobID))) {
				bad("invalid_dependency_failure", "failed dependency lacks its frozen typed target failure")
			}
			if w.State == jobdb.DependencyPending && d.State == jobdb.DependencyPending {
				kind, id := targetParts(d.Target)
				expected[migrationTarget{key: key, kind: kind, id: id}] = true
			}
		}
		if affected {
			tenant(key.JobKey.TenantId).PendingWaitsAffected++
		}
		if err := jobdb.ValidateDependencies(deps); err != nil {
			bad("invalid_wait_dependencies", "wait has invalid dependency descriptors")
		}
		if w.FailedDependencyIndex != nil && (*w.FailedDependencyIndex < 0 || *w.FailedDependencyIndex >= len(w.Dependencies)) {
			bad("invalid_failed_dependency", "failed dependency index is out of range")
		}
		if w.State == jobdb.DependencyFailed && w.Failure == nil {
			bad("missing_wait_failure", "failed wait lacks its frozen failure")
		}
		if (w.State == jobdb.DependencyPending && pending == 0) || (w.State != jobdb.DependencyPending && pending != 0) || ((w.State == jobdb.DependencyPending || w.State == jobdb.DependencySatisfied) && (w.Failure != nil || w.FailedDependencyIndex != nil)) {
			bad("inconsistent_wait_outcome", "wait has contradictory pending or closure facts")
		}
	}
	for t := range expected {
		if !s.targets[t] {
			r.issue("missing_target_index", t.key.JobKey.TenantId, "", t.key.JobKey.JobId, t.key.WaitID, "pending wait target is missing from the subscription index")
		}
	}
	for t := range s.targets {
		if !expected[t] {
			r.issue("stale_target_index", t.key.JobKey.TenantId, "", t.key.JobKey.JobId, t.key.WaitID, "subscription index does not match a pending dependency")
		}
	}
	for _, key := range s.jobOrder {
		j := s.jobs[key]
		if j.wait != "" && !waits[jobdb.DependencyWaitKey{JobKey: key, WaitID: j.wait}] {
			r.issue("missing_active_wait", key.TenantId, j.goal, key.JobId, j.wait, "job's current wait ID has no retained wait")
		}
	}
}

func migrationDescriptor(raw string) (string, string, jobdb.SubmitJob, jobdb.SubmitRestartJob, error) {
	var d struct {
		Kind, Parent, ID string
		Job              json.RawMessage
	}
	var submit jobdb.SubmitJob
	var restart jobdb.SubmitRestartJob
	if !utf8.ValidString(raw) {
		return "", "", submit, restart, fmt.Errorf("descriptor contains invalid UTF-8")
	}
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		return "", "", submit, restart, err
	}
	switch d.Kind {
	case "submit":
		if err := json.Unmarshal(d.Job, &submit); err != nil {
			return "", "", submit, restart, err
		}
		if submit.TenantId == "" || submit.JobType == "" {
			return "", "", submit, restart, fmt.Errorf("incomplete submit descriptor")
		}
	case "start-or-append":
		if d.ID == "" || jobdb.ValidateIdentifier(d.ID) != nil {
			return "", "", submit, restart, fmt.Errorf("incomplete append descriptor")
		}
		submit.JobID = d.ID
	case "restart":
		if err := json.Unmarshal(d.Job, &restart); err != nil {
			return "", "", submit, restart, err
		}
		if restart.PriorJobKey.Validate() != nil {
			return "", "", submit, restart, fmt.Errorf("incomplete restart descriptor")
		}
	default:
		return "", "", submit, restart, fmt.Errorf("unknown descriptor kind")
	}
	return d.Kind, d.Parent, submit, restart, nil
}

func (s *goalMigrationInput) validatePreparations(r *GoalMigrationReport, lost map[jobdb.GoalKey][]string, tenant func(string) *GoalMigrationTenant) {
	for _, p := range s.preparations {
		bad := func(code, message string) { r.issue(code, p.key.TenantId, p.goal, p.key.JobId, "", message) }
		if p.key.Validate() != nil || strings.TrimSpace(p.goal) == "" || !s.owner(p.key.TenantId, p.cell, p.name, p.repo) {
			bad("invalid_preparation_identity", "preparation has invalid identity or captured ownership")
		}
		j, published := s.jobs[p.key]
		if published && (j.goal != p.goal || j.cell != p.cell || j.repo != p.repo) {
			bad("preparation_identity_mismatch", "preparation differs from its retained published identity")
		}
		gk := jobdb.GoalKey{TenantId: p.key.TenantId, GoalID: p.goal}
		if p.shared {
			if _, ok := s.goals[gk]; !ok {
				bad("missing_preparation_goal", "shared preparation references a missing published goal")
			}
		}
		kind, parent, submit, restart, err := migrationDescriptor(p.descriptor)
		if err != nil || parent != p.parent || (kind == "submit" && (submit.TenantId != p.key.TenantId || (submit.GoalID != "" && submit.GoalID != p.goal) || (submit.GoalID != "") != p.shared)) || (kind == "restart" && (restart.PriorJobKey.TenantId != p.key.TenantId || restart.PriorJobKey.JobId != p.prior || restart.ShareGoal != p.shared)) {
			bad("invalid_preparation_descriptor", "saved descriptor conflicts with captured preparation identity")
		}
		if kind == "start-or-append" && (submit.JobID != p.key.JobId || p.shared || p.prior != "" || p.parent != "" || p.request != "" || (published && (j.appendKey == "" || j.appendGeneration < 1))) {
			bad("invalid_preparation_descriptor", "append descriptor conflicts with captured identity or generation")
		}
		descriptorDeps := submit.Dependencies
		if kind == "restart" {
			descriptorDeps = restart.Dependencies
		}
		depsMatch := len(descriptorDeps) == len(p.dependencies)
		if depsMatch {
			for i, d := range descriptorDeps {
				if d != p.dependencies[i] {
					depsMatch = false
					break
				}
			}
		}
		if !depsMatch {
			bad("preparation_dependencies_mismatch", "captured preparation dependencies differ from its accepted descriptor")
		}
		if !p.shared && !published {
			if _, exists := s.goals[gk]; exists {
				bad("preparation_goal_collision", "unpublished new-goal preparation collides with a published goal")
			}
		}
		if kind == "submit" && submit.GoalID != "" && published {
			receipt, ok := s.receipts[p.key]
			if !ok || receipt.descriptor != p.descriptor {
				bad("missing_accepted_receipt", "published selected submission lacks its exact accepted receipt")
			}
		}
		if p.prior != "" {
			prior, ok := s.jobs[jobdb.JobKey{TenantId: p.key.TenantId, JobId: p.prior}]
			if !ok {
				bad("missing_restart_source", "restart preparation references a missing prior job")
			}
			if ok && p.shared && prior.goal != p.goal {
				bad("restart_goal_mismatch", "shared restart changes its source primary goal")
			}
		}
		if err := jobdb.ValidateDependencies(p.dependencies); err != nil {
			bad("invalid_preparation_dependencies", "preparation has invalid dependency descriptors")
		}
		affected := !published && len(lost[gk]) > 0
		for _, d := range p.dependencies {
			if d.Target.GoalID != "" {
				key := jobdb.GoalKey{TenantId: p.key.TenantId, GoalID: d.Target.GoalID}
				if _, ok := s.goals[key]; !ok {
					bad("missing_preparation_target", "preparation references a missing dependency goal")
				}
				if !published && len(lost[key]) > 0 {
					affected = true
				}
			} else {
				if _, ok := s.jobs[jobdb.JobKey{TenantId: p.key.TenantId, JobId: d.Target.JobID}]; !ok {
					bad("missing_preparation_target", "preparation references a missing dependency job")
				}
			}
		}
		if affected {
			tenant(p.key.TenantId).PreparationsAffected++
			r.notice("preparation_loses_secondary", p.key.TenantId, p.goal, p.key.JobId, "", "unpublished preparation retains its descriptor and targets despite dropped secondary producers")
		}
	}
	for key, receipt := range s.receipts {
		j, ok := s.jobs[key]
		kind, _, submit, _, err := migrationDescriptor(receipt.descriptor)
		if !ok || !s.owner(key.TenantId, receipt.cell, receipt.name, receipt.repo) || receipt.requested == "" || j.goal != receipt.goal || j.cell != receipt.cell || j.name != receipt.name || j.repo != receipt.repo || err != nil || kind != "submit" || submit.TenantId != key.TenantId || submit.GoalID != receipt.goal {
			r.issue("incompatible_receipt", key.TenantId, receipt.goal, key.JobId, "", "accepted selected receipt lacks its exact compatible retained identity/descriptor")
		}
	}
}
