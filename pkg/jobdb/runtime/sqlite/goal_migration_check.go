package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ErrGoalMigrationBlocked means inspection completed but the source cannot be
// converted safely. Execution errors (including unreadable files) are distinct.
var ErrGoalMigrationBlocked = errors.New("SQLite goal migration blocked")

// GoalMigrationIssue describes a source fact requiring repair or operator notice.
type GoalMigrationIssue struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	TenantID string `json:"tenantId,omitempty"`
	GoalID   string `json:"goalId,omitempty"`
	JobID    string `json:"jobId,omitempty"`
	WaitID   string `json:"waitId,omitempty"`
	Message  string `json:"message"`
}

// GoalMigrationTenant counts retained source facts, including hidden jobs.
type GoalMigrationTenant struct {
	TenantID             string `json:"tenantId"`
	Jobs                 int    `json:"jobs"`
	HiddenJobs           int    `json:"hiddenJobs"`
	Goals                int    `json:"goals"`
	DroppedMemberships   int    `json:"droppedMemberships"`
	PendingWaitsAffected int    `json:"pendingWaitsAffected"`
	PreparationsAffected int    `json:"preparationsAffected"`
	LiveOverlaps         int    `json:"liveOverlaps"`
}

// GoalMigrationPlan identifies retained origin/closure witnesses. Failure IDs
// are allocated only by conversion; checking never reserves or publishes jobs.
type GoalMigrationPlan struct {
	GoalKey             jobdb.GoalKey   `json:"goalKey"`
	OriginJobID         string          `json:"originJobId"`
	State               jobdb.GoalState `json:"state"`
	SuccessfulJobID     string          `json:"successfulJobId,omitempty"`
	ClosureJobID        string          `json:"closureJobId,omitempty"`
	CreateFailureRecord bool            `json:"createFailureRecord"`
	ClosedAt            *time.Time      `json:"closedAt,omitempty"`
	OriginalCreatedAt   time.Time       `json:"originalCreatedAt"`
	DerivedCreatedAt    time.Time       `json:"derivedCreatedAt"`
	OriginalUpdatedAt   time.Time       `json:"originalUpdatedAt"`
	DerivedUpdatedAt    time.Time       `json:"derivedUpdatedAt"`
	NonTerminalJobIDs   []string        `json:"nonTerminalJobIds"`
}

// GoalMigrationMembership is a removed secondary relationship, not a reassigned
// primary. Its original timestamp is reported even though conversion drops it.
type GoalMigrationMembership struct {
	GoalKey       jobdb.GoalKey `json:"goalKey"`
	JobID         string        `json:"jobId"`
	PrimaryGoalID string        `json:"primaryGoalId"`
	AddedAt       time.Time     `json:"addedAt"`
}

// GoalMigrationReport is a read-only preflight result, not evidence that a
// conversion committed or that incompatible writers/overlaps have drained.
type GoalMigrationReport struct {
	Path               string                    `json:"path"`
	SourceFormat       int                       `json:"sourceFormat"`
	TargetFormat       int                       `json:"targetFormat"`
	Verdict            string                    `json:"verdict"`
	Tenants            []GoalMigrationTenant     `json:"tenants"`
	Goals              []GoalMigrationPlan       `json:"goals"`
	DroppedMemberships []GoalMigrationMembership `json:"droppedMemberships"`
	Issues             []GoalMigrationIssue      `json:"issues"`
}

// CheckGoalMigration inspects an existing literal filesystem path in one
// read-only SQLite transaction. It never calls runtime constructors, creates
// ancillary stores, normalizes a format marker or converts the source.
// A blocked report accompanies ErrGoalMigrationBlocked. Other errors accompany
// an ERROR verdict and must not be interpreted as a convertible source.
func CheckGoalMigration(ctx context.Context, path string) (report GoalMigrationReport, err error) {
	report = newGoalMigrationReport()
	defer func() {
		if err != nil && !errors.Is(err, ErrGoalMigrationBlocked) {
			report.Verdict = "ERROR"
			report.Issues = append(report.Issues, GoalMigrationIssue{Severity: "error", Code: "check_failed", Message: err.Error()})
		}
	}()
	if strings.TrimSpace(path) == "" {
		return report, fmt.Errorf("SQLite migration check requires an explicit database path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return report, err
	}
	report.Path = abs
	info, err := os.Stat(abs)
	if err != nil {
		return report, fmt.Errorf("SQLite migration check: %w", err)
	}
	if !info.Mode().IsRegular() {
		return report, fmt.Errorf("SQLite migration check requires a regular database file: %s", abs)
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := url.Values{"mode": {"ro"}, "_pragma": {"query_only(1)", "busy_timeout(5000)"}}
	u.RawQuery = q.Encode()
	db, err := sql.Open(sqliteDriverName, u.String())
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx = ctxOrBackground(ctx)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	// The manual converter must call this same inspector after acquiring its
	// own conversion lock. A previously returned report is never that decision.
	err = inspectGoalMigration(ctx, tx, &report)
	return report, err
}

func newGoalMigrationReport() GoalMigrationReport {
	return GoalMigrationReport{TargetFormat: 13, Verdict: "BLOCKED", Tenants: []GoalMigrationTenant{}, Goals: []GoalMigrationPlan{}, DroppedMemberships: []GoalMigrationMembership{}, Issues: []GoalMigrationIssue{}}
}

func (r *GoalMigrationReport) issue(code, tenant, goal, job, wait, message string) {
	r.Issues = append(r.Issues, GoalMigrationIssue{Severity: "error", Code: code, TenantID: tenant, GoalID: goal, JobID: job, WaitID: wait, Message: message})
}
func (r *GoalMigrationReport) notice(code, tenant, goal, job, wait, message string) {
	r.Issues = append(r.Issues, GoalMigrationIssue{Severity: "notice", Code: code, TenantID: tenant, GoalID: goal, JobID: job, WaitID: wait, Message: message})
}
func (r *GoalMigrationReport) finish() error {
	sort.Slice(r.Issues, func(i, j int) bool {
		a, b := r.Issues[i], r.Issues[j]
		return strings.Join([]string{a.TenantID, a.GoalID, a.JobID, a.WaitID, a.Code, a.Message}, "\x00") < strings.Join([]string{b.TenantID, b.GoalID, b.JobID, b.WaitID, b.Code, b.Message}, "\x00")
	})
	for _, issue := range r.Issues {
		if issue.Severity == "error" {
			r.Verdict = "BLOCKED"
			return ErrGoalMigrationBlocked
		}
	}
	r.Verdict = "CONVERTIBLE"
	if len(r.Issues) > 0 {
		r.Verdict = "CONVERTIBLE_WITH_NOTICES"
	}
	return nil
}

func inspectGoalMigration(ctx context.Context, tx *sql.Tx, report *GoalMigrationReport) error {
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&report.SourceFormat); err != nil {
		return err
	}
	var tables int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&tables); err != nil {
		return err
	}
	if report.SourceFormat == 0 && tables == 0 {
		report.Verdict = "EMPTY"
		return nil
	}
	if report.SourceFormat == 13 {
		if err := validateRetainedGoalSchema(ctx, tx); err != nil {
			report.issue("inconsistent_schema", "", "", "", "", err.Error())
			return report.finish()
		}
		if err := checkMigrationIntegrity(ctx, tx, report); err != nil {
			return err
		}
		if err := verifyRetainedGoalFacts(ctx, tx); err != nil {
			report.issue("invalid_retained_facts", "", "", "", "", err.Error())
		}
		if err := report.finish(); err != nil {
			return err
		}
		report.Verdict = "CURRENT"
		return nil
	}
	if report.SourceFormat != 11 && report.SourceFormat != 12 {
		report.issue("unsupported_format", "", "", "", "", fmt.Sprintf("format %d is not a supported legacy format (11 or 12)", report.SourceFormat))
		return report.finish()
	}
	if err := validateLeaseEventBaseSchema(ctx, tx, report.SourceFormat == 12); err != nil {
		report.issue("inconsistent_schema", "", "", "", "", err.Error())
		return report.finish()
	}
	if err := checkMigrationOptionalSchema(ctx, tx, report); err != nil {
		return err
	}
	if err := checkMigrationIntegrity(ctx, tx, report); err != nil {
		return err
	}
	for _, issue := range report.Issues {
		if issue.Severity == "error" {
			return report.finish()
		}
	}
	input, err := loadGoalMigrationInput(ctx, tx, report)
	if err != nil {
		return err
	}
	input.validate(report)
	return report.finish()
}

func checkMigrationOptionalSchema(ctx context.Context, tx *sql.Tx, report *GoalMigrationReport) error {
	var kind, ddl string
	err := tx.QueryRowContext(ctx, "SELECT type,sql FROM sqlite_master WHERE name='jobdb_submission_receipts'").Scan(&kind, &ddl)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		normalize := func(s string) string {
			return strings.TrimSuffix(strings.Join(strings.Fields(strings.ReplaceAll(s, "IF NOT EXISTS ", "")), " "), ";")
		}
		if kind != "table" || normalize(ddl) != normalize(legacyReceiptSchemaSQL) {
			report.issue("inconsistent_schema", "", "", "", "", "inconsistent schema jobdb_submission_receipts")
		}
	}
	// Unknown views/triggers/foreign keys into removed tables cannot silently
	// survive conversion as broken references. Known legacy objects are removed
	// by the converter; unrelated application/chapter/config tables are retained.
	rows, err := tx.QueryContext(ctx, "SELECT name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, ddl string
		if err = rows.Scan(&name, &ddl); err != nil {
			return err
		}
		if name == "jobdb_goals" || name == "jobdb_goal_candidates" || name == "jobdb_goals_cell_idx" || name == "jobdb_goals_state_idx" || name == "jobdb_goals_created_idx" || name == "jobdb_goals_updated_idx" || name == "jobdb_goal_candidates_job_idx" {
			continue
		}
		if migrationSQLMentionsGoalTable(ddl) {
			report.issue("unsupported_schema_reference", "", "", "", "", "object "+name+" references a removed goal table")
		}
	}
	return rows.Err()
}

// Treat unknown dependencies conservatively, but do not mistake SQL string
// values, comments or longer application identifiers for legacy table names.
func migrationSQLMentionsGoalTable(ddl string) bool {
	legacy := func(word string) bool {
		return strings.EqualFold(word, "jobdb_goals") || strings.EqualFold(word, "jobdb_goal_candidates")
	}
	for i := 0; i < len(ddl); {
		if strings.HasPrefix(ddl[i:], "--") {
			end := strings.IndexByte(ddl[i:], '\n')
			if end < 0 {
				return false
			}
			i += end + 1
			continue
		}
		if strings.HasPrefix(ddl[i:], "/*") {
			end := strings.Index(ddl[i+2:], "*/")
			if end < 0 {
				return false
			}
			i += end + 4
			continue
		}
		if strings.ContainsRune("'\"`[", rune(ddl[i])) {
			quote := ddl[i]
			endQuote := quote
			if quote == '[' {
				endQuote = ']'
			}
			i++
			var token strings.Builder
			for i < len(ddl) {
				c := ddl[i]
				i++
				if c == endQuote {
					if i < len(ddl) && ddl[i] == endQuote && quote != '[' {
						token.WriteByte(c)
						i++
						continue
					}
					break
				}
				token.WriteByte(c)
			}
			if quote != '\'' && legacy(token.String()) {
				return true
			}
			continue
		}
		if strings.ContainsRune(" \t\r\n(),.;=+-*/<>!|&%", rune(ddl[i])) {
			i++
			continue
		}
		start := i
		for i < len(ddl) && !strings.ContainsRune(" \t\r\n(),.;=+-*/<>!|&%'\"`[", rune(ddl[i])) {
			i++
		}
		if legacy(ddl[start:i]) {
			return true
		}
	}
	return false
}

func checkMigrationIntegrity(ctx context.Context, tx *sql.Tx, report *GoalMigrationReport) error {
	rows, err := tx.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return err
	}
	for rows.Next() {
		var result string
		if err = rows.Scan(&result); err != nil {
			rows.Close()
			return err
		}
		if result != "ok" {
			report.issue("integrity_check", "", "", "", "", result)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = tx.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var table, parent string
		var row sql.NullInt64
		var index int
		if err = rows.Scan(&table, &row, &parent, &index); err != nil {
			return err
		}
		report.issue("foreign_key_check", "", "", "", "", fmt.Sprintf("table %s row %v references missing %s (constraint %d)", table, row, parent, index))
	}
	return rows.Err()
}
