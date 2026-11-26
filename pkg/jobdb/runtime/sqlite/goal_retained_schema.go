package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// These historical definitions are independent of the live schema. Keep them
// frozen when format 13 is adopted; checker support for 11/12 must not drift.
//
//go:embed schema/legacy-11.sql
var legacyGoalSchemaSQL string

//go:embed schema/legacy-lease-events.sql
var legacyLeaseEventSchemaSQL string

//go:embed schema/legacy-receipts.sql
var legacyReceiptSchemaSQL string

// Fresh runtime initialization and exclusive manual conversion share this
// retained-job schema. Existing format-13 storage is validated without repairs.
//
//go:embed schema/format-13.sql
var retainedGoalSchemaSQL string

func createRetainedGoalSchema(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, retainedGoalSchemaSQL+leaseEventSchemaSQL+submissionReceiptSchemaSQL)
	return err
}

// validateRetainedGoalSchema verifies definitions without installing objects or
// advancing the format marker. Conversion can call it before its final marker
// change; runtime opening can use it without repairing malformed storage.
func validateRetainedGoalSchema(ctx context.Context, tx *sql.Tx) error {
	return validateRetainedSchemaForFormat(ctx, tx, 13)
}

func validateRetainedSchemaForFormat(ctx context.Context, tx *sql.Tx, version int) error {
	expected, err := sql.Open(sqliteDriverName, ":memory:")
	if err != nil {
		return err
	}
	defer expected.Close()
	if _, err = expected.ExecContext(ctx, retainedGoalSchemaSQL+leaseEventSchemaSQL+submissionReceiptSchemaSQL); err != nil {
		return err
	}
	if version >= 14 {
		if _, err = expected.ExecContext(ctx, resourceSchemaSQL); err != nil {
			return err
		}
	}
	if version >= 15 {
		if _, err = expected.ExecContext(ctx, leaseUpdateSchemaSQL); err != nil {
			return err
		}
	}
	if version >= 16 {
		if _, err = expected.ExecContext(ctx, cellArchiveSchemaSQL); err != nil {
			return err
		}
	}
	rows, err := expected.QueryContext(ctx, `SELECT name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, want, got string
		if err = rows.Scan(&name, &want); err != nil {
			return err
		}
		if err = tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE name=?`, name).Scan(&got); err != nil {
			return fmt.Errorf("SQLite format %d: inconsistent schema %s: %w", version, name, err)
		}
		normalize := func(s string) string {
			s = strings.Join(strings.Fields(strings.ReplaceAll(s, "IF NOT EXISTS ", "")), " ")
			return s
		}
		if normalize(got) != normalize(want) {
			return fmt.Errorf("SQLite format %d: inconsistent schema %s", version, name)
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	var legacy int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name IN ('jobdb_goals','jobdb_goal_candidates')`).Scan(&legacy); err != nil {
		return err
	}
	if legacy != 0 {
		return fmt.Errorf("SQLite format %d: legacy goal storage remains", version)
	}
	return nil
}

// verifyRetainedGoalFacts checks the facts persisted by the transition store,
// including origins for open recovery gaps and cross-primary result witnesses.
// It needs no chapter/configuration/secret service and does not change rows.
func verifyRetainedGoalFacts(ctx context.Context, tx *sql.Tx) error {
	var missingTenant, missingGoal string
	err := tx.QueryRowContext(ctx, `SELECT tenant_id,goal_id FROM jobdb_jobs GROUP BY tenant_id,goal_id HAVING sum(goal_origin)<>1 ORDER BY tenant_id,goal_id LIMIT 1`).Scan(&missingTenant, &missingGoal)
	if err == nil {
		return fmt.Errorf("%w: missing or multiple origins for tenant %q goal %q", jobdb.ErrInvalidJobRecord, missingTenant, missingGoal)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var unclosed string
	err = tx.QueryRowContext(ctx, `SELECT job_id FROM jobdb_jobs WHERE record_kind<>'EXECUTION' AND goal_closure_state IS NULL ORDER BY tenant_id,job_id LIMIT 1`).Scan(&unclosed)
	if err == nil {
		return fmt.Errorf("%w: protected failure %q has no closure", jobdb.ErrInvalidJobRecord, unclosed)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT tenant_id,goal_id FROM jobdb_jobs WHERE goal_origin=1 ORDER BY tenant_id,goal_id`)
	if err != nil {
		return err
	}
	keys := []jobdb.GoalKey{}
	for rows.Next() {
		var key jobdb.GoalKey
		if err = rows.Scan(&key.TenantId, &key.GoalID); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	store := retainedStore(ctx, tx)
	for _, key := range keys {
		if _, err = store.Goal(key); err != nil {
			return fmt.Errorf("tenant %q goal %q: %w", key.TenantId, key.GoalID, err)
		}
	}
	return nil
}
