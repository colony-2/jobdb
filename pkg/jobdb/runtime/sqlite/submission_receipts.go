package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/goalstate"
)

// Kept independently of jobs and preparations, without cascading foreign keys:
// deleting public work or staging must never make a destination reusable.
const submissionReceiptSchemaSQL = `CREATE TABLE IF NOT EXISTS jobdb_submission_receipts (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL,
 descriptor BLOB NOT NULL, requested_cell TEXT NOT NULL,
 goal_id TEXT NOT NULL, cell_id TEXT NOT NULL, cell_name TEXT NOT NULL, cell_repo TEXT NOT NULL,
 PRIMARY KEY(tenant_id,job_id)
);`

func selectedReceipt(ctx context.Context, tx *sql.Tx, req goalstate.Preparation) (goalstate.Prepared, bool, error) {
	var descriptor, requestedCell string
	h := jobdb.JobHandle{JobKey: req.Key}
	err := tx.QueryRowContext(ctx, `SELECT descriptor,requested_cell,goal_id,cell_id,cell_name,cell_repo FROM jobdb_submission_receipts WHERE tenant_id=? AND job_id=?`, req.Key.TenantId, req.Key.JobId).Scan(&descriptor, &requestedCell, &h.GoalID, &h.CellID, &h.Cell, &h.CellRepo)
	if errors.Is(err, sql.ErrNoRows) {
		return goalstate.Prepared{}, false, nil
	}
	if err != nil {
		return goalstate.Prepared{}, false, fmt.Errorf("%w: %v", jobdb.ErrSubmissionReceiptUnavailable, err)
	}
	target := jobdb.ResolvedCell{Cell: h.Cell, CellID: h.CellID, CellRepo: h.CellRepo}
	if err = jobdb.AuthorizePrimaryGoalSelection(ctx, jobdb.GoalKey{TenantId: req.Key.TenantId, GoalID: h.GoalID}, target); err != nil {
		return goalstate.Prepared{}, true, err
	}
	if descriptor != req.Descriptor {
		return goalstate.Prepared{}, true, goalstate.PreparationMismatch(descriptor, req.Descriptor)
	}
	if req.Cell != requestedCell {
		cells, err := loadCells(ctx, tx, req.Key.TenantId)
		if err != nil {
			return goalstate.Prepared{}, true, err
		}
		c, ok := cells.ByName(req.Cell)
		if !ok || c.ID != h.CellID {
			return goalstate.Prepared{}, true, jobdb.NewExistingJobMismatchError("cell differs")
		}
	}
	if req.CellRepo != "" {
		repo, err := jobdb.NormalizeCellRepo(req.CellRepo)
		if err != nil {
			return goalstate.Prepared{}, true, err
		}
		if repo != h.CellRepo {
			return goalstate.Prepared{}, true, jobdb.NewExistingJobMismatchError("repository differs")
		}
	}
	return goalstate.Prepared{Key: req.Key, Target: target, GoalID: h.GoalID, Published: true, Receipt: &h}, true, nil
}

func saveSelectedReceipt(ctx context.Context, tx *sql.Tx, key jobdb.JobKey, target jobdb.ResolvedCell, goalID string) error {
	var descriptor, requestedCell string
	if err := tx.QueryRowContext(ctx, `SELECT descriptor,requested_cell FROM jobdb_goal_preparations WHERE tenant_id=? AND job_id=?`, key.TenantId, key.JobId).Scan(&descriptor, &requestedCell); err != nil {
		return err
	}
	selected, err := goalstate.SelectedGoal(descriptor)
	if err != nil {
		return err
	}
	if selected == "" {
		return nil
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO jobdb_submission_receipts(tenant_id,job_id,descriptor,requested_cell,goal_id,cell_id,cell_name,cell_repo) VALUES(?,?,?,?,?,?,?,?)`, key.TenantId, key.JobId, descriptor, requestedCell, goalID, target.CellID, target.Cell, target.CellRepo)
	return err
}

func (r *Runtime) SupportsPrimaryGoalSelection() bool { return true }
