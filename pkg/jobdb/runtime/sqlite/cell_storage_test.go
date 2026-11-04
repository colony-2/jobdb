package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestCellColumnsAndCreationReplaySurviveReopen(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: t.TempDir() + "/cells.db"}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = r.Close(ctx) }()
	req := jobdb.CreateCellRequest{TenantID: "t", Name: "root", Repo: "example.test/root", RequestID: "create", Metadata: map[string]json.RawMessage{"nested": json.RawMessage(`{"n":9007199254740993,"a":[null,true]}`)}}
	original, err := r.CreateCell(ctx, req)
	require.NoError(t, err)
	h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "t", Cell: "root", JobID: "job", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	_, err = r.UpsertSchedule(ctx, jobdb.UpsertScheduleRequest{TenantId: "t", ScheduleId: "schedule", Trigger: jobdb.ScheduleTrigger{Kind: jobdb.ScheduleTriggerInterval, Interval: time.Hour}, Target: jobdb.ScheduleTarget{Cell: "root", JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	for _, q := range []string{
		`SELECT cell_id,cell_name,cell_repo,payload FROM jobdb_goals WHERE tenant_id='t'`,
		`SELECT cell_id,cell_name,cell_repo,target_json FROM jobdb_schedules WHERE tenant_id='t'`,
	} {
		var id, name, repo string
		var raw []byte
		require.NoError(t, r.db.QueryRowContext(ctx, q).Scan(&id, &name, &repo, &raw))
		require.Equal(t, original.ID, id)
		require.Equal(t, original.Name, name)
		require.Equal(t, original.Repo, repo)
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &fields))
		for _, key := range []string{"cell", "cellId", "cellRepo"} {
			require.NotContains(t, fields, key)
		}
	}
	var targetID, requested string
	require.NoError(t, r.db.QueryRowContext(ctx, `SELECT cell_id,requested_cell FROM jobdb_goal_preparations WHERE tenant_id='t' AND job_id=?`, h.JobKey.JobId).Scan(&targetID, &requested))
	require.Equal(t, original.ID, targetID)
	require.Equal(t, "root", requested)
	for table, oldColumn := range map[string]string{"jobdb_cells": "payload", "jobdb_cell_requests": "payload", "jobdb_goal_preparations": "cell_target"} {
		var count int
		require.NoError(t, r.db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info(?) WHERE name=?`, table, oldColumn).Scan(&count))
		require.Zero(t, count)
	}
	name := "renamed"
	repo := "example.test/repointed"
	changed, err := r.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "t", CellID: original.ID}, IfMatch: fmt.Sprintf("%q", original.Revision), Name: &name, Repo: &repo})
	require.NoError(t, err)
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	got, err := r.GetCell(ctx, jobdb.CellKey{TenantID: "t", CellID: original.ID})
	require.NoError(t, err)
	require.Equal(t, changed, got)
	replay, err := r.CreateCell(ctx, req)
	require.NoError(t, err)
	require.Equal(t, original, replay)
	alias, err := r.ListCells(ctx, jobdb.ListCellsRequest{TenantID: "t", Name: "root"})
	require.NoError(t, err)
	require.Equal(t, []jobdb.Cell{changed}, alias.Cells)
}
