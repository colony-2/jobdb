package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	secretsqlite "github.com/colony-2/jobdb/pkg/jobdb/secrets/sqlite"
	"github.com/stretchr/testify/require"
)

// Exercise the real startup path with nonempty application and auxiliary stores.
// Compare every original SQL value before any post-upgrade mutation, then read
// retained application objects and external artifact bytes through the runtime.
func TestLeaseEventStartupPreservesSavedState(t *testing.T) {
	for _, format := range []int{11, 12} {
		t.Run(fmt.Sprint(format), func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			dir := t.TempDir()
			cfg := Config{DBPath: filepath.Join(dir, "saved.db")}
			r, e := NewFromConfig(ctx, cfg)
			require.NoError(t, e)
			h, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "saved", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: &jobdb.SimpleTaskData{Data: []byte(`{"saved":true}`), Artifacts: []jobdb.Artifact{jobdb.NewArtifactFromBytes("input.txt", []byte("saved artifact bytes\x00"))}}}})
			require.NoError(t, e)
			_, e = celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "dependent", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(2), Dependencies: []jobdb.Dependency{{Target: jobdb.DependencyTarget{JobID: h.JobKey.JobId}, Condition: jobdb.JobPrereqCondition("success")}}}})
			require.NoError(t, e)
			lease, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "saved-worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Hour})
			require.NoError(t, e)
			require.NoError(t, lease.KeepAlive(ctx))
			_, e = r.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "saved", Value: json.RawMessage(`{"bytes":[0,1,255]}`)})
			require.NoError(t, e)
			cell, e := r.GetCell(ctx, jobdb.CellKey{TenantID: "tenant", CellID: h.CellID})
			require.NoError(t, e)
			name := "renamed"
			_, e = r.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "tenant", CellID: h.CellID}, IfMatch: fmt.Sprintf("%q", cell.Revision), Name: &name})
			require.NoError(t, e)
			_, e = r.db.Exec("CREATE TABLE unrelated_saved(value BLOB); INSERT INTO unrelated_saved VALUES(x'0001ff')")
			require.NoError(t, e)

			secretPath := filepath.Join(dir, "separate-secrets.db")
			secretStore, e := secretsqlite.Open(ctx, secretPath, jobdb.SecretLimits{})
			require.NoError(t, e)
			require.NoError(t, secretStore.Set(ctx, "tenant", "saved", "retained secret\x00"))
			require.NoError(t, secretStore.Close())
			secretBytes, e := os.ReadFile(secretPath)
			require.NoError(t, e)

			job, e := r.GetJob(ctx, h.JobKey)
			require.NoError(t, e)
			goal, e := r.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: h.GoalID})
			require.NoError(t, e)
			config, e := r.GetTenantConfiguration(ctx, "tenant")
			require.NoError(t, e)
			chapter, e := r.GetChapter(ctx, jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 0})
			require.NoError(t, e)
			events, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
			require.NoError(t, e)
			if format == 11 {
				_, e = r.db.Exec("DROP TABLE jobdb_lease_events")
				require.NoError(t, e)
			}
			_, e = r.db.Exec(fmt.Sprintf("PRAGMA user_version=%d", format))
			require.NoError(t, e)
			saved := compatibilitySnapshot(t, r.db)
			require.NoError(t, r.Close(ctx))
			for repeat := 0; repeat < 3; repeat++ {
				r, e = NewFromConfig(ctx, cfg)
				require.NoError(t, e)
				got := compatibilitySnapshot(t, r.db)
				for table, values := range saved {
					require.Equal(t, values, got[table], table)
				}
				var version int
				require.NoError(t, r.db.QueryRow("PRAGMA user_version").Scan(&version))
				require.Equal(t, 11, version)
				actualJob, err := r.GetJob(ctx, h.JobKey)
				require.NoError(t, err)
				require.Equal(t, job, actualJob)
				actualGoal, err := r.GetGoal(ctx, jobdb.GoalKey{TenantId: "tenant", GoalID: h.GoalID})
				require.NoError(t, err)
				require.Equal(t, goal, actualGoal)
				actualConfig, err := r.GetTenantConfiguration(ctx, "tenant")
				require.NoError(t, err)
				require.Equal(t, config, actualConfig)

				actualCell, err := r.GetCell(ctx, jobdb.CellKey{TenantID: "tenant", CellID: h.CellID})
				require.NoError(t, err)
				require.Equal(t, "renamed", actualCell.Name)
				require.Contains(t, actualCell.Aliases, "test")
				actualChapter, err := r.GetChapter(ctx, jobdb.ChapterRef{JobKey: h.JobKey, Ordinal: 0})
				require.NoError(t, err)
				require.Equal(t, chapter, actualChapter)
				assertStartArtifact(t, ctx, r, h.JobKey, "input.txt", []byte("saved artifact bytes\x00"))
				actualEvents, err := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
				require.NoError(t, err)
				if format == 12 {
					require.Equal(t, events, actualEvents)
				} else {
					require.Empty(t, actualEvents.Events)
				}
				actualSecrets, err := os.ReadFile(secretPath)
				require.NoError(t, err)
				require.Equal(t, secretBytes, actualSecrets)
				require.NoError(t, r.Close(ctx))
			}
		})
	}
}

func TestLeaseEventPollAndInternalClaimFailureRollback(t *testing.T) {
	for _, path := range []string{"poll", "internal"} {
		t.Run(path, func(t *testing.T) {
			ctx := celltest.Context(context.Background())
			var r *Runtime
			var mutate func() error
			if path == "poll" {
				r, _, _ = leaseEventFixture(t)
				mutate = func() error {
					_, e := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, Limit: 1})
					return e
				}
			} else {
				var req jobdb.CompleteTaskIfWaitingRequest
				r, _, req = waitingTaskFixture(t)
				mutate = func() error { return r.CompleteTaskIfWaiting(ctx, req) }
			}
			before := compatibilitySnapshot(t, r.db)
			_, e := r.db.Exec(`CREATE TRIGGER reject_event BEFORE INSERT ON jobdb_lease_events BEGIN SELECT RAISE(ABORT,'injected event failure'); END`)
			require.NoError(t, e)
			require.Error(t, mutate())
			require.Equal(t, before, compatibilitySnapshot(t, r.db))
			_, e = r.db.Exec("DROP TRIGGER reject_event")
			require.NoError(t, e)
			require.NoError(t, mutate())
		})
	}
}
