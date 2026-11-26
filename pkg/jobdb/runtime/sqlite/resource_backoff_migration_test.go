package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	"github.com/stretchr/testify/require"
)

func resourceLegacyFixture(t *testing.T) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "format13.db")
	db, err := sql.Open(sqliteDriverName, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(retainedGoalSchemaSQL + leaseEventSchemaSQL + submissionReceiptSchemaSQL + `PRAGMA user_version=13`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO jobdb_cells VALUES('tenant','cell','test','example.test/test','',x'7b7d','ACTIVE','1',1,1,NULL)`)
	require.NoError(t, err)
	for i, count := range []int{0, 4, 5, 6} {
		id := fmt.Sprint(i)
		_, err = db.Exec(`INSERT INTO jobdb_jobs(tenant_id,job_id,goal_id,cell_id,cell_name,cell_repo,job_type,route_job_type,initial_payload_digest,available_at_ns,created_at_ns,updated_at_ns,goal_origin,consecutive_expirations,lease_id,lease_worker_id,lease_expires_at_ns,client_payload_revision,payload) VALUES('tenant',?,?, 'cell','test','example.test/test','work','work','digest',123,456,789,1,?,'retained-lease','worker',?,17,x'7b7d')`, id, id, count, time.Now().Add(time.Hour).UnixNano())
		require.NoError(t, err)
	}
	return path, db
}
func TestResourceBackoffMigration(t *testing.T) {
	t.Run("M02_history_bytes_and_ancillary_rows", func(t *testing.T) {
		path, db := resourceLegacyFixture(t)
		ctx := context.Background()
		uri, err := resolveConfiguredBlobStoreURI(Config{}, path)
		require.NoError(t, err)
		store, err := buildChapterStore(db, uri, 256, nil)
		require.NoError(t, err)
		defer store.Close(ctx)
		key := storyKeyForJob(jobdb.JobKey{TenantId: "tenant", JobId: "0"})
		chapters := []jobdb.Chapter{
			{Ordinal: 0, TaskType: "work", Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`"original input"`)}}},
			{Ordinal: 1, TaskType: "work", Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"completed":true}`)}}}},
		}
		originalBytes := make(map[int64][]byte)
		for i, ch := range chapters {
			raw, err := encodeChapter(ch)
			require.NoError(t, err)
			originalBytes[ch.Ordinal] = raw
			chapter := story.NewChapter().WithOrdinal(ch.Ordinal).WithBytes(raw)
			if i == 0 {
				_, err = store.CreateStory(chapterContext(ctx), key, story.CreateOptions{InitialChapter: chapter})
			} else {
				err = store.SaveChapter(chapterContext(ctx), key, chapter)
			}
			require.NoError(t, err)
		}
		before := compatibilitySnapshot(t, db)
		_, err = MigrateGoals(ctx, path)
		require.NoError(t, err)
		after := compatibilitySnapshot(t, db)
		for table, rows := range before {
			if table != "jobdb_jobs" {
				require.Equal(t, rows, after[table], table)
			}
		}
		for _, ch := range chapters {
			retained, err := store.Chapter(chapterContext(ctx), key, ch.Ordinal)
			require.NoError(t, err)
			require.Equal(t, originalBytes[ch.Ordinal], retained.Body())
		}
	})
	t.Run("M01_M02_once_preserves_all_other_columns", func(t *testing.T) {
		path, db := resourceLegacyFixture(t)
		ctx := context.Background()
		type facts struct {
			ID, Lease, Worker                             string
			Available, Created, Updated, Expiry, Revision int64
			Payload                                       []byte
		}
		read := func() []facts {
			rows, err := db.Query(`SELECT job_id,lease_id,lease_worker_id,available_at_ns,created_at_ns,updated_at_ns,lease_expires_at_ns,client_payload_revision,payload FROM jobdb_jobs ORDER BY job_id`)
			require.NoError(t, err)
			defer rows.Close()
			var out []facts
			for rows.Next() {
				var f facts
				require.NoError(t, rows.Scan(&f.ID, &f.Lease, &f.Worker, &f.Available, &f.Created, &f.Updated, &f.Expiry, &f.Revision, &f.Payload))
				out = append(out, f)
			}
			require.NoError(t, rows.Err())
			return out
		}
		before := read()
		report, err := MigrateGoals(ctx, path)
		require.NoError(t, err, "%+v", report)
		require.Equal(t, "MIGRATED", report.Verdict)
		require.Equal(t, 16, report.TargetFormat)
		require.Equal(t, before, read())
		var budgets []int64
		rows, err := db.Query(`SELECT crash_budget_remaining FROM jobdb_jobs ORDER BY job_id`)
		require.NoError(t, err)
		for rows.Next() {
			var b int64
			require.NoError(t, rows.Scan(&b))
			budgets = append(budgets, b)
		}
		require.NoError(t, rows.Close())
		require.Equal(t, []int64{5, 1, 0, 0}, budgets)
		_, err = db.Exec(`UPDATE jobdb_jobs SET crash_budget_remaining=100 WHERE job_id='0'`)
		require.NoError(t, err)
		report, err = MigrateGoals(ctx, path)
		require.NoError(t, err)
		require.Equal(t, "CURRENT", report.Verdict)
		var b int
		require.NoError(t, db.QueryRow(`SELECT crash_budget_remaining FROM jobdb_jobs WHERE job_id='0'`).Scan(&b))
		require.Equal(t, 100, b)
	})

	t.Run("M02_all_scheduler_fields_and_states", func(t *testing.T) {
		path, db := resourceLegacyFixture(t)
		rows, err := db.Query(`SELECT name FROM pragma_table_info('jobdb_jobs') ORDER BY cid`)
		require.NoError(t, err)
		var names []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, rows.Close())
		for _, state := range []string{"queued", "final", "cancelled", "hidden"} {
			projection := append([]string(nil), names...)
			for i, name := range names {
				if name == "job_id" || name == "goal_id" {
					projection[i] = "'" + state + "'"
				}
			}
			_, err = db.Exec(`INSERT INTO jobdb_jobs(` + strings.Join(names, ",") + `) SELECT ` + strings.Join(projection, ",") + ` FROM jobdb_jobs WHERE job_id='0'`)
			require.NoError(t, err)
		}
		_, err = db.Exec(`UPDATE jobdb_jobs SET lease_id=NULL,lease_worker_id=NULL,lease_expires_at_ns=NULL,wait_for=x'5b2231225d',available_at_ns=8000000000000000000,wait_deadline_ns=7000000000000000000,route_task_type='task',payload=x'7b227461736b5f77616974223a7b22696e7075745f73746570223a302c226f75747075745f73746570223a312c22696e7075745f68617368223a2278222c226e657874223a22776f726b227d7d',client_payload=x'7b2261223a6e756c6c7d',metadata=x'7b226d223a317d',append_key='batch',append_gen=3 WHERE job_id='queued';
 UPDATE jobdb_jobs SET archived_at_ns=900,completion_status='success',completion_detail='retained',goal_closure_state='SUCCEEDED',goal_closed_at_ns=900,goal_winner_job_id='final',goal_closure_origin='native' WHERE job_id='final';
 UPDATE jobdb_jobs SET cancel_requested=1 WHERE job_id='cancelled';
 UPDATE jobdb_jobs SET hidden_at_ns=901 WHERE job_id='hidden';
 UPDATE jobdb_jobs SET lease_expires_at_ns=1 WHERE job_id IN ('1','3')`)
		require.NoError(t, err)
		read := func() [][]any {
			rows, err := db.Query(`SELECT ` + strings.Join(names, ",") + ` FROM jobdb_jobs ORDER BY job_id`)
			require.NoError(t, err)
			defer rows.Close()
			var all [][]any
			for rows.Next() {
				values := make([]any, len(names))
				pointers := make([]any, len(names))
				for i := range values {
					pointers[i] = &values[i]
				}
				require.NoError(t, rows.Scan(pointers...))
				all = append(all, values)
			}
			require.NoError(t, rows.Err())
			return all
		}
		before := read()
		_, err = MigrateGoals(context.Background(), path)
		require.NoError(t, err)
		require.Equal(t, before, read())
		var n int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM jobdb_resource_sequences`).Scan(&n))
		require.Zero(t, n)
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM jobdb_resource_receipts`).Scan(&n))
		require.Zero(t, n)
	})
	t.Run("M03_noninteger_and_unsupported", func(t *testing.T) {
		for _, value := range []any{1.5, "bad"} {
			path, db := resourceLegacyFixture(t)
			_, err := db.Exec(`UPDATE jobdb_jobs SET consecutive_expirations=? WHERE job_id='0'`, value)
			require.NoError(t, err)
			_, err = MigrateGoals(context.Background(), path)
			require.Error(t, err)
			var version int
			require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
			require.Equal(t, 13, version)
		}
		path, db := resourceLegacyFixture(t)
		_, err := db.Exec(`PRAGMA user_version=999`)
		require.NoError(t, err)
		_, err = MigrateGoals(context.Background(), path)
		require.Error(t, err)
		var version int
		require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
		require.Equal(t, 999, version)
	})
	t.Run("M03_corrupt_rollback", func(t *testing.T) {
		path, db := resourceLegacyFixture(t)
		_, err := db.Exec(`UPDATE jobdb_jobs SET consecutive_expirations=-1 WHERE job_id='0'`)
		require.NoError(t, err)
		_, err = MigrateGoals(context.Background(), path)
		require.Error(t, err)
		var version int
		require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
		require.Equal(t, 13, version)
		var cols int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM pragma_table_info('jobdb_jobs') WHERE name='crash_budget_remaining'`).Scan(&cols))
		require.Zero(t, cols)
	})
	t.Run("M03_interrupted_M04_live_zero", func(t *testing.T) {
		path, db := resourceLegacyFixture(t)
		_, err := migrateGoals(context.Background(), path, func(stage string) error {
			if stage == "marked" {
				return fmt.Errorf("interrupted")
			}
			return nil
		})
		require.Error(t, err)
		var version int
		require.NoError(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
		require.Equal(t, 13, version)
		_, err = MigrateGoals(context.Background(), path)
		require.NoError(t, err)
		require.NoError(t, db.Close())
		reopened, err := sql.Open(sqliteDriverName, path)
		require.NoError(t, err)
		defer reopened.Close()
		var b int
		var lease string
		require.NoError(t, reopened.QueryRow(`SELECT crash_budget_remaining,lease_id FROM jobdb_jobs WHERE job_id='2'`).Scan(&b, &lease))
		require.Zero(t, b)
		require.Equal(t, "retained-lease", lease)
	})
	t.Run("M03_constructor_refuses_13_without_mutation", func(t *testing.T) {
		path, db := resourceLegacyFixture(t)
		require.NoError(t, db.Close())
		before, err := os.ReadFile(path)
		require.NoError(t, err)
		_, err = NewFromConfig(context.Background(), Config{DBPath: path})
		require.ErrorIs(t, err, ErrMigrationRequired)
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, after)
	})
}
