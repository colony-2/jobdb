package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/stretchr/testify/require"
)

func TestLeaseEventsDefaultLegacyMutations(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "legacy.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer r.Close(ctx)
	var n, version int
	require.NoError(t, r.db.QueryRow("PRAGMA user_version").Scan(&version))
	require.Equal(t, 11, version)
	require.NoError(t, r.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='jobdb_lease_events'").Scan(&n))
	require.Equal(t, 1, n)
	for _, targeted := range []bool{false, true} {
		h, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: fmt.Sprint(targeted), Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
		require.NoError(t, e)
		var l jobdb.ExecutionLease
		if targeted {
			l, e = r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}})
		} else {
			var ls []jobdb.ExecutionLease
			ls, e = r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, Limit: 1})
			require.Len(t, ls, 1)
			l = ls[0]
		}
		require.NoError(t, e)
		require.NotNil(t, l)
		require.NoError(t, l.KeepAlive(ctx))
		_, e = r.RenewExecutionLeaseByID(ctx, h.JobKey, l.LeaseID(), "worker", time.Minute)
		require.NoError(t, e)
		require.NoError(t, l.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 1, TaskType: "job", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`2`)}}}}}))
	}
	_, err = r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, err)
	require.NoError(t, r.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='jobdb_lease_events'").Scan(&n))
	require.Equal(t, 1, n)
}

// snapshot captures every persisted value, including event rows, chapters and signing keys.
func compatibilitySnapshot(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	rows, e := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	require.NoError(t, e)
	var names []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	out := map[string][]string{}
	for _, name := range names {
		rows, e = db.Query(fmt.Sprintf(`SELECT * FROM %q ORDER BY rowid`, name))
		require.NoError(t, e)
		cols, e := rows.Columns()
		require.NoError(t, e)
		out[name] = []string{}
		for rows.Next() {
			v := make([]any, len(cols))
			ptr := make([]any, len(cols))
			for i := range v {
				ptr[i] = &v[i]
			}
			require.NoError(t, rows.Scan(ptr...))
			out[name] = append(out[name], fmt.Sprintf("%#v", v))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return out
}

func TestLeaseEventExtensionTransitions(t *testing.T) {
	r, cfg, key := leaseEventFixture(t)
	ctx := celltest.Context(context.Background())
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Hour})
	require.NoError(t, e)
	require.NoError(t, l.KeepAlive(ctx))
	before := compatibilitySnapshot(t, r.db)
	_, e = r.db.Exec("PRAGMA user_version=12")
	require.NoError(t, e)
	require.NoError(t, r.Close(ctx))

	r, e = NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	require.Equal(t, before, compatibilitySnapshot(t, r.db))
	var version int
	require.NoError(t, r.db.QueryRow("PRAGMA user_version").Scan(&version))
	require.Equal(t, 11, version)
	require.NoError(t, r.Close(ctx))
	r, e = NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	defer r.Close(ctx)
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Len(t, page.Events, 2)
	require.NoError(t, migrate(ctx, r.db))
	require.NoError(t, migrate(ctx, r.db))
	require.Equal(t, before["jobdb_cell_catalog"], compatibilitySnapshot(t, r.db)["jobdb_cell_catalog"])
}

func TestLeaseEventSchemaRejectsWithoutPartialMigration(t *testing.T) {
	for _, damage := range []string{"PRAGMA user_version=99", "DROP TABLE jobdb_lease_events; PRAGMA user_version=12", "DROP INDEX jobdb_jobs_poll_idx; PRAGMA user_version=12"} {
		t.Run(damage, func(t *testing.T) {
			r, _, _ := leaseEventFixture(t)
			_, e := r.db.Exec(damage)
			require.NoError(t, e)
			before := compatibilitySnapshot(t, r.db)
			var version int
			require.NoError(t, r.db.QueryRow("PRAGMA user_version").Scan(&version))
			require.Error(t, migrate(context.Background(), r.db))
			var after int
			require.NoError(t, r.db.QueryRow("PRAGMA user_version").Scan(&after))
			require.Equal(t, version, after)
			require.Equal(t, before, compatibilitySnapshot(t, r.db))
		})
	}
	t.Run("extension installation rollback", func(t *testing.T) {
		r, cfg, key := leaseEventFixture(t)
		ctx := celltest.Context(context.Background())
		original, e := r.GetJob(ctx, key)
		require.NoError(t, e)
		_, e = r.db.Exec("DROP TABLE jobdb_lease_events; CREATE INDEX jobdb_lease_events_time_idx ON jobdb_jobs(job_id)")
		require.NoError(t, e)
		before := compatibilitySnapshot(t, r.db)
		var schemaBefore string
		require.NoError(t, r.db.QueryRow("SELECT group_concat(sql,';') FROM (SELECT sql FROM sqlite_master ORDER BY name)").Scan(&schemaBefore))
		require.NoError(t, r.Close(ctx))
		_, e = NewFromConfig(ctx, cfg)
		require.Error(t, e)
		raw, e := sql.Open(sqliteDriverName, cfg.DBPath)
		require.NoError(t, e)
		require.Equal(t, before, compatibilitySnapshot(t, raw))
		var schemaAfter string
		require.NoError(t, raw.QueryRow("SELECT group_concat(sql,';') FROM (SELECT sql FROM sqlite_master ORDER BY name)").Scan(&schemaAfter))
		require.Equal(t, schemaBefore, schemaAfter)
		var version int
		require.NoError(t, raw.QueryRow("PRAGMA user_version").Scan(&version))
		require.Equal(t, 11, version)
		_, e = raw.Exec("DROP INDEX jobdb_lease_events_time_idx")
		require.NoError(t, e)
		require.NoError(t, raw.Close())
		r, e = NewFromConfig(ctx, cfg)
		require.NoError(t, e)
		defer r.Close(ctx)
		actual, e := r.GetJob(ctx, key)
		require.NoError(t, e)
		require.Equal(t, original, actual)
		page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
		require.NoError(t, e)
		require.Empty(t, page.Events)
	})
}

func TestLeaseEventsHistoricalMixedWriter(t *testing.T) {
	binary := os.Getenv("JOBDB_HISTORICAL_WRITER")
	if binary == "" {
		t.Skip("build testdata/historical-writer.go against authentic f92ae253 source and set JOBDB_HISTORICAL_WRITER")
	}
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "mixed.db")}
	run := func(id string) {
		out, e := exec.Command(binary, cfg.DBPath, id).CombinedOutput()
		require.NoError(t, e, "%s", out)
	}
	run("historical-before")
	raw, e := sql.Open(sqliteDriverName, cfg.DBPath)
	require.NoError(t, e)
	old := compatibilitySnapshot(t, raw)
	require.NoError(t, raw.Close())
	r, e := NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	upgraded := compatibilitySnapshot(t, r.db)
	for table, values := range old {
		require.Equal(t, values, upgraded[table], table)
	}
	initial, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Empty(t, initial.Events)
	config, e := r.GetTenantConfiguration(ctx, "tenant")
	require.NoError(t, e)
	require.JSONEq(t, `{"historical":true}`, string(config.Values["saved"]))
	assertStartArtifact(t, ctx, r, jobdb.JobKey{TenantId: "tenant", JobId: "historical-live"}, "saved.bin", []byte("historical artifact bytes\x00"))
	h, e := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "new", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, e)
	l, e := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "new", Routes: []jobdb.Route{{JobType: "job"}}})
	require.NoError(t, e)
	require.NoError(t, l.KeepAlive(ctx))
	events, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Len(t, events.Events, 2)
	require.NoError(t, r.Close(ctx))
	run("historical-after")
	r, e = NewFromConfig(ctx, cfg)
	require.NoError(t, e)
	defer r.Close(ctx)
	page, e := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
	require.Equal(t, events, page)
	jobs, e := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}})
	require.NoError(t, e)
	require.GreaterOrEqual(t, len(jobs.Jobs), 5)
	require.Equal(t, old["jobdb_cell_catalog"], compatibilitySnapshot(t, r.db)["jobdb_cell_catalog"])
	assertStartArtifact(t, ctx, r, jobdb.JobKey{TenantId: "tenant", JobId: "historical-live"}, "saved.bin", []byte("historical artifact bytes\x00"))
	for _, id := range []string{"historical-before", "historical-after"} {
		info, e := r.GetJob(ctx, jobdb.JobKey{TenantId: "tenant", JobId: id})
		require.NoError(t, e)
		require.Equal(t, jobdb.JobStatusCompleted, info.Status)
	}
}

func TestLeaseEventsDefaultInternalClaim(t *testing.T) {
	r, _, req := waitingTaskFixture(t)
	ctx := celltest.Context(context.Background())
	require.NoError(t, r.CompleteTaskIfWaiting(ctx, req))
	var count int
	require.NoError(t, r.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='jobdb_lease_events'").Scan(&count))
	require.Equal(t, 1, count)
}

func TestLeaseEventConstructorDefault(t *testing.T) {
	db, e := sql.Open(sqliteDriverName, ":memory:")
	require.NoError(t, e)
	defer db.Close()
	db.SetMaxOpenConns(1)
	r, e := New(db, WithBlobStoreURI("blobfs://"+t.TempDir()))
	require.NoError(t, e)
	defer r.Close(context.Background())
	_, e = r.ListLeaseEvents(celltest.Context(context.Background()), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.NoError(t, e)
}

func TestLeaseEventRepeatedMigrationReadOnly(t *testing.T) {
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "readonly.db")}
	r, e := NewFromConfig(context.Background(), cfg)
	require.NoError(t, e)
	require.NoError(t, r.Close(context.Background()))
	db, e := sql.Open(sqliteDriverName, "file:"+cfg.DBPath+"?mode=ro")
	require.NoError(t, e)
	defer db.Close()
	require.NoError(t, migrate(context.Background(), db))
}
