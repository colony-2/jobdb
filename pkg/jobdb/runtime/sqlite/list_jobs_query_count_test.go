package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore"
	"github.com/stretchr/testify/require"
)

// Count at the SQL driver boundary, including queries from detail getters.
type listCountingConnector struct {
	dsn     string
	drv     driver.Driver
	queries atomic.Int64
}

func (c *listCountingConnector) Driver() driver.Driver { return c.drv }
func (c *listCountingConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &listCountingConn{Conn: conn, queries: &c.queries}, nil
}

type listCountingConn struct {
	driver.Conn
	queries *atomic.Int64
}

func (c *listCountingConn) Prepare(query string) (driver.Stmt, error) {
	c.queries.Add(1)
	return c.Conn.Prepare(query)
}
func (c *listCountingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.queries.Add(1)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *listCountingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.queries.Add(1)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func TestStoredListFieldsUseOneQueryPerPage(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "list.db")
	r, err := NewFromConfig(ctx, Config{DBPath: path})
	require.NoError(t, err)
	defer r.Close(ctx)
	policy := jobdb.RunPolicy{Retry: jobdb.RetryPolicy{MaximumAttempts: 4}}
	task := &taskWait{InputStep: 2, OutputStep: 3, InputHash: "stored-hash", Next: "resume"}
	payload, err := encodeJobPayload(jobPayload{RunPolicy: policy, TaskWait: task})
	require.NoError(t, err)
	waitFor, err := encodeWaitFor([]string{"unresolved-b", "unresolved-a"})
	require.NoError(t, err)
	for i := 0; i < 201; i++ {
		_, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: fmt.Sprintf("job-%03d", i), JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
		require.NoError(t, err)
	}
	// Inject stored snapshots without task chapters. Both branches must copy them.
	_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET payload=?, wait_for=?, client_payload=?, client_payload_revision=7, route_task_type='input'`, payload, waitFor, []byte(`{"outside":[9007199254740993,null]}`))
	require.NoError(t, err)
	_, err = r.db.ExecContext(ctx, `UPDATE jobdb_jobs SET archived_at_ns=?, completion_status='future_category', completion_detail=? WHERE job_id < 'job-100'`, time.Now().UnixNano(), " \tfailure 雪\n ")
	require.NoError(t, err)
	// Output memberships and history must not be touched, even for a full page.
	_, err = r.db.ExecContext(ctx, `DROP TABLE jobdb_goal_candidates`)
	require.NoError(t, err)
	originalStore := r.chapterStore
	r.chapterStore = &chapterstore.Store{}
	defer func() { r.chapterStore = originalStore }()
	counted := &listCountingConnector{dsn: path, drv: r.db.Driver()}
	countedDB := sql.OpenDB(counted)
	defer countedDB.Close()
	originalDB := r.db
	r.db = countedDB
	defer func() { r.db = originalDB }()
	for _, size := range []int{1, 100, 200} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			req := jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, PageSize: size}
			total, archived := 0, 0
			for {
				counted.queries.Store(0)
				page, err := r.ListJobs(ctx, req)
				require.NoError(t, err)
				require.EqualValues(t, 1, counted.queries.Load())
				for _, row := range page.Jobs {
					total++
					require.Equal(t, `{"outside":[9007199254740993,null]}`, string(row.ClientPayload))
					require.EqualValues(t, 7, row.ClientPayloadRevision)
					require.Equal(t, jobdb.ExecutionState{RunPolicy: policy, TaskWait: &jobdb.TaskWait{InputOrdinal: 2, OutputOrdinal: 3, InputHash: "stored-hash", ResumeJobType: "resume"}}, row.ExecutionState)
					require.Equal(t, []string{"unresolved-b", "unresolved-a"}, row.WaitFor)
					require.Empty(t, row.OtherGoalIDs)
					require.Nil(t, row.BlockingWait)
					if row.ArchivedAt != nil {
						archived++
						require.Equal(t, "future_category", row.CompletionStatus)
						require.Equal(t, " \tfailure 雪\n ", row.CompletionDetail)
					} else {
						require.Empty(t, row.CompletionStatus)
						require.Empty(t, row.CompletionDetail)
					}
				}
				if page.NextPageToken == "" {
					break
				}
				req.PageToken = page.NextPageToken
			}
			require.Equal(t, 201, total)
			require.Equal(t, 100, archived)
		})
	}
}
