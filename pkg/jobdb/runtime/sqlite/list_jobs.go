package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/crashconcern"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadata"
	sqlitedriver "modernc.org/sqlite"
)

// These pure predicates preserve the Go API's JSON equality and nanosecond
// deadline semantics while allowing SQLite to filter before LIMIT. They never
// query targets, evaluate goals, or fetch dependency candidates.
func init() {
	sqlitedriver.MustRegisterDeterministicScalarFunction("jobdb_list_wait_for", 1, func(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
		ids, err := decodeWaitFor(listJSON(args[0]))
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(ids)
		return string(raw), err
	})
	sqlitedriver.MustRegisterDeterministicScalarFunction("jobdb_list_wait_pending", 2, func(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
		raw := listJSON(args[0])
		if len(raw) == 0 {
			return nil, jobdb.ErrDependencyWaitNotFound
		}
		var w struct {
			State    jobdb.DependencyState `json:"state"`
			Deadline *time.Time            `json:"deadline"`
		}
		if err := json.Unmarshal(raw, &w); err != nil {
			return nil, err
		}
		if w.State == jobdb.DependencyPending && (w.Deadline == nil || time.Unix(0, args[1].(int64)).Before(*w.Deadline)) {
			return int64(1), nil
		}
		return int64(0), nil
	})
	sqlitedriver.MustRegisterDeterministicScalarFunction("jobdb_list_metadata_matches", 2, func(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
		var predicates []normalizedMetadataPredicate
		if err := json.Unmarshal(listJSON(args[1]), &predicates); err != nil {
			return nil, err
		}
		match, err := metadataMatches(listJSON(args[0]), predicates)
		if err != nil {
			return nil, err
		}
		if match {
			return int64(1), nil
		}
		return int64(0), nil
	})
}

func listJSON(v driver.Value) []byte {
	switch x := v.(type) {
	case []byte:
		return x
	case string:
		return []byte(x)
	default:
		return nil
	}
}

func (r *Runtime) listCompactJobs(ctx context.Context, req jobdb.ListJobsRequest, pageSize int) (jobdb.ListJobsResponse, error) {
	predicates, err := jobdb.MetadataPredicates(req.MetadataFilter)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	normalized, err := normalizeMetadataPredicates(predicates)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	active, archived, err := listStoreSelection(req)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	now := time.Now().UTC().UnixNano()
	// Bind one observation time for both status and pending-wait projection.
	args := []any{now, crashconcern.DefaultThreshold}
	where := []string{}
	in := func(column string, values []string) {
		if len(values) == 0 {
			return
		}
		where = append(where, column+" IN ("+placeholders(len(values))+")")
		for _, value := range values {
			args = append(args, value)
		}
	}
	in("tenant_id", req.TenantIds)
	if !req.IncludeHidden {
		where = append(where, "hidden_at_ns IS NULL")
	}
	if !active && !archived {
		where = append(where, "0")
	} else if !active {
		where = append(where, "archived_at_ns IS NOT NULL")
	} else if !archived {
		where = append(where, "archived_at_ns IS NULL")
	}
	statuses := make([]string, len(req.Statuses))
	for i, status := range req.Statuses {
		statuses[i] = string(status)
	}
	in("list_status", statuses)
	in("job_type", req.JobTypes)
	if len(req.JobKeys) > 0 {
		parts := []string{}
		for _, key := range req.JobKeys {
			parts = append(parts, "(tenant_id=? AND job_id=?)")
			args = append(args, key.TenantId, key.JobId)
		}
		where = append(where, "("+strings.Join(parts, " OR ")+")")
	}
	if len(req.GoalIDs) > 0 {
		marks := placeholders(len(req.GoalIDs))
		where = append(where, "(goal_id IN ("+marks+") OR EXISTS(SELECT 1 FROM jobdb_goal_candidates gc WHERE gc.tenant_id=p.tenant_id AND gc.job_id=p.job_id AND gc.goal_id IN ("+marks+")))")
		for i := 0; i < 2; i++ {
			for _, id := range req.GoalIDs {
				args = append(args, id)
			}
		}
	}
	if len(req.JobTasks) > 0 {
		parts := []string{}
		for _, route := range req.JobTasks {
			if route.JobType == "" || route.TaskType == "" {
				continue
			}
			parts = append(parts, "(route_job_type=? AND route_task_type=?)")
			args = append(args, route.JobType, route.TaskType)
		}
		if len(parts) == 0 {
			where = append(where, "0")
		} else {
			where = append(where, "("+strings.Join(parts, " OR ")+")")
		}
	}
	if len(req.AvailableRoutes) > 0 {
		parts := []string{}
		for _, route := range req.AvailableRoutes {
			parts = append(parts, "(effective_job_type=? AND effective_task_type=?)")
			args = append(args, route.JobType, route.TaskType)
		}
		where = append(where, "list_status='READY' AND ("+strings.Join(parts, " OR ")+")")
	}
	if req.RootOnly {
		where = append(where, "list_parent=''")
	}
	in("list_parent", req.ParentJobIDs)
	if req.CreatedAfter != nil {
		where = append(where, "created_at_ns>=?")
		args = append(args, req.CreatedAfter.UnixNano())
	}
	if req.CreatedBefore != nil {
		where = append(where, "created_at_ns<=?")
		args = append(args, req.CreatedBefore.UnixNano())
	}
	if req.PageToken != "" {
		at, key, err := jobdb.DecodeListJobsPageToken(req.PageToken)
		if err != nil {
			return jobdb.ListJobsResponse{}, err
		}
		where = append(where, "(created_at_ns < ? OR (created_at_ns = ? AND list_key < ?))")
		args = append(args, at.UnixNano(), at.UnixNano(), key.String())
	}
	if len(normalized) > 0 {
		raw, err := json.Marshal(normalized)
		if err != nil {
			return jobdb.ListJobsResponse{}, err
		}
		where = append(where, "jobdb_list_metadata_matches(metadata,?)")
		args = append(args, string(raw))
	}
	args = append(args, pageSize+1)
	query := `WITH observation AS (SELECT ? AS now_ns, ? AS crash_threshold), projected AS (
 SELECT j.*, w.payload AS wait_payload,
  CASE WHEN j.dependency_wait_id<>'' AND j.archived_at_ns IS NULL AND j.cancel_requested=0
   THEN jobdb_list_wait_pending(w.payload,o.now_ns) ELSE 0 END AS pending_wait,
  COALESCE(j.parent_job_id,json_extract(CAST(j.metadata AS TEXT),'$.internal.parentJobId'),'') AS list_parent,
  j.tenant_id || '/' || j.job_id AS list_key,
  CASE WHEN j.alternate_job_type IS NOT NULL AND j.alternate_at_ns<=o.now_ns THEN j.alternate_job_type ELSE j.route_job_type END AS effective_job_type,
  CASE WHEN j.alternate_job_type IS NOT NULL AND j.alternate_at_ns<=o.now_ns THEN COALESCE(j.alternate_task_type,'') ELSE j.route_task_type END AS effective_task_type,
  CASE
   WHEN j.archived_at_ns IS NOT NULL THEN CASE WHEN j.cancel_requested OR j.completion_status='cancelled' THEN 'CANCELLED' ELSE 'COMPLETED' END
   WHEN j.cancel_requested THEN 'CANCELLED'
   WHEN COALESCE(j.lease_id,'')<>'' AND j.lease_expires_at_ns>o.now_ns THEN 'ACTIVE'
   WHEN j.dependency_wait_id<>'' AND jobdb_list_wait_pending(w.payload,o.now_ns) THEN 'PENDING_JOBS'
   WHEN (j.wait_deadline_ns IS NULL OR j.wait_deadline_ns>o.now_ns) AND EXISTS (
    SELECT 1 FROM json_each(jobdb_list_wait_for(j.wait_for)) d
    JOIN jobdb_jobs dep ON dep.tenant_id=j.tenant_id AND dep.job_id=d.value
    WHERE d.value<>'' AND dep.archived_at_ns IS NULL) THEN 'PENDING_JOBS'
   WHEN j.available_at_ns>o.now_ns THEN 'AWAITING_FUTURE'
   WHEN j.consecutive_expirations>=o.crash_threshold THEN 'CRASH_CONCERN'
   ELSE 'READY' END AS list_status
 FROM jobdb_jobs j CROSS JOIN observation o LEFT JOIN jobdb_dependency_waits w
 ON w.tenant_id=j.tenant_id AND w.job_id=j.job_id AND w.wait_id=j.dependency_wait_id
)
SELECT tenant_id,job_id,goal_id,job_type,route_job_type,route_task_type,
 client_payload_revision,client_payload,payload,wait_for,metadata,list_parent,available_at_ns,created_at_ns,archived_at_ns,
 cancel_requested,completion_status,completion_detail,lease_expires_at_ns,list_status,
 CASE WHEN pending_wait THEN dependency_wait_id ELSE '' END,
 CASE WHEN pending_wait THEN json_extract(CAST(wait_payload AS TEXT),'$.deadline') ELSE NULL END
FROM projected p WHERE ` + strings.Join(where, " AND ") + ` ORDER BY created_at_ns DESC,list_key DESC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	defer rows.Close()
	out := jobdb.ListJobsResponse{Jobs: make([]jobdb.JobSummary, 0, pageSize)}
	for rows.Next() {
		var s jobdb.JobSummary
		var route jobdb.Route
		var metadata, clientPayload, payload, waitFor []byte
		var available, created int64
		var archivedAt, leaseExpires sql.NullInt64
		var completion, completionDetail, deadline sql.NullString
		var waitID string
		if err = rows.Scan(&s.JobKey.TenantId, &s.JobKey.JobId, &s.GoalID, &s.JobType, &route.JobType, &route.TaskType,
			&s.ClientPayloadRevision, &clientPayload, &payload, &waitFor, &metadata, &s.ParentJobID, &available, &created, &archivedAt, &s.CancelRequested, &completion, &completionDetail, &leaseExpires, &s.Status, &waitID, &deadline); err != nil {
			return jobdb.ListJobsResponse{}, err
		}
		s.NextRoute = &route
		s.AvailableAt = timeFromNS(available)
		s.CreatedAt = timeFromNS(created)
		s.ArchivedAt = nullTimeFromNS(archivedAt)
		s.LeaseExpiresAt = nullTimeFromNS(leaseExpires)
		s.CompletionStatus = completion.String
		s.CompletionDetail = completionDetail.String
		s.ClientPayload = clientPayload
		s.ExecutionState = jobExecutionState(payload)
		s.WaitFor, err = decodeWaitFor(waitFor)
		if err != nil {
			return jobdb.ListJobsResponse{}, err
		}
		s.Metadata = jobdb.StripRuntimeMetadata(metadata)
		s.SchemaHash = jobmetadata.SchemaHashFromStoredMetadata(metadata)
		if waitID != "" {
			s.BlockingWaitSummary = &jobdb.DependencyWaitSummary{WaitID: waitID}
			if deadline.Valid {
				at, parseErr := time.Parse(time.RFC3339Nano, deadline.String)
				if parseErr != nil {
					return jobdb.ListJobsResponse{}, fmt.Errorf("wait deadline: %w", parseErr)
				}
				s.BlockingWaitSummary.Deadline = &at
			}
		}
		out.Jobs = append(out.Jobs, s)
	}
	if err = rows.Err(); err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	if len(out.Jobs) > pageSize {
		out.Jobs = out.Jobs[:pageSize]
		last := out.Jobs[pageSize-1]
		out.NextPageToken, err = jobdb.EncodeListJobsPageToken(last.CreatedAt, last.JobKey)
		if err != nil {
			return jobdb.ListJobsResponse{}, err
		}
	}
	return out, nil
}
