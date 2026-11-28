package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadataedit"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
)

const jobMetadataTable = "jobdb_extended_job_metadata"
const jobMetadataEventsTable = "jobdb_extended_job_metadata_events"
const jobMetadataSchemaSQL = `CREATE TABLE ` + jobMetadataTable + `(
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, key TEXT NOT NULL,
 version INTEGER NOT NULL CHECK(version > 0), value BLOB,
 PRIMARY KEY(tenant_id,job_id,key),
 FOREIGN KEY(tenant_id,job_id) REFERENCES jobdb_jobs(tenant_id,job_id) ON DELETE CASCADE
)`
const jobMetadataEventsSchemaSQL = `CREATE TABLE ` + jobMetadataEventsTable + `(
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL, key TEXT NOT NULL,
 version INTEGER NOT NULL CHECK(version > 0), value BLOB,
 PRIMARY KEY(tenant_id,job_id,key,version),
 FOREIGN KEY(tenant_id,job_id) REFERENCES jobdb_jobs(tenant_id,job_id) ON DELETE CASCADE
)`

func validateJobMetadataSchema(definition, expected string) bool {
	return strings.Join(strings.Fields(definition), " ") == strings.Join(strings.Fields(expected), " ")
}

func ensureJobMetadataSchema(ctx context.Context, tx *sql.Tx) error {
	present := 0
	for _, table := range []struct{ name, ddl string }{{jobMetadataTable, jobMetadataSchemaSQL}, {jobMetadataEventsTable, jobMetadataEventsSchemaSQL}} {
		var definition string
		err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE name=? AND type='table'`, table.name).Scan(&definition)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if !validateJobMetadataSchema(definition, table.ddl) {
			return fmt.Errorf("SQLite: inconsistent schema %s", table.name)
		}
		present++
	}
	switch present {
	case 2:
		return nil
	case 0:
		_, err := tx.ExecContext(ctx, jobMetadataSchemaSQL+";"+jobMetadataEventsSchemaSQL)
		return err
	default:
		return fmt.Errorf("SQLite: incomplete job metadata schema")
	}
}

func (runtime *Runtime) EditJobMetadata(ctx context.Context, key jobdb.JobKey, edit jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, error) {
	return runtime.editJobMetadata(ctx, key, edit, "", "")
}

func (lease *executionLease) EditJobMetadata(ctx context.Context, edit jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, error) {
	return lease.runtime.editJobMetadata(ctx, lease.jobKey, edit, lease.leaseID, lease.workerID)
}

func (runtime *Runtime) EditJobMetadataWithVerifiedLease(ctx context.Context, key jobdb.JobKey, edit jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, error) {
	claims, ok := leaseauth.ClaimsFromContext(ctx)
	if !ok || claims.LeaseID == "" || !leaseauth.Matches(claims, key, claims.LeaseID) {
		return jobdb.JobMetadataEntry{}, jobdb.ErrExecutionLeaseLost
	}
	return runtime.editJobMetadata(ctx, key, edit, claims.LeaseID, claims.WorkerID)
}

func (runtime *Runtime) editJobMetadata(ctx context.Context, key jobdb.JobKey, edit jobdb.JobMetadataEdit, leaseID, workerID string) (jobdb.JobMetadataEntry, error) {
	if err := key.Validate(); err != nil {
		return jobdb.JobMetadataEntry{}, jobdb.ErrInvalidJobMetadata
	}
	if err := edit.Validate(); err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := runtime.validate(); err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	if leaseID == "" {
		if _, bound := leaseauth.ClaimsFromContext(ctx); bound {
			return jobdb.JobMetadataEntry{}, jobdb.ErrLeasePermissionDenied
		}
		if err := jobmetadataedit.CheckScope(ctx, key, ""); err != nil {
			return jobdb.JobMetadataEntry{}, err
		}
		var cellID string
		err := runtime.db.QueryRowContext(ctx, `SELECT cell_id FROM jobdb_jobs WHERE tenant_id=? AND job_id=? AND hidden_at_ns IS NULL AND record_kind='EXECUTION'`, key.TenantId, key.JobId).Scan(&cellID)
		if errors.Is(err, sql.ErrNoRows) {
			return jobdb.JobMetadataEntry{}, jobdb.ErrJobNotFound
		}
		if err != nil {
			return jobdb.JobMetadataEntry{}, err
		}
		if err := jobmetadataedit.CheckScope(ctx, key, cellID); err != nil {
			return jobdb.JobMetadataEntry{}, err
		}
		if err := jobdb.RequireProjectWrite(ctx, runtime.projects, key.TenantId); err != nil {
			return jobdb.JobMetadataEntry{}, err
		}
	}
	var result jobdb.JobMetadataEntry
	err := runtime.withTx(ctx, func(tx *sql.Tx) error {
		var row jobRow
		rowErr := tx.QueryRowContext(ctx, `SELECT record_kind,cell_id,archived_at_ns,cancel_requested,lease_id,lease_worker_id,lease_expires_at_ns FROM jobdb_jobs WHERE tenant_id=? AND job_id=? AND hidden_at_ns IS NULL`, key.TenantId, key.JobId).Scan(&row.recordKind, &row.cell.CellID, &row.archivedAtNS, &row.cancelRequested, &row.leaseID, &row.leaseWorkerID, &row.leaseExpiresAtNS)
		if errors.Is(rowErr, sql.ErrNoRows) || row.recordKind.IsTerminalRecord() {
			return jobdb.ErrJobNotFound
		}
		if rowErr != nil {
			return rowErr
		}
		if leaseID != "" {
			if _, err := leaseauth.Authorize(ctx, key, leaseID); err != nil {
				return err
			}
			if workerID == "" {
				return jobdb.ErrExecutionLeaseLost
			}
			if err := validateLeaseRow(row, leaseID, workerID, runtime.now()); err != nil {
				return err
			}
		} else if err := jobmetadataedit.CheckScope(ctx, key, row.cell.CellID); err != nil {
			return err
		}
		var current jobdb.JobMetadataEntry
		var value []byte
		if err := tx.QueryRowContext(ctx, `SELECT version,value FROM `+jobMetadataTable+` WHERE tenant_id=? AND job_id=? AND key=?`, key.TenantId, key.JobId, edit.Key).Scan(&current.Version, &value); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		current.Present, current.Value = value != nil, value
		entry, changed, err := jobmetadataedit.Apply(current, edit)
		if err != nil {
			return err
		}
		if changed {
			var stored any
			if entry.Present {
				stored = []byte(entry.Value)
			}
			args := []any{key.TenantId, key.JobId, edit.Key, entry.Version, stored}
			if _, err = tx.ExecContext(ctx, `INSERT INTO `+jobMetadataTable+`(tenant_id,job_id,key,version,value) VALUES(?,?,?,?,?) ON CONFLICT(tenant_id,job_id,key) DO UPDATE SET version=excluded.version,value=excluded.value`, args...); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO `+jobMetadataEventsTable+`(tenant_id,job_id,key,version,value) VALUES(?,?,?,?,?)`, args...); err != nil {
				return err
			}
		}
		result = entry
		return nil
	})
	if err != nil {
		return jobdb.JobMetadataEntry{}, err
	}
	return result, nil
}

const currentJobMetadataSQL = `CASE WHEN p.hidden_at_ns IS NULL AND p.record_kind='EXECUTION' THEN
 (SELECT json_group_object(m.key,json(CASE WHEN m.value IS NULL
 THEN json_object('version',CAST(m.version AS TEXT),'present',json('false'))
 ELSE json_object('version',CAST(m.version AS TEXT),'present',json('true'),'value',json(CAST(m.value AS TEXT))) END))
 FROM ` + jobMetadataTable + ` m WHERE m.tenant_id=p.tenant_id AND m.job_id=p.job_id) END`

func decodeCurrentJobMetadata(raw []byte) (map[string]jobdb.JobMetadataEntry, error) {
	if raw == nil {
		return nil, nil
	}
	var entries map[string]jobdb.JobMetadataEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	return entries, nil
}
