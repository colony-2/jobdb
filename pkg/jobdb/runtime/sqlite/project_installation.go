package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	rowsqlite "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
)

// ProjectInventory is a content inventory of this runtime database, together
// with an owner-supplied digest of independent participants. It contains hashes,
// never retained job bodies or secret values. Inspect it while all writers and
// providers are fenced. The library cannot establish an external runtime fence.
type ProjectInventory struct {
	Format       int                     `json:"format"`
	Participants string                  `json:"participants"`
	Tables       []ProjectTableInventory `json:"tables"`
	Scopes       []string                `json:"scopes"`
}
type ProjectTableInventory struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
	Rows   int    `json:"rows"`
	Digest string `json:"digest"`
}

var projectInventoryTables = map[string]string{
	"jobdb_resource_sequences": "tenant_id",
	"jobdb_resource_receipts":  "tenant_id",
	"jobdb_jobs":               "tenant_id", "jobdb_schemas": "tenant_id", "jobdb_cells": "tenant_id", "jobdb_cell_names": "tenant_id", "jobdb_cell_catalog": "tenant_id", "jobdb_cell_requests": "tenant_id",
	"jobdb_dependency_waits": "tenant_id", "jobdb_dependency_targets": "tenant_id", "jobdb_goal_preparations": "tenant_id", "jobdb_submission_receipts": "tenant_id", "jobdb_lease_events": "tenant_id", "jobdb_configuration": "tenant_id",
	"jobdb_chapter_stories": "anthology_id", "jobdb_chapter_chapters": "anthology_id", "jobdb_chapter_artifacts": "anthology_id",
}

func validParticipantDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && s == strings.ToLower(s)
}
func inspectProjects(ctx context.Context, conn *sql.Conn, participants string) (ProjectInventory, error) {
	out := ProjectInventory{Participants: participants, Tables: []ProjectTableInventory{}, Scopes: []string{}}
	if !validParticipantDigest(participants) {
		return out, fmt.Errorf("independent participant inventory SHA-256 is required")
	}
	if e := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&out.Format); e != nil {
		return out, e
	}
	if out.Format != 14 {
		return out, fmt.Errorf("project installation requires current SQLite format 14, got %d", out.Format)
	}
	rows, e := conn.QueryContext(ctx, `SELECT name,sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='jobdb_project_catalog' ORDER BY name`)
	if e != nil {
		return out, e
	}
	var tables [][2]string
	for rows.Next() {
		var name, definition string
		if e = rows.Scan(&name, &definition); e != nil {
			rows.Close()
			return out, e
		}
		if _, ok := projectInventoryTables[name]; !ok {
			rows.Close()
			return out, fmt.Errorf("unrecognized participant table %s", name)
		}
		tables = append(tables, [2]string{name, definition})
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, e
	}
	if len(tables) != len(projectInventoryTables) {
		return out, fmt.Errorf("incomplete SQLite participant schema")
	}
	scopes := map[string]bool{}
	for _, table := range tables {
		name := table[0]
		q, e := conn.QueryContext(ctx, `SELECT * FROM "`+name+`"`)
		if e != nil {
			return out, e
		}
		cols, e := q.Columns()
		if e != nil {
			q.Close()
			return out, e
		}
		scopeIndex := slices.Index(cols, projectInventoryTables[name])
		if scopeIndex < 0 {
			q.Close()
			return out, fmt.Errorf("missing scope column in %s", name)
		}
		var hashes []string
		for q.Next() {
			values := make([]any, len(cols))
			dest := make([]any, len(cols))
			for i := range values {
				dest[i] = &values[i]
			}
			if e = q.Scan(dest...); e != nil {
				q.Close()
				return out, e
			}
			scope, ok := values[scopeIndex].(string)
			if !ok {
				q.Close()
				return out, fmt.Errorf("invalid scope in %s", name)
			}
			if scope != "c2" && scope != "0" {
				q.Close()
				return out, fmt.Errorf("unapproved legacy project ID in %s", name)
			}
			scopes[scope] = true
			encoded, e := json.Marshal(values)
			if e != nil {
				q.Close()
				return out, e
			}
			h := sha256.Sum256(encoded)
			hashes = append(hashes, hex.EncodeToString(h[:]))
		}
		e = q.Err()
		q.Close()
		if e != nil {
			return out, e
		}
		sort.Strings(hashes)
		encoded, _ := json.Marshal(struct {
			Columns []string
			Hashes  []string
		}{cols, hashes})
		h := sha256.Sum256(encoded)
		out.Tables = append(out.Tables, ProjectTableInventory{Name: name, Schema: table[1], Rows: len(hashes), Digest: hex.EncodeToString(h[:])})
	}
	for id := range scopes {
		out.Scopes = append(out.Scopes, id)
	}
	sort.Strings(out.Scopes)
	return out, nil
}

// InspectProjectInstallation does not create a catalog or migrate a schema. Its
// single read transaction provides a consistent main-database inventory.
func InspectProjectInstallation(ctx context.Context, db *sql.DB, participants string) (ProjectInventory, error) {
	conn, e := db.Conn(ctx)
	if e != nil {
		return ProjectInventory{}, e
	}
	defer conn.Close()
	if _, e = conn.ExecContext(ctx, "BEGIN"); e != nil {
		return ProjectInventory{}, e
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
	return inspectProjects(ctx, conn, participants)
}

// InstallProjectCatalog rechecks the exact inventory and approved legacy seeds
// under the same SQLite writer lock as the completion marker. It modifies only
// the catalog table. A crash or mismatch cannot publish a partial installation.
// External stores, backups, keys and privileged writers remain owner obligations.
func InstallProjectCatalog(ctx context.Context, db *sql.DB, expected ProjectInventory, seeds []projectcatalog.Seed) error {
	if e := jobdb.RequireProjectAdministration(ctx); e != nil {
		return e
	}
	approved := map[string]bool{}
	for _, s := range seeds {
		if s.ID != s.Name || (s.ID != "c2" && s.ID != "0") || approved[s.ID] {
			return jobdb.ErrInvalidProject
		}
		approved[s.ID] = true
	}
	for _, id := range expected.Scopes {
		if !approved[id] {
			return jobdb.ErrProjectConflict
		}
	}
	conn, e := db.Conn(ctx)
	if e != nil {
		return e
	}
	defer conn.Close()
	for _, q := range []string{"PRAGMA busy_timeout=5000", "PRAGMA synchronous=FULL", "BEGIN IMMEDIATE"} {
		if _, e = conn.ExecContext(ctx, q); e != nil {
			return e
		}
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
	actual, e := inspectProjects(ctx, conn, expected.Participants)
	if e != nil {
		return e
	}
	want, _ := json.Marshal(expected)
	got, _ := json.Marshal(actual)
	if string(want) != string(got) {
		return jobdb.ErrProjectConflict
	}
	if _, e = conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS jobdb_project_catalog (singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload BLOB)`); e != nil {
		return e
	}
	if _, e = conn.ExecContext(ctx, `INSERT INTO jobdb_project_catalog(singleton,payload) VALUES(1,NULL) ON CONFLICT(singleton) DO NOTHING`); e != nil {
		return e
	}
	catalog := &projectcatalog.Service{Store: installationStore{conn}}
	manifest := projectcatalog.InstallationDigest(got)
	if e = catalog.Install(ctx, projectcatalog.Installation{Manifest: manifest, Seeds: seeds}); e != nil {
		return e
	}
	_, e = conn.ExecContext(ctx, "COMMIT")
	return e
}

// installationStore operates only inside the upgrader's already held transaction.
type installationStore struct{ conn *sql.Conn }

func (s installationStore) Read(ctx context.Context) (*projectcatalog.State, error) {
	var raw []byte
	if e := s.conn.QueryRowContext(ctx, `SELECT payload FROM jobdb_project_catalog WHERE singleton=1`).Scan(&raw); e != nil {
		return nil, e
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var state projectcatalog.State
	e := json.Unmarshal(raw, &state)
	return &state, e
}
func (s installationStore) Update(ctx context.Context, f func(**projectcatalog.State) error) error {
	state, e := s.Read(ctx)
	if e != nil {
		return e
	}
	if e = f(&state); e != nil {
		return e
	}
	raw, e := json.Marshal(state)
	if e != nil {
		return e
	}
	_, e = s.conn.ExecContext(ctx, `UPDATE jobdb_project_catalog SET payload=? WHERE singleton=1`, raw)
	return e
}

// InitializeProjectStorage explicitly creates current resource tables on a fresh
// main database. It opens no provider or blob store and creates no project ID.
// A failed initialization remains uninstalled and fenced for explicit recovery.
func InitializeProjectStorage(ctx context.Context, db *sql.DB) error {
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'").Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return jobdb.ErrProjectConflict
	}
	if err := configureSQLite(ctx, db); err != nil {
		return err
	}
	if err := migrate(ctx, db); err != nil {
		return err
	}
	if _, err := rowsqlite.New(db); err != nil {
		return err
	}
	_, err := configuration.NewSQLStore(ctx, db, configuration.SQLite)
	return err
}
