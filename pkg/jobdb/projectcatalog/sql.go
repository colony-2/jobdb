package projectcatalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type Dialect string

const (
	SQLite   Dialect = "sqlite"
	Postgres Dialect = "postgres"
)

// SQLStore serializes the small administrative catalog through one installation
// row. Resource data stays in its existing stores. PostgreSQL uses a row lock;
// SQLite obtains its writer lock before reading, including on borrowed handles.
// The database is caller-owned. Opening a store does not install or import it.
type SQLStore struct {
	db      *sql.DB
	dialect Dialect
}

func NewSQLStore(db *sql.DB, dialect Dialect) (*SQLStore, error) {
	if db == nil || (dialect != SQLite && dialect != Postgres) {
		return nil, fmt.Errorf("project catalog requires a database and supported dialect")
	}
	return &SQLStore{db: db, dialect: dialect}, nil
}

// CreateSchema is an explicit upgrader step. A crash here leaves an uninstalled
// catalog; resource writers must remain fenced until Install succeeds.
func (s *SQLStore) CreateSchema(ctx context.Context) error {
	payloadType := "BLOB"
	if s.dialect == Postgres {
		payloadType = "BYTEA"
	}
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS jobdb_project_catalog (singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload `+payloadType+`)`)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO jobdb_project_catalog(singleton,payload) VALUES(1,NULL) ON CONFLICT(singleton) DO NOTHING`)
	return err
}
func decode(raw []byte) (*State, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var s State
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	return &s, nil
}
func (s *SQLStore) Read(ctx context.Context) (*State, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if s.dialect == SQLite {
		if _, err = conn.ExecContext(ctx, `PRAGMA busy_timeout=5000`); err != nil {
			return nil, err
		}
	}
	var raw []byte
	err = conn.QueryRowContext(ctx, `SELECT payload FROM jobdb_project_catalog WHERE singleton=1`).Scan(&raw)
	if err != nil {
		return nil, err
	}
	return decode(raw)
}
func (s *SQLStore) Update(ctx context.Context, f func(**State) error) error {
	if s.dialect == SQLite {
		return s.updateSQLite(ctx, f)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Admission must survive a process/host failure before an independent store
	// can begin an effect, even if the borrowed session relaxed commit flushing.
	if _, err = tx.ExecContext(ctx, `SET LOCAL synchronous_commit=on`); err != nil {
		return err
	}
	var raw []byte
	if err = tx.QueryRowContext(ctx, `SELECT payload FROM jobdb_project_catalog WHERE singleton=1 FOR UPDATE`).Scan(&raw); err != nil {
		return err
	}
	state, err := decode(raw)
	if err != nil {
		return err
	}
	if err = f(&state); err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("catalog cannot be uninstalled")
	}
	next, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE jobdb_project_catalog SET payload=$1 WHERE singleton=1`, next); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *SQLStore) updateSQLite(ctx context.Context, f func(**State) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// PRAGMA is connection-local. A caller-owned pool may contain connections
	// that have never been configured by a runtime constructor.
	if _, err = conn.ExecContext(ctx, `PRAGMA busy_timeout=5000`); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `PRAGMA synchronous=FULL`); err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`)
	var raw []byte
	if err = conn.QueryRowContext(ctx, `SELECT payload FROM jobdb_project_catalog WHERE singleton=1`).Scan(&raw); err != nil {
		return err
	}
	state, err := decode(raw)
	if err != nil {
		return err
	}
	if err = f(&state); err != nil {
		return err
	}
	if state == nil {
		return fmt.Errorf("catalog cannot be uninstalled")
	}
	next, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if _, err = conn.ExecContext(ctx, `UPDATE jobdb_project_catalog SET payload=? WHERE singleton=1`, next); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `COMMIT`)
	return err
}
