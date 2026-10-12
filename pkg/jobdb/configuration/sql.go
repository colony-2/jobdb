package configuration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type SQLDialect string

const (
	SQLite   SQLDialect = "sqlite"
	Postgres SQLDialect = "postgres"
)

// SQLStore stores revision and values in one row, so reads are coherent even at
// READ COMMITTED. Update serializes writers with a row lock (Postgres) or a write
// transaction (SQLite). The caller owns the database and its lifecycle.
type SQLStore struct {
	db      *sql.DB
	dialect SQLDialect
}

// NewSQLStore initializes the additive configuration table in a runtime database.
// JSON is stored as bytes (not JSONB), preserving arbitrary JSON numbers exactly.
func NewSQLStore(ctx context.Context, db *sql.DB, dialect SQLDialect) (*SQLStore, error) {
	if db == nil {
		return nil, fmt.Errorf("configuration database is required")
	}
	typ := "BLOB"
	switch dialect {
	case SQLite:
	case Postgres:
		typ = "BYTEA"
	default:
		return nil, fmt.Errorf("unsupported configuration SQL dialect")
	}
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS jobdb_configuration (tenant_id TEXT PRIMARY KEY, snapshot `+typ+` NOT NULL)`)
	if err != nil {
		return nil, SafeError(err)
	}
	return &SQLStore{db: db, dialect: dialect}, nil
}
func (s *SQLStore) Read(ctx context.Context, tenant string) (jobdb.ConfigurationSnapshot, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT snapshot FROM jobdb_configuration WHERE tenant_id = $1", tenant).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Empty(), nil
	}
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, sqlError(err)
	}
	return decodeStored(raw)
}
func decodeStored(raw []byte) (jobdb.ConfigurationSnapshot, error) {
	var v jobdb.ConfigurationSnapshot
	if err := ReadJSON(bytes.NewReader(raw), len(raw), &v); err != nil || v.Values == nil || !ValidateRevision(v.Revision) {
		return jobdb.ConfigurationSnapshot{}, jobdb.ErrConfigurationFailed
	}
	return v, nil
}
func (s *SQLStore) Update(ctx context.Context, tenant string, change func(jobdb.ConfigurationSnapshot) (jobdb.ConfigurationSnapshot, error)) (jobdb.ConfigurationSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, sqlError(err)
	}
	defer tx.Rollback()
	empty, _ := json.Marshal(Empty())
	// This is the first statement: it also acquires SQLite's write lock before any
	// read, avoiding deferred read-to-write upgrades and lost updates across handles.
	if _, err = tx.ExecContext(ctx, "INSERT INTO jobdb_configuration (tenant_id, snapshot) VALUES ($1, $2) ON CONFLICT (tenant_id) DO NOTHING", tenant, empty); err != nil {
		return jobdb.ConfigurationSnapshot{}, sqlError(err)
	}
	query := "SELECT snapshot FROM jobdb_configuration WHERE tenant_id = $1"
	if s.dialect == Postgres {
		query += " FOR UPDATE"
	}
	var raw []byte
	if err = tx.QueryRowContext(ctx, query, tenant).Scan(&raw); err != nil {
		return jobdb.ConfigurationSnapshot{}, sqlError(err)
	}
	current, err := decodeStored(raw)
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	next, err := change(current)
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	raw, err = json.Marshal(next)
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, jobdb.ErrConfigurationFailed
	}
	if _, err = tx.ExecContext(ctx, "UPDATE jobdb_configuration SET snapshot = $2 WHERE tenant_id = $1", tenant, raw); err != nil {
		return jobdb.ConfigurationSnapshot{}, sqlError(err)
	}
	if err = tx.Commit(); err != nil {
		return jobdb.ConfigurationSnapshot{}, sqlError(err)
	}
	return next, nil
}

// Driver errors expose numeric SQL states/codes; never inspect or expose their text.
func sqlError(err error) error {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		switch state.SQLState() {
		case "40001", "40P01", "55P03", "57014", "53300", "57P01", "08000", "08003", "08006":
			return jobdb.ErrConfigurationUnavailable
		}
	}
	var code interface{ Code() int }
	if errors.As(err, &code) && (code.Code()&255 == 5 || code.Code()&255 == 6) {
		return jobdb.ErrConfigurationUnavailable
	}
	return SafeError(err)
}
