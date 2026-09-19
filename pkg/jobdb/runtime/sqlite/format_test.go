package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestOldDatabaseFormatIsRejectedWithoutConversion(t *testing.T) {
	db, err := sql.Open(sqliteDriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE old_jobs (value TEXT);INSERT INTO old_jobs VALUES ('untouched')`); err != nil {
		t.Fatal(err)
	}
	if err := migrate(context.Background(), db); err == nil || !strings.Contains(err.Error(), "fresh database") {
		t.Fatalf("old format accepted: %v", err)
	}
	var value string
	if err := db.QueryRow(`SELECT value FROM old_jobs`).Scan(&value); err != nil || value != "untouched" {
		t.Fatalf("old data changed: %s %v", value, err)
	}
}
