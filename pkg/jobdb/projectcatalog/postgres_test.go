//go:build integration

package projectcatalog

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestProjectPostgresBorrowedHandles(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	dir := t.TempDir()
	cfg := embeddedpostgres.DefaultConfig().Port(uint32(port)).DataPath(filepath.Join(dir, "data")).RuntimePath(filepath.Join(dir, "runtime")).Logger(io.Discard)
	pg := embeddedpostgres.NewDatabase(cfg)
	require.NoError(t, pg.Start())
	t.Cleanup(func() { require.NoError(t, pg.Stop()) })
	ctx := context.Background()
	open := func() *SQLStore {
		db, err := sql.Open("postgres", cfg.GetConnectionURL())
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		_, err = db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS project_fixture`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `SET search_path TO project_fixture`)
		require.NoError(t, err)
		s, err := NewSQLStore(db, Postgres)
		require.NoError(t, err)
		return s
	}
	a, b := open(), open()
	require.NoError(t, a.CreateSchema(ctx))
	first, second := &Service{Store: a, CanDelete: func(context.Context, string) error { return nil }}, &Service{Store: b}
	require.NoError(t, first.Install(admin(), Installation{Manifest: InstallationDigest([]byte("PG explicit installation")), Seeds: []Seed{{ID: "c2", Name: "c2"}}}))
	q := jobdb.CreateProjectRequest{Name: "parallel", RequestID: "equal"}
	start := make(chan struct{})
	errs := make(chan error, 2)
	receipts := make(chan jobdb.ProjectCreation, 2)
	for _, s := range []*Service{first, second} {
		go func(s *Service) { <-start; r, e := s.CreateProject(admin(), q); receipts <- r; errs <- e }(s)
	}
	close(start)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	one, two := <-receipts, <-receipts
	require.Equal(t, one, two)
	require.NoError(t, first.DeleteProject(admin(), jobdb.DeleteProjectRequest{ID: one.Project.ID, IfMatch: one.ETag}))
	again, err := second.CreateProject(admin(), q)
	require.NoError(t, err)
	require.Equal(t, one, again)
	// Callback failure rolls back the entire document and all receipt changes.
	sentinel := errors.New("abort")
	require.ErrorIs(t, b.Update(ctx, func(p **State) error { delete((*p).Receipts, "equal"); return sentinel }), sentinel)
	again, err = first.CreateProject(admin(), q)
	require.NoError(t, err)
	require.Equal(t, one, again)
	replacement := create(t, first, "sealed")
	require.NoError(t, second.AdmitWrite(ctx, replacement.Project.ID))
	require.ErrorIs(t, first.DeleteProject(admin(), jobdb.DeleteProjectRequest{ID: replacement.Project.ID, IfMatch: replacement.ETag}), jobdb.ErrProjectConflict)
	reopened := &Service{Store: open()}
	old, err := reopened.GetProject(admin(), "c2")
	require.NoError(t, err)
	require.Equal(t, "c2", old.ID)
}
