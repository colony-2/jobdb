package projectcatalog

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func openSQLite(t *testing.T, path string) *SQLStore {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := NewSQLStore(db, SQLite)
	require.NoError(t, err)
	return store
}
func TestProjectSQLSerializationAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	a, b := openSQLite(t, path), openSQLite(t, path)
	require.NoError(t, a.CreateSchema(context.Background()))
	first, other := &Service{Store: a}, &Service{Store: b}
	in := Installation{Manifest: InstallationDigest([]byte("empty SQL fixture"))}
	require.NoError(t, first.Install(admin(), in))
	q := jobdb.CreateProjectRequest{Name: "raced", RequestID: "same"}
	start := make(chan struct{})
	results := make(chan jobdb.ProjectCreation, 2)
	errs := make(chan error, 2)
	for _, s := range []*Service{first, other} {
		go func(s *Service) { <-start; r, e := s.CreateProject(admin(), q); results <- r; errs <- e }(s)
	}
	close(start)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	one, two := <-results, <-results
	require.Equal(t, one, two)
	// Two different RequestIDs racing one name must not leave a losing receipt.
	start = make(chan struct{})
	for i, s := range []*Service{first, other} {
		go func(i int, s *Service) {
			<-start
			r, e := s.CreateProject(admin(), jobdb.CreateProjectRequest{Name: "unique", RequestID: []string{"one", "two"}[i]})
			results <- r
			errs <- e
		}(i, s)
	}
	close(start)
	e1, e2 := <-errs, <-errs
	require.True(t, (e1 == nil && errors.Is(e2, jobdb.ErrProjectConflict)) || (e2 == nil && errors.Is(e1, jobdb.ErrProjectConflict)))
	<-results
	<-results
	reopened := &Service{Store: openSQLite(t, path)}
	retry, err := reopened.CreateProject(admin(), q)
	require.NoError(t, err)
	require.Equal(t, one, retry)
	require.NoError(t, reopened.AdmitWrite(context.Background(), one.Project.ID))
	other.CanDelete = func(context.Context, string) error { return nil }
	require.ErrorIs(t, other.DeleteProject(admin(), jobdb.DeleteProjectRequest{ID: one.Project.ID, IfMatch: one.ETag}), jobdb.ErrProjectConflict)
	require.NoError(t, reopened.Install(admin(), in))
}
func TestProjectAdmissionDeleteOrdering(t *testing.T) {
	for _, sqlite := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "sqlite"}[sqlite], func(t *testing.T) {
			var store Store = &MemoryStore{}
			if sqlite {
				s := openSQLite(t, filepath.Join(t.TempDir(), "race.db"))
				require.NoError(t, s.CreateSchema(context.Background()))
				store = s
			}
			s := &Service{Store: store}
			require.NoError(t, s.Install(admin(), Installation{Manifest: InstallationDigest([]byte("race fixture"))}))
			p := create(t, s, "deletion-wins")
			probed, release := make(chan struct{}), make(chan struct{})
			s.CanDelete = func(context.Context, string) error { close(probed); <-release; return nil }
			deleted := make(chan error, 1)
			go func() {
				deleted <- s.DeleteProject(admin(), jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag})
			}()
			<-probed
			admission := make(chan error, 1)
			go func() { admission <- s.AdmitWrite(context.Background(), p.Project.ID) }()
			close(release)
			require.NoError(t, <-deleted)
			require.ErrorIs(t, <-admission, jobdb.ErrProjectNotFound)
			s.CanDelete = func(context.Context, string) error { return nil }
			p = create(t, s, "admission-wins")
			require.NoError(t, s.AdmitWrite(context.Background(), p.Project.ID))
			require.ErrorIs(t, s.DeleteProject(admin(), jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}), jobdb.ErrProjectConflict)
		})
	}
}

type uncertainStore struct {
	Store
	fail bool
}

func (s *uncertainStore) Update(ctx context.Context, f func(**State) error) error {
	err := s.Store.Update(ctx, f)
	if err == nil && s.fail {
		return errors.New("reply lost after commit")
	}
	return err
}
func TestProjectLostReplyRetainsReceiptAndSeal(t *testing.T) {
	base := fresh(t)
	fault := &uncertainStore{Store: base.Store, fail: true}
	s := &Service{Store: fault}
	q := jobdb.CreateProjectRequest{Name: "lost", RequestID: "stable"}
	_, err := s.CreateProject(admin(), q)
	require.Error(t, err)
	fault.fail = false
	receipt, err := s.CreateProject(admin(), q)
	require.NoError(t, err)
	fault.fail = true
	require.Error(t, s.AdmitWrite(context.Background(), receipt.Project.ID))
	fault.fail = false
	s.CanDelete = func(context.Context, string) error { return nil }
	require.ErrorIs(t, s.DeleteProject(admin(), jobdb.DeleteProjectRequest{ID: receipt.Project.ID, IfMatch: receipt.ETag}), jobdb.ErrProjectConflict)
}
func TestProjectProcessCrash(t *testing.T) {
	if path := os.Getenv("JOBDB_PROJECT_CRASH_DB"); path != "" {
		store := openSQLite(t, path)
		s := &Service{Store: store}
		if os.Getenv("JOBDB_PROJECT_CRASH_STAGE") == "transaction" {
			_ = store.Update(context.Background(), func(p **State) error { (*p).Projects["partial"] = record{}; os.Exit(77); return nil })
		} else {
			_, err := s.CreateProject(admin(), jobdb.CreateProjectRequest{Name: "committed", RequestID: "crash"})
			if err != nil {
				os.Exit(78)
			}
			os.Exit(77)
		}
		return
	}
	path := filepath.Join(t.TempDir(), "crash.db")
	store := openSQLite(t, path)
	require.NoError(t, store.CreateSchema(context.Background()))
	s := &Service{Store: store}
	require.NoError(t, s.Install(admin(), Installation{Manifest: InstallationDigest([]byte("crash fixture"))}))
	for _, stage := range []string{"transaction", "committed"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestProjectProcessCrash$")
		cmd.Env = append(os.Environ(), "JOBDB_PROJECT_CRASH_DB="+path, "JOBDB_PROJECT_CRASH_STAGE="+stage)
		output, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit, string(output))
		require.Equal(t, 77, exit.ExitCode(), string(output))
		state, err := store.Read(context.Background())
		require.NoError(t, err)
		require.NotContains(t, state.Projects, "partial")
		if stage == "transaction" {
			require.Empty(t, state.Projects)
			require.Empty(t, state.Receipts)
		} else {
			r, err := s.CreateProject(admin(), jobdb.CreateProjectRequest{Name: "committed", RequestID: "crash"})
			require.NoError(t, err)
			require.Equal(t, state.Receipts["crash"].Result, r)
			require.Len(t, state.Projects, 1)
			require.Len(t, state.Receipts, 1)
		}
	}
}
