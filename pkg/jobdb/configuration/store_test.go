package configuration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestStores(t *testing.T) {
	t.Run("memory", func(t *testing.T) { s := &configuration.MemoryStore{}; exerciseStore(t, s, s) })
	t.Run("sqlite", func(t *testing.T) {
		dsn := filepath.Join(t.TempDir(), "config.db") + "?_pragma=busy_timeout(10000)"
		open := func() *configuration.SQLStore {
			db, err := sql.Open("sqlite", dsn)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			s, err := configuration.NewSQLStore(context.Background(), db, configuration.SQLite)
			require.NoError(t, err)
			return s
		}
		a, b := open(), open()
		exerciseStore(t, a, b)
		reopened := &configuration.Service{Store: open()}
		got, err := reopened.GetTenantConfiguration(context.Background(), "tenant")
		require.NoError(t, err)
		require.Contains(t, got.Values, "n")
	})
	t.Run("postgres", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		port := listener.Addr().(*net.TCPAddr).Port
		require.NoError(t, listener.Close())
		dir := t.TempDir()
		cfg := embeddedpostgres.DefaultConfig().Port(uint32(port)).DataPath(filepath.Join(dir, "data")).RuntimePath(filepath.Join(dir, "runtime")).Logger(io.Discard)
		pg := embeddedpostgres.NewDatabase(cfg)
		require.NoError(t, pg.Start())
		t.Cleanup(func() { require.NoError(t, pg.Stop()) })
		open := func() *configuration.SQLStore {
			db, err := sql.Open("postgres", cfg.GetConnectionURL())
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			s, err := configuration.NewSQLStore(context.Background(), db, configuration.Postgres)
			require.NoError(t, err)
			return s
		}
		exerciseStore(t, open(), open())
	})
}
func exerciseStore(t *testing.T, a, b configuration.Store) {
	t.Helper()
	ctx := context.Background()
	s := &configuration.Service{Store: a}
	other := &configuration.Service{Store: b}
	empty, err := s.GetTenantConfiguration(ctx, "tenant")
	require.NoError(t, err)
	require.NotNil(t, empty.Values)
	require.Empty(t, empty.Values)
	set := func(name, raw string) string {
		rev, err := s.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: name, Value: json.RawMessage(raw)})
		require.NoError(t, err)
		return rev
	}
	first := set("n", `9007199254740993123456789`)
	equal := set("n", `9007199254740993123456789.0`)
	require.Equal(t, first, equal)
	for k, v := range map[string]string{"null": "null", "false": "false", "zero": "0", "empty": `""`, "unicode\x00/key": `"a\u0000b"`, "object": `{"b":2,"a":[1,true]}`} {
		set(k, v)
	}
	before, err := other.GetTenantConfiguration(ctx, "tenant")
	require.NoError(t, err)
	require.Equal(t, `9007199254740993123456789`, string(before.Values["n"]))
	before.Values["n"][0] = '0'
	delete(before.Values, "false")
	again, err := s.GetTenantConfiguration(ctx, "tenant")
	require.NoError(t, err)
	require.Equal(t, byte('9'), again.Values["n"][0])
	require.Contains(t, again.Values, "false")
	isolated, err := s.GetTenantConfiguration(ctx, "other")
	require.NoError(t, err)
	require.Empty(t, isolated.Values)
	_, err = s.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: "n", Value: json.RawMessage(`2`), ExpectedRevision: &empty.Revision})
	require.ErrorIs(t, err, jobdb.ErrConfigurationRevisionMismatch)
	// Concurrent CAS from two handles: exactly one commits, even on different keys.
	start := make(chan struct{})
	results := make(chan error, 2)
	for i, service := range []*configuration.Service{s, other} {
		go func(i int, service *configuration.Service) {
			<-start
			_, e := service.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "tenant", Name: fmt.Sprint("cas", i), Value: json.RawMessage(`1`), ExpectedRevision: &again.Revision})
			results <- e
		}(i, service)
	}
	close(start)
	wins := 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			wins++
		} else {
			require.ErrorIs(t, err, jobdb.ErrConfigurationRevisionMismatch)
		}
	}
	require.Equal(t, 1, wins)
	// Many unconditioned writes cannot lose each other's updates.
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			service := s
			if i%2 == 1 {
				service = other
			}
			_, e := service.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "many", Name: fmt.Sprint(i), Value: json.RawMessage(fmt.Sprint(i))})
			errs <- e
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	many, err := other.GetTenantConfiguration(ctx, "many")
	require.NoError(t, err)
	require.Len(t, many.Values, 12)
	// Final deletion retains revision, avoiding ABA after recreation.
	rev, err := s.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "delete", Name: "x", Value: json.RawMessage(`1`)})
	require.NoError(t, err)
	deleted, err := s.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "delete", Name: "x"})
	require.NoError(t, err)
	require.NotEqual(t, rev, deleted)
	noop, err := other.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "delete", Name: "x"})
	require.NoError(t, err)
	require.Equal(t, deleted, noop)
	recreated, err := s.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "delete", Name: "x", Value: json.RawMessage(`1`)})
	require.NoError(t, err)
	require.NotEqual(t, rev, recreated)
	require.NotEqual(t, deleted, recreated)
	// Lower limits do not prevent removing entries to recover an oversized tenant.
	small := &configuration.Service{Store: a, Limits: jobdb.ConfigurationLimits{MaxEntries: 1}}
	_, err = small.GetTenantConfiguration(ctx, "many")
	require.ErrorIs(t, err, jobdb.ErrConfigurationReadTooLarge)
	_, err = small.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "many", Name: "extra", Value: json.RawMessage(`1`)})
	require.ErrorIs(t, err, jobdb.ErrConfigurationTooLarge)
	for i := 0; i < 11; i++ {
		_, err = small.RemoveTenantConfigurationValue(ctx, jobdb.RemoveTenantConfigurationValueRequest{TenantID: "many", Name: fmt.Sprint(i)})
		require.NoError(t, err)
	}
	_, err = small.GetTenantConfiguration(ctx, "many")
	require.NoError(t, err)
}
func TestValidationAndLimits(t *testing.T) {
	s := &configuration.Service{Store: &configuration.MemoryStore{}, Limits: jobdb.ConfigurationLimits{MaxValueBytes: 32, MaxNameBytes: 8, MaxEntries: 1, MaxSnapshotBytes: 128}}
	for _, raw := range []string{"", `{"x":1,"x":2}`, `{"a":{"b":1,"b":2}}`, `1 2`, `"\ud800"`, string([]byte{'"', 255, '"'})} {
		_, err := s.SetTenantConfigurationValue(context.Background(), jobdb.SetTenantConfigurationValueRequest{TenantID: "t", Name: "x", Value: json.RawMessage(raw)})
		require.ErrorIs(t, err, jobdb.ErrInvalidConfigurationRequest)
	}
	for _, req := range []jobdb.SetTenantConfigurationValueRequest{{TenantID: "t", Name: "long-name", Value: json.RawMessage(`1`)}, {TenantID: "t", Name: "x", Value: json.RawMessage(`"` + strings.Repeat("x", 33) + `"`)}} {
		_, err := s.SetTenantConfigurationValue(context.Background(), req)
		require.ErrorIs(t, err, jobdb.ErrConfigurationTooLarge)
	}
	// Concurrent aggregate-limit enforcement must admit only one key.
	errs := make(chan error, 2)
	for _, name := range []string{"a", "b"} {
		go func(name string) {
			_, e := s.SetTenantConfigurationValue(context.Background(), jobdb.SetTenantConfigurationValueRequest{TenantID: "t", Name: name, Value: json.RawMessage(`1`)})
			errs <- e
		}(name)
	}
	wins := 0
	for i := 0; i < 2; i++ {
		err := <-errs
		if err == nil {
			wins++
		} else {
			require.ErrorIs(t, err, jobdb.ErrConfigurationTooLarge)
		}
	}
	require.Equal(t, 1, wins)
	for _, tag := range []string{"", `*`, `W/"cfg-0"`, `"cfg-0", "cfg-1"`, `""`, `cfg-0`} {
		_, err := configuration.ParseETag(tag)
		require.ErrorIs(t, err, jobdb.ErrInvalidConfigurationRequest)
	}
}

func TestConfigurationValueEncodingBounds(t *testing.T) {
	limits := jobdb.ConfigurationLimits{MaxValueBytes: 16}
	require.ErrorIs(t, configuration.ValidateValue(json.RawMessage(`"<<<<"`), limits), jobdb.ErrConfigurationTooLarge)
	for _, depth := range []int{126, 127} {
		raw := json.RawMessage(strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth))
		err := configuration.ValidateValue(raw, jobdb.ConfigurationLimits{})
		if depth == 126 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, jobdb.ErrInvalidConfigurationRequest)
		}
	}
}
