package workflow_test

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func alternateRuntime(t *testing.T, backend string) jobdb.WorkflowRuntime {
	t.Helper()
	var rt jobdb.WorkflowRuntime = toy.New()
	if backend != "toy" {
		s, err := sqlite.NewFromConfig(context.Background(), sqlite.Config{DBPath: filepath.Join(t.TempDir(), "job.db")})
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close(context.Background()) })
		rt = s
	}
	if backend == "remote" {
		server := httptest.NewServer(remote.NewServer(rt))
		t.Cleanup(server.Close)
		r, err := remote.New(server.URL, server.Client())
		require.NoError(t, err)
		rt = r
	}
	return rt
}

func waitAlternate(at time.Time) {
	if d := time.Until(at); d > 0 {
		time.Sleep(d)
	}
}
