package sqlite_test

import "github.com/colony-2/jobdb/pkg/jobdb/celltest"

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestPrimaryGoalDirectConformance(t *testing.T) {
	t.Run("toy", func(t *testing.T) {
		r := toy.New(toy.WithProjects(celltest.UnrestrictedProjects{}))
		runtimetest.RunPrimaryGoalSelectionConformance(t, r)
		runtimetest.RunPrimaryGoalSelectionBoundaries(t, r)
	})
	t.Run("sqlite", func(t *testing.T) {
		embedded, err := sqlite.StartEmbeddedRuntime(context.Background(), sqlite.WithProjects(celltest.UnrestrictedProjects{}))
		require.NoError(t, err)
		defer embedded.Shutdown()
		runtimetest.RunPrimaryGoalSelectionConformance(t, embedded.Runtime)
		runtimetest.RunPrimaryGoalSelectionBoundaries(t, embedded.Runtime)
	})
}
