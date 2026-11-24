package remote

import (
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobidtest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

func TestJobIDRuntime(test *testing.T) {
	runPayloadAndRouteConformance(test, func(test *testing.T, harnesses ...runtimetest.Harness) {
		for _, harness := range harnesses {
			test.Run(harness.Name, func(test *testing.T) {
				fixture := harness.New(test)
				test.Cleanup(func() { require.NoError(test, fixture.Cleanup(test.Context())) })
				jobidtest.Run(test, fixture.Runtime, true)
			})
		}
	})
}
