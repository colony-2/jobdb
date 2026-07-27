package runtimeconformance_test

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
)

func TestBuiltInRuntimeConformance(t *testing.T) {
	runtimetest.RunWorkflowRuntimeConformance(t, adaptHarnesses(jobdbtest.BuiltInRuntimeHarnesses())...)
}

func TestRemoteRuntimeConformance(t *testing.T) {
	runtimetest.RunWorkflowRuntimeConformance(t, adaptHarnesses(jobdbtest.RemoteRuntimeHarnesses())...)
}

func adaptHarnesses(in []jobdbtest.RuntimeHarness) []runtimetest.Harness {
	out := make([]runtimetest.Harness, 0, len(in))
	for _, item := range in {
		item := item
		out = append(out, runtimetest.Harness{
			Name: item.Name,
			Capabilities: runtimetest.Capabilities{
				Leases:         item.SupportsLeases,
				Schedules:      true,
				SchemaRegistry: true,
				RuntimeStorage: item.SupportsRuntimeStorage,
			},
			New: func(t testing.TB) runtimetest.Fixture {
				tt, ok := t.(*testing.T)
				if !ok {
					t.Fatalf("runtime conformance fixture requires *testing.T")
				}
				built := item.New(tt)
				return runtimetest.Fixture{
					Runtime:        built.Runtime,
					WorkerTenantID: built.WorkerTenantID,
					Cleanup: func(context.Context) error {
						built.Shutdown(tt)
						return nil
					},
				}
			},
		})
	}
	return out
}
