package remote

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"

	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"

	toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
)

func TestCompactListConformance(t *testing.T) {
	runPayloadAndRouteConformance(t, runtimetest.RunCompactListConformance)
}

func TestClientPayloadConformance(t *testing.T) {
	runPayloadAndRouteConformance(t, runtimetest.RunClientPayloadConformance)
}
func TestAvailableRoutesConformance(t *testing.T) {
	runPayloadAndRouteConformance(t, runtimetest.RunAvailableRoutesConformance)
}
func TestRouteConformance(t *testing.T) {
	runPayloadAndRouteConformance(t, runtimetest.RunRouteConformance)
}
func runPayloadAndRouteConformance(t *testing.T, run func(*testing.T, ...runtimetest.Harness)) {
	for _, kind := range []string{"toy", "sqlite"} {
		for _, remote := range []bool{false, true} {
			name := kind
			if remote {
				name = "remote-" + name
			}
			run(t, runtimetest.Harness{Name: name, New: func(tb testing.TB) runtimetest.Fixture {
				var runtime jobdb.WorkflowRuntime = toyruntime.New()
				cleanup := func() {}
				if kind == "sqlite" {
					embedded, err := sqliteruntime.NewFromConfig(celltest.Context(context.Background()), sqliteruntime.Config{DBPath: tb.TempDir() + "/jobdb.db"})
					if err != nil {
						tb.Fatal(err)
					}
					runtime = embedded
					cleanup = func() { _ = embedded.Close(context.Background()) }
				}
				var validity func(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
				if remote {
					validator, ok := runtime.(interface {
						ValidateExecutionLeaseByID(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
					})
					if !ok {
						tb.Fatal("native backend must provide lease validation")
					}
					validity = validator.ValidateExecutionLeaseByID
					server := httptest.NewServer(celltest.Handler(NewServer(runtime)))
					inner := cleanup
					cleanup = func() { server.Close(); inner() }
					client, err := New(server.URL, server.Client())
					if err != nil {
						tb.Fatal(err)
					}
					runtime = client
				}
				return runtimetest.Fixture{Runtime: runtime, LeaseEventLeaseValidity: validity, Cleanup: func(context.Context) error { cleanup(); return nil }}
			}})
		}
	}
}
