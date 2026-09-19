package remote

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
)

func TestClientPayloadConformance(t *testing.T) {
	runPayloadAndRouteConformance(t, runtimetest.RunClientPayloadConformance)
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
					embedded, err := sqliteruntime.StartEmbeddedRuntime(context.Background())
					if err != nil {
						tb.Fatal(err)
					}
					runtime = embedded.Runtime
					cleanup = embedded.Shutdown
				}
				if remote {
					server := httptest.NewServer(NewServer(runtime))
					inner := cleanup
					cleanup = func() { server.Close(); inner() }
					client, err := New(server.URL, server.Client())
					if err != nil {
						tb.Fatal(err)
					}
					runtime = client
				}
				return runtimetest.Fixture{Runtime: runtime, Cleanup: func(context.Context) error { cleanup(); return nil }}
			}})
		}
	}
}
