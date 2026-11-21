package remote

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

// These are genuine common optional ports, not a full-runtime embedding. The
// direct backend's transport ports are forwarded separately by consumerFixture.
type retainedConformanceRuntime struct {
	jobdb.ConsumerRuntime
	jobdb.ConfigurationAdmin
	jobdb.JobSchemaRegistry
	jobdb.JobDeletionRuntime
}

func TestRetainedConsumerConformance(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		for _, transport := range []string{"direct", "http", "http-options"} {
			t.Run(kind+"-"+transport, func(t *testing.T) {
				fixture := func(tb testing.TB) runtimetest.Fixture {
					// Own storage and servers in the individual suite case; workers
					// shut down before the case releases these resources.
					tt, ok := tb.(*testing.T)
					require.True(tb, ok)
					backend := consumerFixture(tt, consumerBackend(tt, kind))
					var rt jobdb.ConsumerRuntime = backend
					if transport != "direct" {
						handler := NewConsumerServer(backend)
						if transport == "http-options" {
							var err error
							handler, err = NewConsumerServerWithOptions(backend, ServerOptions{LeaseTokenSigningKey: []byte(strings.Repeat("k", 32))})
							require.NoError(tb, err)
						}
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							handler.ServeHTTP(w, r.WithContext(runtimetest.PrimaryGoalContext(r.Context())))
						}))
						tb.Cleanup(server.Close)
						client, err := New(server.URL, server.Client())
						require.NoError(tb, err)
						rt = client
					}
					var view jobdb.ConsumerRuntime = backend
					if transport != "direct" {
						common := retainedConformanceRuntime{
							ConsumerRuntime:    rt,
							ConfigurationAdmin: rt.(jobdb.ConfigurationAdmin),
							JobSchemaRegistry:  rt.(jobdb.JobSchemaRegistry),
							JobDeletionRuntime: rt.(jobdb.JobDeletionRuntime),
						}
						view = common
						if delegator, ok := rt.(jobdb.LeaseDelegator); ok {
							view = struct {
								retainedConformanceRuntime
								jobdb.LeaseDelegator
							}{common, delegator}
						}
					}
					for _, typ := range []reflect.Type{reflect.TypeOf(view), reflect.PointerTo(reflect.TypeOf(view))} {
						_, found := typ.MethodByName("AddGoalCandidate")
						require.False(tb, found)
					}
					f := runtimetest.Fixture{Runtime: view, SchemaRegistry: view.(jobdb.JobSchemaRegistry)}
					if transport != "direct" {
						f.LeaseEventLeaseValidity = backend.ValidateExecutionLeaseByID
					}
					return f
				}
				h := runtimetest.Harness{Name: kind + "-" + transport, Capabilities: runtimetest.Capabilities{Leases: true, SchemaRegistry: true, RuntimeStorage: true}, New: fixture}
				t.Run("workflow", func(t *testing.T) { runtimetest.RunWorkflowRuntimeConformance(t, h) })
				t.Run("compact", func(t *testing.T) { runtimetest.RunCompactListConformance(t, h) })
				t.Run("cells", func(t *testing.T) { runtimetest.RunCellConformance(t, fixture(t).Runtime) })
				t.Run("configuration", func(t *testing.T) { runtimetest.RunConfigurationConformance(t, fixture(t).Runtime) })
			})
		}
	}
}
