package runtimetest

import (
	"context"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"os"
	"os/exec"
	"strings"
	"testing"
)

type forwardingEventWrapper struct{ jobdb.WorkflowRuntime }
type failingEventWrapper struct {
	jobdb.WorkflowRuntime
	err error
}

func (r failingEventWrapper) ListLeaseEvents(context.Context, jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	return jobdb.ListLeaseEventsResponse{}, r.err
}

func TestLeaseEventConformanceCompatibility(t *testing.T) {
	mode := os.Getenv("JOBDB_EVENT_CONFORMANCE_MODE")
	if mode != "" {
		h := Harness{Name: "probe", New: func(testing.TB) Fixture {
			native := toy.New()
			var r jobdb.WorkflowRuntime = native
			if strings.Contains(mode, "forwarded") {
				r = forwardingEventWrapper{r}
			}
			if strings.Contains(mode, "failure") {
				r = failingEventWrapper{WorkflowRuntime: r, err: errors.New("probe backend failure")}
			}
			if strings.Contains(mode, "unsupported") {
				r = failingEventWrapper{WorkflowRuntime: r, err: jobdb.ErrLeaseEventsUnsupported}
			}
			return Fixture{Runtime: r, LeaseEventLeaseValidity: native.ValidateExecutionLeaseByID}
		}}
		if strings.HasPrefix(mode, "strict") {
			RunLeaseEventConformance(t, h)
		} else {
			RunWorkflowRuntimeConformance(t, h)
		}
		return
	}
	for _, tc := range []struct {
		mode    string
		fails   bool
		message string
	}{
		{"general-forwarded", false, "PASS"},
		{"general-default", false, "PASS"},
		{"general-failure", true, "probe backend failure"},
		{"general-unsupported", true, "lease_events_unsupported"},
		{"strict-failure", true, "probe backend failure"},
		{"strict-unsupported", true, "lease_events_unsupported"},
		{"strict-forwarded", false, "PASS"},
		{"strict-default", false, "PASS"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			filter := "^TestLeaseEventConformanceCompatibility$"
			if strings.HasPrefix(tc.mode, "general") {
				filter += "/^lease_events$"
			}
			cmd := exec.Command(os.Args[0], "-test.v", "-test.run="+filter)
			cmd.Env = append(os.Environ(), "JOBDB_EVENT_CONFORMANCE_MODE="+tc.mode)
			out, e := cmd.CombinedOutput()
			if (e != nil) != tc.fails || !strings.Contains(string(out), tc.message) {
				t.Fatalf("mode %s: err=%v, output=%s", tc.mode, e, out)
			}
		})
	}
}
