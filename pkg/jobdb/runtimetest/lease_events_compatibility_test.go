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

type legacyEventWrapper struct{ jobdb.WorkflowRuntime }
type failingEventWrapper struct{ jobdb.WorkflowRuntime }

func (failingEventWrapper) ListLeaseEvents(context.Context, jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	return jobdb.ListLeaseEventsResponse{}, errors.New("probe backend failure")
}

func TestLeaseEventConformanceCompatibility(t *testing.T) {
	mode := os.Getenv("JOBDB_EVENT_CONFORMANCE_MODE")
	if mode != "" {
		h := Harness{Name: "probe", New: func(testing.TB) Fixture {
			var r jobdb.WorkflowRuntime = toy.New()
			if strings.Contains(mode, "absent") {
				r = legacyEventWrapper{r}
			}
			if strings.Contains(mode, "failure") {
				r = failingEventWrapper{r}
			}
			return Fixture{Runtime: r}
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
		{"general-absent", false, "optional lease events unsupported"},
		{"general-default", false, "PASS"},
		{"general-failure", true, "probe backend failure"},
		{"strict-absent", true, "lease_events_unsupported"},
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
