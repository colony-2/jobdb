package jobdb_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func ExampleExecutionInfrastructureError() {
	cause := errors.New("storage unavailable")
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	err := jobdb.ClassifyExecutionInfrastructure(cause, jobdb.ExecutionInfrastructureRuntimeIO, "get_chapter", &key, nil)
	var failure *jobdb.ExecutionInfrastructureError
	if errors.As(err, &failure) {
		fmt.Println(failure.Reason, failure.Ordinal == nil, errors.Is(err, cause))
	}
	// Output: runtime_io true true
}

func ExampleExecutionProvenanceReader() {
	// Use the runtime that executed the job. This helper forwards optional support
	// and returns ErrExecutionProvenanceUnsupported when it is absent.
	var runtime jobdb.WorkflowRuntime
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	evidence, lookupErr := jobdb.ReadExecutionProvenance(ctx, runtime, jobdb.ReadExecutionProvenanceRequest{
		JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, Consumer: "c2j",
	})
	// A failed diagnostic must never replace the original execution error.
	_, _ = evidence, lookupErr
}

func ExampleCurrentBuildInfo() {
	linked := jobdb.CurrentBuildInfo()
	// The linked JobDB dependency version is independent of the host's VCS ID.
	_, _, _ = linked.Version, linked.Revision, linked.Replacement
}
