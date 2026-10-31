package workflow_test

import (
	"context"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
)

func ExampleReadCatchHistory() {
	inspect := func(ctx context.Context, runtime jobdb.WorkflowRuntime, key jobdb.JobKey) error {
		history, err := workflow.ReadCatchHistory(ctx, runtime, jobdb.GetJobRunRequest{JobKey: key})
		if err != nil {
			return err
		}
		for _, a := range history.Attempts {
			if a.Selection != workflow.CatchSelectionSelected || a.Handling != workflow.CatchHandlingAccepted || a.Accepted == nil || a.Accepted.Disposition != workflow.CatchHandled || a.Accepted.Failure != nil {
				continue
			}
			fmt.Printf("boundary=%s attempt=%s rule=%s handled\n", a.Boundary.BoundaryID, a.Boundary.AttemptID, a.RuleID)
		}
		return nil
	}
	// Pass an existing configured runtime and JobKey to inspect. Polling the same
	// function refreshes active runs; historical inspection needs no workers.
	_ = inspect
}
