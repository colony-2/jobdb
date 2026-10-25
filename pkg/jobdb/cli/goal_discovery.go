package cli

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/spf13/cobra"
)

func addGoalDiscoveryCommands(root *cobra.Command, tenant *string, client func() (*remote.Runtime, error)) {
	var goals jobdb.ListGoalsRequest
	var states, jobTypes, jobIDs []string
	var createdAfter, createdBefore, updatedAfter, updatedBefore string
	list := &cobra.Command{Use: "list", Short: "List tenant goals with authoritative state and candidate counts", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		rt, err := client()
		if err != nil {
			return err
		}
		req := goals
		req.TenantId = *tenant
		for _, state := range states {
			req.States = append(req.States, jobdb.GoalState(state))
		}
		for _, bound := range []struct {
			raw string
			dst **time.Time
		}{{createdAfter, &req.CreatedAfter}, {createdBefore, &req.CreatedBefore}, {updatedAfter, &req.UpdatedAfter}, {updatedBefore, &req.UpdatedBefore}} {
			if bound.raw != "" {
				v, err := time.Parse(time.RFC3339Nano, bound.raw)
				if err != nil {
					return err
				}
				*bound.dst = &v
			}
		}
		if len(jobTypes) > 0 || len(jobIDs) > 0 {
			req.Candidate = &jobdb.GoalCandidateFilter{JobTypes: jobTypes, JobIDs: jobIDs}
		}
		out, err := rt.ListGoals(cmd.Context(), req)
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(out)
	}}
	list.Flags().StringSliceVar(&goals.GoalIDs, "goal-id", nil, "exact goal IDs")
	list.Flags().StringSliceVar(&states, "state", nil, "OPEN, SUCCEEDED, FAILED")
	list.Flags().StringSliceVar(&jobTypes, "candidate-type", nil, "goals with a candidate of these job types")
	list.Flags().StringSliceVar(&jobIDs, "candidate-job", nil, "goals containing these candidate job IDs")
	list.Flags().StringVar(&createdAfter, "created-after", "", "inclusive RFC3339 creation bound")
	list.Flags().StringVar(&createdBefore, "created-before", "", "inclusive RFC3339 creation bound")
	list.Flags().StringVar(&updatedAfter, "updated-after", "", "inclusive RFC3339 goal-record update bound")
	list.Flags().StringVar(&updatedBefore, "updated-before", "", "inclusive RFC3339 goal-record update bound")
	list.Flags().IntVar(&goals.PageSize, "page-size", 100, "maximum goals per page")
	list.Flags().StringVar(&goals.PageToken, "page-token", "", "continuation token")

	var waits jobdb.ListDependencyWaitsRequest
	var waitStates []string
	var goalTarget, jobTarget string
	wait := &cobra.Command{Use: "waits", Short: "Inspect consumer waits or currently unresolved reverse dependencies", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		rt, err := client()
		if err != nil {
			return err
		}
		req := waits
		req.TenantId = *tenant
		if goalTarget != "" && jobTarget != "" {
			return fmt.Errorf("choose --target-goal or --target-job")
		}
		if goalTarget != "" || jobTarget != "" {
			req.Target = &jobdb.DependencyTarget{GoalID: goalTarget, JobID: jobTarget}
		}
		for _, state := range waitStates {
			req.States = append(req.States, jobdb.DependencyState(state))
		}
		out, err := rt.ListDependencyWaits(cmd.Context(), req)
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(out)
	}}
	wait.Flags().StringVar(&waits.ConsumerJobID, "consumer-job", "", "consumer job ID (includes retained history)")
	wait.Flags().StringVar(&goalTarget, "target-goal", "", "goal whose unresolved consumers to list")
	wait.Flags().StringVar(&jobTarget, "target-job", "", "job whose unresolved consumers to list")
	wait.Flags().StringSliceVar(&waitStates, "state", nil, "wait states; reverse lookup supports PENDING only")
	wait.Flags().IntVar(&waits.PageSize, "page-size", 100, "maximum waits per page")
	wait.Flags().StringVar(&waits.PageToken, "page-token", "", "continuation token")
	root.AddCommand(list, wait)
}
