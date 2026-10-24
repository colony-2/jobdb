package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/spf13/cobra"
)

func newGoalsCmd() *cobra.Command {
	var tenant, baseURL, message string
	root := &cobra.Command{Use: "goals", Short: "Inspect goals, add candidates, and record explicit failure", Args: cobra.NoArgs}
	root.PersistentFlags().StringVarP(&tenant, "tenant", "t", "", "tenant ID (required)")
	root.PersistentFlags().StringVarP(&baseURL, "url", "u", "http://127.0.0.1:9047", "runtime server URL")
	client := func() (*remote.Runtime, error) {
		if strings.TrimSpace(tenant) == "" {
			return nil, fmt.Errorf("--tenant is required")
		}
		base, err := url.Parse(baseURL)
		if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
			return nil, fmt.Errorf("--url must be an HTTP(S) server URL without credentials, query, or fragment")
		}
		return remote.New(strings.TrimRight(base.String(), "/"), &http.Client{Transport: configurationAuthTransport{token: os.Getenv("JOBDB_BEARER_TOKEN")}})
	}
	get := &cobra.Command{Use: "get GOAL_ID", Short: "Print a goal's committed state as JSON", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		rt, err := client()
		if err != nil {
			return err
		}
		out, err := rt.GetGoal(cmd.Context(), jobdb.GoalKey{TenantId: tenant, GoalID: args[0]})
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(out)
	}}
	add := &cobra.Command{Use: "add-candidate GOAL_ID JOB_ID", Short: "Add an existing job as a candidate (idempotent)", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		rt, err := client()
		if err != nil {
			return err
		}
		out, err := rt.AddGoalCandidate(cmd.Context(), jobdb.AddGoalCandidateRequest{GoalKey: jobdb.GoalKey{TenantId: tenant, GoalID: args[0]}, JobID: args[1]})
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(out)
	}}
	fail := &cobra.Command{Use: "fail GOAL_ID --message REASON", Short: "Fail a goal without cancelling its candidate jobs", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		rt, err := client()
		if err != nil {
			return err
		}
		out, err := rt.FailGoal(cmd.Context(), jobdb.FailGoalRequest{GoalKey: jobdb.GoalKey{TenantId: tenant, GoalID: args[0]}, Message: message})
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(out)
	}}
	fail.Flags().StringVarP(&message, "message", "m", "", "human explanation (required)")
	var pageToken string
	var pageSize int
	var hidden bool
	candidates := &cobra.Command{Use: "candidates GOAL_ID", Short: "Print one page of jobs associated with a goal", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		rt, err := client()
		if err != nil {
			return err
		}
		out, err := rt.ListJobs(cmd.Context(), jobdb.ListJobsRequest{TenantIds: []string{tenant}, GoalIDs: []string{args[0]}, IncludeHidden: hidden, PageToken: pageToken, PageSize: pageSize})
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(out)
	}}
	candidates.Flags().StringVar(&pageToken, "page-token", "", "continuation token")
	candidates.Flags().IntVar(&pageSize, "page-size", jobdb.DefaultListJobsPageSize, "maximum jobs per page")
	candidates.Flags().BoolVar(&hidden, "include-hidden", false, "include deleted jobs")
	root.AddCommand(get, add, fail, candidates)
	return root
}
