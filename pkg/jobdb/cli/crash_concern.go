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

func newClearCrashConcernCmd() *cobra.Command {
	var tenant, baseURL, reason, worker string
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:     "clear-crash-concern JOB_ID",
		Aliases: []string{"reset-crash-concern"},
		Short:   "Clear a job's crash concern so workers can acquire it again",
		Long: `Reset the consecutive expired-lease count without changing job history.
Live leases, cancelled, completed, and hidden jobs cannot be cleared.
Dependencies, scheduled availability, deadlines, and routing remain in effect.
Set JOBDB_BEARER_TOKEN if your deployment requires bearer authentication.`,
		Example: "  jobdb jobs clear-crash-concern -t c2 JOB_ID --reason 'worker issue fixed'",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			req := jobdb.ClearCrashConcernRequest{JobKey: jobdb.JobKey{TenantId: tenant, JobId: args[0]}, Reason: reason, WorkerID: worker}
			if err := req.Validate(); err != nil {
				return fmt.Errorf("--tenant (-t) and JOB_ID are required: %w", err)
			}
			base, err := url.Parse(baseURL)
			if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
				return fmt.Errorf("--url (-u) must be an HTTP(S) server URL without credentials, query, or fragment")
			}
			client := &http.Client{Transport: configurationAuthTransport{token: os.Getenv("JOBDB_BEARER_TOKEN")}}
			rt, err := remote.New(strings.TrimRight(base.String(), "/"), client)
			if err != nil {
				return err
			}
			if err := rt.ClearCrashConcern(cmd.Context(), req); err != nil {
				return err
			}
			if jsonOutput {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
					TenantID string `json:"tenantId"`
					JobID    string `json:"jobId"`
					Cleared  bool   `json:"cleared"`
				}{tenant, args[0], true})
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Cleared crash concern for job %s/%s.\n", tenant, args[0])
			return err
		},
	}
	cmd.Flags().StringVarP(&tenant, "tenant", "t", "", "tenant ID (required)")
	cmd.Flags().StringVarP(&baseURL, "url", "u", "http://127.0.0.1:9047", "runtime server URL")
	cmd.Flags().StringVar(&reason, "reason", "", "operator's reason for clearing crash concern")
	cmd.Flags().StringVar(&worker, "worker-id", "jobdb-cli", "operator identifier for backend tracing")
	cmd.Flags().BoolVarP(&jsonOutput, "json", "j", false, "print the result as JSON")
	return cmd
}
