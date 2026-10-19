package cli

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/spf13/cobra"
)

func newJobsCmd() *cobra.Command {
	root := &cobra.Command{Use: "jobs", Short: "Manage jobs through the runtime API", Args: cobra.NoArgs}
	var tenant, baseURL, olderThan, before string
	var metadata []string
	var all, dryRun, jsonOutput bool
	cmd := &cobra.Command{
		Use:   "delete [JOB_ID...]",
		Short: "Delete jobs selected by creation age, IDs, and application metadata",
		Long: `Hide matching jobs from job and schedule-run listings.
Selectors intersect. Metadata values are strings and repeated filters are ANDed.
Direct reads, history, and artifacts remain available, including to thin clones.
Unfinished jobs are cancelled and leases revoked; completed outcomes are preserved.
This command does not reclaim disk space.
Children are not implicitly hidden. IDs continue to identify the same jobs.
Use --dry-run to count matches without changing jobs.
Set JOBDB_BEARER_TOKEN if your deployment requires bearer authentication.`,
		Example: `  jobdb jobs delete -t c2 --older-than 30d -m recipe=build --dry-run
  jobdb jobs delete -t c2 --older-than 30d -m recipe=build
  jobdb jobs delete -t c2 JOB_ID
  jobdb jobs delete -t c2 --all`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(tenant) == "" {
				return fmt.Errorf("--tenant (-t) is required: %w", jobdb.ErrInvalidJobDeletion)
			}
			req := jobdb.DeleteJobsRequest{TenantID: tenant, JobIDs: args, All: all, DryRun: dryRun}
			if cmd.Flags().Changed("older-than") && cmd.Flags().Changed("created-before") {
				return fmt.Errorf("--older-than (-o) and --created-before (-b) are mutually exclusive")
			}
			if cmd.Flags().Changed("older-than") {
				age, err := parseJobAge(olderThan)
				if err != nil {
					return err
				}
				cutoff := time.Now().UTC().Add(-age)
				req.CreatedBefore = &cutoff
			}
			if cmd.Flags().Changed("created-before") {
				cutoff, err := time.Parse(time.RFC3339Nano, before)
				if err != nil {
					return fmt.Errorf("--created-before (-b) must be an RFC3339 timestamp: %w", err)
				}
				req.CreatedBefore = &cutoff
			}
			filter := jobdb.Metadata()
			for _, item := range metadata {
				key, value, ok := strings.Cut(item, "=")
				if !ok {
					return fmt.Errorf("--metadata (-m) must be KEY=VALUE")
				}
				var err error
				filter, err = filter.EqualFilter(jobdb.FieldName(key), value)
				if err != nil {
					return fmt.Errorf("--metadata (-m): %w", err)
				}
			}
			req.MetadataFilter = filter
			if err := req.Validate(); err != nil {
				return err
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
			result, err := rt.DeleteJobs(cmd.Context(), req)
			if err != nil {
				return err
			}
			if jsonOutput {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(result)
			}
			if dryRun {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "Matched %d jobs; dry run, none deleted.\n", result.MatchedCount)
			} else {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "Deleted %d jobs.\n", result.DeletedCount)
			}
			return err
		},
	}
	cmd.Flags().StringVarP(&tenant, "tenant", "t", "", "tenant ID (required)")
	cmd.Flags().StringVarP(&baseURL, "url", "u", "http://127.0.0.1:9047", "runtime server URL")
	cmd.Flags().StringVarP(&olderThan, "older-than", "o", "", "creation age, e.g. 30d or 12h; a day is 24 hours")
	cmd.Flags().StringVarP(&before, "created-before", "b", "", "exclusive creation-time cutoff in RFC3339 format")
	cmd.Flags().StringArrayVarP(&metadata, "metadata", "m", nil, "application metadata KEY=VALUE string equality; repeat for AND")
	cmd.Flags().BoolVarP(&all, "all", "a", false, "delete every job in this tenant; cannot be combined with selectors")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "n", false, "count matches without deleting")
	cmd.Flags().BoolVarP(&jsonOutput, "json", "j", false, "print the result as JSON")
	root.AddCommand(cmd)
	return root
}

func parseJobAge(raw string) (time.Duration, error) {
	invalid := fmt.Errorf("--older-than (-o) must be a positive age such as 30d or 12h")
	if strings.HasSuffix(raw, "d") {
		days, err := strconv.ParseFloat(strings.TrimSuffix(raw, "d"), 64)
		ns := days * float64(24*time.Hour)
		if err != nil || math.IsNaN(ns) || math.IsInf(ns, 0) || ns < 1 || ns >= float64(math.MaxInt64) {
			return 0, invalid
		}
		return time.Duration(ns), nil
	}
	age, err := time.ParseDuration(raw)
	if err != nil || age <= 0 {
		return 0, invalid
	}
	return age, nil
}
