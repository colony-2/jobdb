package cli

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/spf13/cobra"
)

func newConfigurationCmd() *cobra.Command {
	var tenant, baseURL string
	root := &cobra.Command{Use: "config", Short: "Manage tenant configuration on any JobDB backend", Args: cobra.NoArgs,
		Long: `Manage tenant configuration through the public administration API.
Values are JSON: use '"hello"' for a string, 'false' for a boolean,
or '{"region":"eu-west-1"}' for an object in the value file or stdin.
Set JOBDB_BEARER_TOKEN if your deployment requires bearer authentication.`}
	root.PersistentFlags().StringVarP(&tenant, "tenant", "t", "", "tenant ID (required)")
	root.PersistentFlags().StringVarP(&baseURL, "url", "u", "http://127.0.0.1:9047", "runtime server URL")
	for _, op := range []string{"set", "remove"} {
		var valueFile, etag string
		cmd := &cobra.Command{Use: op + " NAME", Short: op + " a tenant configuration value", Args: cobra.ExactArgs(1)}
		cmd.Flags().StringVar(&etag, "if-match", "", "require this quoted tenant revision ETag")
		if op == "set" {
			cmd.Aliases = []string{"add"}
			cmd.Flags().StringVarP(&valueFile, "value-file", "f", "", "JSON value file, or - for stdin (required)")
		}
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			if err := configuration.ValidateTenant(tenant); err != nil {
				return err
			}
			if err := configuration.ValidateName(args[0], jobdb.ConfigurationLimits{MaxNameBytes: len(args[0])}); err != nil {
				return err
			}
			var expected *string
			if cmd.Flags().Changed("if-match") {
				revision, err := configuration.ParseETag(etag)
				if err != nil {
					return err
				}
				expected = &revision
			}
			base, err := url.Parse(baseURL)
			if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
				return fmt.Errorf("invalid server URL")
			}
			client := &http.Client{Transport: configurationAuthTransport{token: os.Getenv("JOBDB_BEARER_TOKEN")}}
			runtime, err := remote.New(strings.TrimRight(base.String(), "/"), client)
			if err != nil {
				return configuration.SafeError(err)
			}
			var revision string
			if op == "set" {
				if valueFile == "" {
					return fmt.Errorf("--value-file (-f) is required")
				}
				var reader io.Reader = cmd.InOrStdin()
				if valueFile != "-" {
					file, e := os.Open(valueFile)
					if e != nil {
						return fmt.Errorf("configuration value file could not be opened")
					}
					defer file.Close()
					reader = file
				}
				limits := (jobdb.ConfigurationLimits{}).WithDefaults()
				raw, e := io.ReadAll(io.LimitReader(reader, int64(limits.MaxValueBytes)+1))
				if e != nil {
					return fmt.Errorf("configuration value could not be read")
				}
				// Validate before making any mutation request; the remote API repeats validation.
				if e = configuration.ValidateValue(raw, limits); e != nil {
					return e
				}
				revision, err = runtime.SetTenantConfigurationValue(cmd.Context(), jobdb.SetTenantConfigurationValueRequest{TenantID: tenant, Name: args[0], Value: raw, ExpectedRevision: expected})
			} else {
				revision, err = runtime.RemoveTenantConfigurationValue(cmd.Context(), jobdb.RemoveTenantConfigurationValueRequest{TenantID: tenant, Name: args[0], ExpectedRevision: expected})
			}
			if err != nil {
				return configuration.SafeError(err)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s configuration %q for tenant %q (ETag %s)\n", op, args[0], tenant, configuration.ETag(revision))
			return err
		}
		root.AddCommand(cmd)
	}
	return root
}

type configurationAuthTransport struct{ token string }

func (t configurationAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	if t.token != "" {
		clone.Header.Set("Authorization", "Bearer "+t.token)
	}
	return http.DefaultTransport.RoundTrip(clone)
}
