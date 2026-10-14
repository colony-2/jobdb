package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/spf13/cobra"
)

func newConfigurationCmd() *cobra.Command {
	var tenant, baseURL string
	root := &cobra.Command{Use: "config", Short: "Manage tenant configuration on any JobDB backend", Args: cobra.NoArgs,
		Long: `Manage tenant configuration through the public administration API.
Values are strings by default; file and stdin contents are preserved exactly.
Use --json (-j) to parse JSON, such as false or {"region":"eu-west-1"}.
Set JOBDB_BEARER_TOKEN if your deployment requires bearer authentication.`}
	root.PersistentFlags().StringVarP(&tenant, "tenant", "t", "", "tenant ID (required)")
	root.PersistentFlags().StringVarP(&baseURL, "url", "u", "http://127.0.0.1:9047", "runtime server URL")
	for _, op := range []string{"set", "remove"} {
		var valueFile, etag string
		var jsonValue bool
		cmd := &cobra.Command{Use: op + " NAME", Short: op + " a tenant configuration value", Args: cobra.ExactArgs(1)}
		cmd.Flags().StringVar(&etag, "if-match", "", "require this quoted tenant revision ETag")
		if op == "set" {
			cmd.Aliases = []string{"add"}
			cmd.Flags().StringVarP(&valueFile, "value-file", "f", "", "UTF-8 value file, or - for stdin; preserves whitespace (required)")
			cmd.Flags().BoolVarP(&jsonValue, "json", "j", false, "parse the value as JSON instead of storing it as a string")
			cmd.Long = "Set a tenant configuration value from a file or stdin. Values are strings by default, including empty input; whitespace is preserved. Use --json (-j) for JSON values."
			cmd.Example = `  jobdb config set codex_api_base_url -t c2 -f codex.url
  jobdb config set build.defaults -t c2 -f defaults.json --json
  printf '%s' 'false' | jobdb config set enablePreview -t c2 -f - -j`
		}
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			if tenant == "" {
				return fmt.Errorf("--tenant (-t) is required: %w", jobdb.ErrInvalidConfigurationRequest)
			}
			if err := configuration.ValidateTenant(tenant); err != nil {
				return fmt.Errorf("--tenant (-t) must be valid UTF-8: %w", err)
			}
			limits := (jobdb.ConfigurationLimits{}).WithDefaults()
			if op == "remove" {
				// Preserve removal of existing names after limits are lowered.
				limits.MaxNameBytes = len(args[0])
			}
			if err := configuration.ValidateName(args[0], limits); err != nil {
				if errors.Is(err, jobdb.ErrConfigurationTooLarge) {
					return fmt.Errorf("configuration name exceeds the %d-byte limit: %w", limits.MaxNameBytes, err)
				}
				return fmt.Errorf("configuration name must be nonempty UTF-8: %w", err)
			}
			var expected *string
			if cmd.Flags().Changed("if-match") {
				revision, err := configuration.ParseETag(etag)
				if err != nil {
					return fmt.Errorf("--if-match must be one quoted revision ETag (no wildcard or weak tag): %w", err)
				}
				expected = &revision
			}
			base, err := url.Parse(baseURL)
			if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
				return fmt.Errorf("--url (-u) must be an HTTP(S) server URL without credentials, query, or fragment")
			}
			client := &http.Client{Transport: configurationAuthTransport{token: os.Getenv("JOBDB_BEARER_TOKEN")}}
			runtime, err := remote.New(strings.TrimRight(base.String(), "/"), client)
			if err != nil {
				return configuration.SafeError(err)
			}
			var revision string
			if op == "set" {
				if valueFile == "" {
					return fmt.Errorf("--value-file (-f) is required: %w", jobdb.ErrInvalidConfigurationRequest)
				}
				var reader io.Reader = cmd.InOrStdin()
				if valueFile != "-" {
					file, e := os.Open(valueFile)
					if e != nil {
						return fmt.Errorf("configuration value file could not be opened; check --value-file (-f), file existence, and read permissions")
					}
					defer file.Close()
					reader = file
				}
				raw, e := io.ReadAll(io.LimitReader(reader, int64(limits.MaxValueBytes)+1))
				if e != nil {
					return fmt.Errorf("configuration value could not be read; check --value-file (-f) or stdin")
				}
				if len(raw) > limits.MaxValueBytes {
					return fmt.Errorf("configuration value exceeds the %d-byte JSON size limit: %w", limits.MaxValueBytes, jobdb.ErrConfigurationTooLarge)
				}
				if !jsonValue {
					if !utf8.Valid(raw) {
						return fmt.Errorf("configuration string must be valid UTF-8: %w", jobdb.ErrInvalidConfigurationRequest)
					}
					// Marshal a string, not []byte (which JSON would encode as base64).
					raw, _ = json.Marshal(string(raw))
				}
				// Validate the encoded value, including JSON escaping overhead,
				// before making any mutation request; the remote API repeats validation.
				if e = configuration.ValidateValue(raw, limits); e != nil {
					if errors.Is(e, jobdb.ErrConfigurationTooLarge) {
						return fmt.Errorf("configuration value exceeds the %d-byte JSON size limit: %w", limits.MaxValueBytes, e)
					}
					return fmt.Errorf("configuration value must be a single valid UTF-8 JSON value with unique object keys; strings need double quotes: %w", e)
				}
				revision, err = runtime.SetTenantConfigurationValue(cmd.Context(), jobdb.SetTenantConfigurationValueRequest{TenantID: tenant, Name: args[0], Value: raw, ExpectedRevision: expected})
			} else {
				revision, err = runtime.RemoveTenantConfigurationValue(cmd.Context(), jobdb.RemoveTenantConfigurationValueRequest{TenantID: tenant, Name: args[0], ExpectedRevision: expected})
			}
			if err != nil {
				safe := configuration.SafeError(err)
				switch safe {
				case jobdb.ErrConfigurationRevisionMismatch:
					return fmt.Errorf("--if-match does not match the current tenant revision; retrieve the current configuration before retrying: %w", safe)
				case jobdb.ErrConfigurationUnavailable:
					return fmt.Errorf("configuration request could not complete; check --url (-u) and server availability: %w", safe)
				default:
					return fmt.Errorf("configuration administration request failed: %w", safe)
				}
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
