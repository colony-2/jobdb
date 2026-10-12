package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsutil"
	"github.com/spf13/cobra"
)

type devSecretFlags struct{ db, key string }

func newSecretsCmd() *cobra.Command {
	var tenant, baseURL string
	root := &cobra.Command{Use: "secrets", Short: "Manage SQLite development/prototype secrets", Args: cobra.NoArgs}
	root.PersistentFlags().StringVarP(&tenant, "tenant", "t", "", "tenant ID (required)")
	root.PersistentFlags().StringVarP(&baseURL, "url", "u", "http://127.0.0.1:9047", "SQLite runtime server URL")
	for _, op := range []string{"set", "remove"} {
		var valueFile string
		cmd := &cobra.Command{Use: op + " NAME", Short: op + " a tenant secret (SQLite development only)", Args: cobra.ExactArgs(1)}
		if op == "set" {
			cmd.Aliases = []string{"add"}
			cmd.Flags().StringVarP(&valueFile, "value-file", "f", "", "UTF-8 value file, or - for stdin (required)")
		}
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			if tenant == "" || args[0] == "" || !utf8.ValidString(tenant) || !utf8.ValidString(args[0]) {
				return jobdb.ErrInvalidSecretRequest
			}
			body := map[string]string{"name": args[0]}
			if op == "set" {
				if valueFile == "" {
					return fmt.Errorf("--value-file (-f) is required")
				}
				var reader io.Reader = cmd.InOrStdin()
				if valueFile != "-" {
					file, err := os.Open(valueFile)
					if err != nil {
						return fmt.Errorf("secret value file could not be opened")
					}
					defer file.Close()
					reader = file
				}
				limit := (jobdb.SecretLimits{}).WithDefaults().MaxResponseBytes
				value, err := io.ReadAll(io.LimitReader(reader, limit+1))
				if err != nil {
					return fmt.Errorf("secret value could not be read")
				}
				if int64(len(value)) > limit {
					return jobdb.ErrSecretRequestTooLarge
				}
				if !utf8.Valid(value) {
					return jobdb.ErrInvalidSecretRequest
				}
				body["value"] = string(value)
			}
			raw, _ := json.Marshal(body)
			if int64(len(raw)) > (jobdb.SecretLimits{}).WithDefaults().MaxResponseBytes {
				return jobdb.ErrSecretRequestTooLarge
			}
			base, err := url.Parse(baseURL)
			if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
				return fmt.Errorf("invalid server URL")
			}
			target := strings.TrimRight(base.String(), "/") + "/dev/v1/tenants/" + url.PathEscape(tenant) + "/secrets/" + op
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(raw))
			if err != nil {
				return jobdb.ErrInvalidSecretRequest
			}
			req.Header.Set("Content-Type", "application/json")
			client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			resp, err := client.Do(req)
			if err != nil {
				return secretsutil.SafeError(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				return secretsutil.StatusError(resp.StatusCode)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s secret %q for tenant %q\n", op, args[0], tenant)
			return err
		}
		root.AddCommand(cmd)
	}
	return root
}
