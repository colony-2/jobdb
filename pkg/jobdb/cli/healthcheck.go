package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	listenAddrEnvVar          = "JOBDB_LISTEN"
	defaultHealthcheckTimeout = 3 * time.Second
)

func newHealthcheckCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "healthcheck [base-url]",
		Short: "Check a running jobdb server",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			target, err := healthcheckTargetURL(args)
			if err != nil {
				return err
			}
			client := &http.Client{Timeout: defaultHealthcheckTimeout}
			return runHealthcheck(cmd.Context(), client, target)
		},
	}
}

func healthcheckTargetURL(args []string) (string, error) {
	if len(args) > 1 {
		return "", fmt.Errorf("healthcheck accepts at most one base URL")
	}
	if len(args) == 1 {
		return healthURLFromBase(args[0])
	}
	listenAddr := strings.TrimSpace(os.Getenv(listenAddrEnvVar))
	if listenAddr == "" {
		listenAddr = defaultListenAddr
	}
	return healthURLFromListenAddr(listenAddr)
}

func healthURLFromBase(base string) (string, error) {
	u, err := url.ParseRequestURI(strings.TrimSpace(base))
	if err != nil {
		return "", fmt.Errorf("parse jobdb base URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("jobdb base URL scheme must be http or https")
	}
	if u.Host == "" {
		return "", fmt.Errorf("jobdb base URL host is required")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("jobdb base URL must not contain a query or fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/healthz"
	u.RawPath = ""
	return u.String(), nil
}

func healthURLFromListenAddr(listenAddr string) (string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(listenAddr))
	if err != nil {
		return "", fmt.Errorf("parse jobdb listen address: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/healthz", nil
}

func runHealthcheck(ctx context.Context, client *http.Client, target string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		return fmt.Errorf("healthcheck HTTP client is required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("build healthcheck request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request jobdb health: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jobdb healthcheck returned %s", resp.Status)
	}
	return nil
}
