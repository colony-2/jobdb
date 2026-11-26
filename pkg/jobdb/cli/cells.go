package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/projectwire"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/spf13/cobra"
)

func newCellCmd() *cobra.Command {
	var tenant, baseURL, repo, name string
	root := &cobra.Command{Use: "cell", Short: "Manage project cells", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }}
	addProjectSelector(root, &tenant, true)
	root.PersistentFlags().StringVarP(&baseURL, "url", "u", "http://127.0.0.1:9047", "JobDB API server base URL")
	create := &cobra.Command{Use: "create", Short: "Create a project cell", Args: cobra.NoArgs, Long: "Create a named cell using cells.create authority. Set JOBDB_BEARER_TOKEN for deployment authentication. Repeating with the same repository reports the existing cell; this command never changes its properties.", RunE: func(cmd *cobra.Command, _ []string) error {
		if tenant == "" {
			return fmt.Errorf("--project is required")
		}
		if e := jobdb.ValidateCellName(name); e != nil {
			return fmt.Errorf("--name is required and must be a valid cell name: %w", e)
		}
		canonical, e := jobdb.NormalizeCellRepo(repo)
		if e != nil {
			return fmt.Errorf("--repo must be a hosted Git repository locator: %w", e)
		}
		rt, e := newCellProjectClient(baseURL)
		if e != nil {
			return e
		}
		c, e := rt.CreateCell(cmd.Context(), jobdb.CreateCellRequest{TenantID: tenant, Name: name, Repo: canonical})
		var ce *jobdb.CellError
		if errors.As(e, &ce) && ce.Code == "cell_conflict" {
			list, lookupErr := rt.ListCells(cmd.Context(), jobdb.ListCellsRequest{TenantID: tenant, Name: name, State: "ALL"})
			if lookupErr != nil {
				return lookupErr
			}
			if len(list.Cells) != 1 {
				return e
			}
			c, lookupErr = rt.GetCell(cmd.Context(), jobdb.CellKey{TenantID: tenant, CellID: list.Cells[0].ID})
			if lookupErr != nil {
				return lookupErr
			}
			if c.Name != name || c.Repo != canonical || c.State != jobdb.CellActive {
				return fmt.Errorf("cell already exists with different properties; update it explicitly with PATCH: %w", jobdb.ErrCellConflict)
			}
			e = nil
		}
		if e != nil {
			return e
		}
		return projectwire.Encode(cmd.OutOrStdout(), "Cell", c)
	}}
	create.Flags().StringVar(&repo, "repo", "", "cell Git repository (required)")
	create.Flags().StringVar(&name, "name", "", "cell name (required; use root to bootstrap root)")
	root.AddCommand(create)
	root.AddCommand(newCellArchiveCmd(&tenant, &baseURL, true), newCellArchiveCmd(&tenant, &baseURL, false))
	return root
}

func newCellProjectClient(baseURL string) (*remote.Runtime, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("--url must be the JobDB HTTP(S) API server base URL")
	}
	return remote.NewProjectRuntime(strings.TrimRight(parsed.String(), "/"), &http.Client{Transport: configurationAuthTransport{token: os.Getenv("JOBDB_BEARER_TOKEN")}})
}

func newCellArchiveCmd(tenant, baseURL *string, archive bool) *cobra.Command {
	action, short := "archive", "Archive a project cell"
	if !archive {
		action, short = "unarchive", "Unarchive a project cell"
	}
	var revision string
	command := &cobra.Command{Use: action + " CELL_ID", Short: short, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if *tenant == "" {
			return fmt.Errorf("--project is required")
		}
		runtime, err := newCellProjectClient(*baseURL)
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		key := jobdb.CellKey{TenantID: *tenant, CellID: args[0]}
		observed := revision
		if !cmd.Flags().Changed("revision") {
			cell, err := runtime.GetCell(ctx, key)
			if err != nil {
				return err
			}
			observed = cell.Revision
		}
		apply := runtime.UnarchiveCell
		if archive {
			apply = runtime.ArchiveCell
		}
		cell, err := apply(ctx, jobdb.ArchiveCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", observed)})
		if errors.Is(err, jobdb.ErrCellPrecondition) {
			return fmt.Errorf("cannot %s cell %q: stale revision: %w", action, key.CellID, err)
		}
		if err != nil {
			return err
		}
		return projectwire.Encode(cmd.OutOrStdout(), "Cell", cell)
	}}
	command.Flags().StringVar(&revision, "revision", "", "previously observed opaque revision (otherwise read the cell once)")
	return command
}

// The bundled development server exposes its existing administrative ingress.
// Production embedding supplies verified CellAuthority in authentication
// middleware instead of installing this local-development policy.
func localCellAdministration(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := jobdb.WithCellAuthority(r.Context(), jobdb.CellAuthority{Principal: "local-administrator", TenantID: "*", Permissions: map[string]bool{"cells.create": true, "cells.get": true, "cells.list": true, "cells.update": true, "cells.retire": true, "jobs.submit": true, "jobs.get": true, "jobs.list": true, "jobs.acquire": true}})
		// Selection uses the same trusted local-administrator policy. Stored
		// goal eligibility and destination authority remain runtime checks.
		ctx = jobdb.WithProjectAdministration(ctx, func(context.Context) error { return nil })
		ctx = jobdb.WithPrimaryGoalSelectionAuthorizer(ctx, func(context.Context, jobdb.GoalKey, jobdb.ResolvedCell) error { return nil })
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
