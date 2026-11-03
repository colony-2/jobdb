package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/spf13/cobra"
)

func newCellCmd() *cobra.Command {
	var tenant, baseURL, repo string
	root := &cobra.Command{Use: "cell", Short: "Manage tenant cells", Args: cobra.NoArgs}
	root.PersistentFlags().StringVarP(&tenant, "tenant", "t", "", "tenant ID (required)")
	root.PersistentFlags().StringVarP(&baseURL, "url", "u", "http://127.0.0.1:9047", "JobDB API server base URL")
	create := &cobra.Command{Use: "create-root", Short: "Bootstrap the tenant's permanent root cell", Args: cobra.NoArgs, Long: "Create the special root cell using administrative cells.create and cells.bootstrapRoot authority. Set JOBDB_BEARER_TOKEN for deployment authentication. Repeating with the same repository reports the existing root; this command never changes its properties.", RunE: func(cmd *cobra.Command, _ []string) error {
		if tenant == "" {
			return fmt.Errorf("--tenant is required")
		}
		canonical, e := jobdb.NormalizeCellRepo(repo)
		if e != nil {
			return fmt.Errorf("--repo must be a hosted Git repository locator: %w", e)
		}
		u, e := url.Parse(baseURL)
		if e != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("--url must be the JobDB HTTP(S) API server base URL")
		}
		rt, e := remote.New(strings.TrimRight(u.String(), "/"), &http.Client{Transport: configurationAuthTransport{token: os.Getenv("JOBDB_BEARER_TOKEN")}})
		if e != nil {
			return e
		}
		c, e := rt.CreateCell(cmd.Context(), jobdb.CreateCellRequest{TenantID: tenant, Name: "root", Repo: canonical})
		var ce *jobdb.CellError
		if errors.As(e, &ce) && ce.Code == "cell_conflict" {
			list, lookupErr := rt.ListCells(cmd.Context(), jobdb.ListCellsRequest{TenantID: tenant, Name: "root", State: "ALL"})
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
			if !c.IsRoot || c.Name != "root" || c.Repo != canonical {
				return fmt.Errorf("root already uses a different repository; update it explicitly with PATCH: %w", jobdb.ErrCellConflict)
			}
			e = nil
		}
		if e != nil {
			return e
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(c)
	}}
	create.Flags().StringVar(&repo, "repo", "", "root cell Git repository (required)")
	root.AddCommand(create)
	return root
}

// The bundled development server exposes its existing administrative ingress.
// Production embedding supplies verified CellAuthority in authentication
// middleware instead of installing this local-development policy.
func localCellAdministration(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := jobdb.WithCellAuthority(r.Context(), jobdb.CellAuthority{Principal: "local-administrator", TenantID: "*", Permissions: map[string]bool{"cells.create": true, "cells.bootstrapRoot": true, "cells.get": true, "cells.list": true, "cells.update": true, "cells.retire": true, "jobs.submit": true, "jobs.get": true, "jobs.list": true, "jobs.acquire": true}})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
