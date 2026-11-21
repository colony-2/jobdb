package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/segmentio/ksuid"
	"github.com/spf13/cobra"
)

// --tenant remains an input alias. Separate values let us reject conflicting
// selectors before issuing any HTTP request, independently of argument order.
func addProjectSelector(cmd *cobra.Command, tenant *string, persistent bool) {
	var project string
	flags := cmd.Flags()
	if persistent {
		flags = cmd.PersistentFlags()
	}
	flags.StringVar(&project, "project", "", "immutable project ID (required)")
	flags.StringVarP(tenant, "tenant", "t", "", "compatibility alias for --project")
	check := func(c *cobra.Command, _ []string) error {
		p, t := c.Flags().Changed("project"), c.Flags().Changed("tenant")
		if !p && !t {
			p = c.InheritedFlags().Changed("project")
			t = c.InheritedFlags().Changed("tenant")
		}
		if p && t && project != *tenant {
			return fmt.Errorf("--project and --tenant select different IDs")
		}
		if p {
			*tenant = project
		}
		return nil
	}
	if persistent {
		cmd.PersistentPreRunE = check
	} else {
		cmd.PreRunE = check
	}
}
func newProjectsCmd() *cobra.Command {
	var baseURL string
	root := &cobra.Command{Use: "project", Short: "Manage stable project identities", Args: cobra.NoArgs}
	root.PersistentFlags().StringVarP(&baseURL, "url", "u", "http://127.0.0.1:9047", "JobDB API server base URL")
	client := func() (*remote.Runtime, error) {
		return remote.NewProjectRuntime(strings.TrimRight(baseURL, "/"), &http.Client{Transport: configurationAuthTransport{token: os.Getenv("JOBDB_BEARER_TOKEN")}})
	}
	var requestID, description, metadata string
	create := &cobra.Command{Use: "create NAME", Short: "Create a project with an immutable ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if requestID == "" {
			requestID = ksuid.New().String()
			fmt.Fprintf(cmd.ErrOrStderr(), "Request ID: %s (reuse this ID if the result is uncertain)\n", requestID)
		}
		rt, e := client()
		if e != nil {
			return e
		}
		r, e := rt.CreateProject(cmd.Context(), jobdb.CreateProjectRequest{RequestID: requestID, Name: args[0], Description: description, Metadata: json.RawMessage(metadata)})
		if e != nil {
			return e
		}
		_, e = cmd.OutOrStdout().Write(append(r.Body, '\n'))
		return e
	}}
	create.Flags().StringVar(&requestID, "request-id", "", "stable creation retry ID; save and reuse it across uncertain results")
	create.Flags().StringVar(&description, "description", "", "creation description")
	create.Flags().StringVar(&metadata, "metadata", "{}", "creation metadata JSON object")
	get := &cobra.Command{Use: "get PROJECT_ID", Short: "Get a project by its immutable ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		rt, e := client()
		if e != nil {
			return e
		}
		p, e := rt.GetProject(cmd.Context(), args[0])
		if e != nil {
			return e
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(p)
	}}
	var name, pageToken string
	var pageSize int
	list := &cobra.Command{Use: "list", Short: "List active projects; names are discovery filters", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		rt, e := client()
		if e != nil {
			return e
		}
		p, e := rt.ListProjects(cmd.Context(), jobdb.ListProjectsRequest{Name: name, PageToken: pageToken, PageSize: pageSize})
		if e != nil {
			return e
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(p)
	}}
	list.Flags().StringVar(&name, "name", "", "exact creation name")
	list.Flags().StringVar(&pageToken, "page-token", "", "opaque continuation token")
	list.Flags().IntVar(&pageSize, "page-size", 50, "maximum projects (1–200)")
	var etag string
	remove := &cobra.Command{Use: "delete PROJECT_ID", Short: "Delete a provably unused project using its current ETag", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		rt, e := client()
		if e != nil {
			return e
		}
		return rt.DeleteProject(cmd.Context(), jobdb.DeleteProjectRequest{ID: args[0], IfMatch: etag})
	}}
	remove.Flags().StringVar(&etag, "if-match", "", "quoted project revision (required)")
	root.AddCommand(create, get, list, remove, newProjectMaintenanceCmd())
	return root
}
func newProjectMaintenanceCmd() *cobra.Command {
	var dbPath, participants, inventoryPath string
	var seedIDs []string
	root := &cobra.Command{Use: "installation", Short: "Inspect or install a fenced SQLite project catalog", Long: "Run only with resource writers and independent providers fenced. Inventory all independent stores, backups and keys separately. These commands neither discover external stores nor establish a deployment fence."}
	root.PersistentFlags().StringVar(&dbPath, "database", "", "existing SQLite database path (required)")
	open := func() (*sql.DB, error) {
		if dbPath == "" {
			return nil, fmt.Errorf("--database is required")
		}
		if _, e := os.Stat(dbPath); e != nil {
			return nil, e
		}
		return sql.Open("sqlite", dbPath)
	}
	inspect := &cobra.Command{Use: "inspect", Short: "Hash current SQLite records without changing them", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		db, e := open()
		if e != nil {
			return e
		}
		defer db.Close()
		inv, e := sqlite.InspectProjectInstallation(cmd.Context(), db, participants)
		if e != nil {
			return e
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(inv)
	}}
	inspect.Flags().StringVar(&participants, "participants-sha256", "", "SHA-256 of the separately verified independent participant inventory")
	install := &cobra.Command{Use: "install", Short: "Recheck the saved inventory and atomically install legacy identities", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if inventoryPath == "" {
			return fmt.Errorf("--inventory is required")
		}
		f, e := os.Open(inventoryPath)
		if e != nil {
			return e
		}
		defer f.Close()
		var inv sqlite.ProjectInventory
		d := json.NewDecoder(io.LimitReader(f, 1<<20))
		d.DisallowUnknownFields()
		if e = d.Decode(&inv); e != nil {
			return e
		}
		if d.Decode(new(any)) != io.EOF {
			return fmt.Errorf("invalid inventory")
		}
		var seeds []projectcatalog.Seed
		for _, id := range seedIDs {
			seeds = append(seeds, projectcatalog.Seed{ID: id, Name: id})
		}
		db, e := open()
		if e != nil {
			return e
		}
		defer db.Close()
		ctx := jobdb.WithProjectAdministration(cmd.Context(), func(context.Context) error { return nil })
		return sqlite.InstallProjectCatalog(ctx, db, inv, seeds)
	}}
	install.Flags().StringVar(&inventoryPath, "inventory", "", "saved inspect output to recheck (required)")
	install.Flags().StringSliceVar(&seedIDs, "seed", nil, "approved legacy IDs, each kept as its creation name (c2 and/or 0)")
	initialize := &cobra.Command{Use: "initialize", Short: "Explicitly initialize a fresh SQLite database with an empty catalog", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if dbPath == "" || participants == "" {
			return fmt.Errorf("--database and --participants-sha256 are required")
		}
		if _, e := os.Stat(dbPath); !os.IsNotExist(e) {
			return fmt.Errorf("initialize requires a new database path")
		}
		file, e := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if e != nil {
			return e
		}
		if e = file.Close(); e != nil {
			return e
		}
		db, e := sql.Open("sqlite", dbPath)
		if e != nil {
			return e
		}
		defer db.Close()
		if e = sqlite.InitializeProjectStorage(cmd.Context(), db); e != nil {
			return e
		}

		inv, e := sqlite.InspectProjectInstallation(cmd.Context(), db, participants)
		if e != nil {
			return e
		}
		ctx := jobdb.WithProjectAdministration(cmd.Context(), func(context.Context) error { return nil })
		return sqlite.InstallProjectCatalog(ctx, db, inv, nil)
	}}
	initialize.Flags().StringVar(&participants, "participants-sha256", "", "SHA-256 of the verified independent participant inventory")
	root.AddCommand(initialize)

	root.AddCommand(inspect, install)
	return root
}

func shapeForCLI(v any) string {
	switch v.(type) {
	case jobdb.GoalInfo:
		return "GoalInfo"
	case jobdb.ListGoalsResponse:
		return "ListGoalsResponse"
	case jobdb.ListDependencyWaitsResponse:
		return "ListDependencyWaitsResponse"
	case jobdb.ListJobsResponse:
		return "ListJobsResponse"
	}
	return ""
}
