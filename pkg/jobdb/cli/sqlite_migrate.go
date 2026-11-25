package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/spf13/cobra"
)

func newSQLiteCheckMigrateCmd() *cobra.Command { return newSQLiteGoalMigrationCmd(false) }
func newSQLiteMigrateCmd() *cobra.Command      { return newSQLiteGoalMigrationCmd(true) }
func newSQLiteGoalMigrationCmd(convert bool) *cobra.Command {
	name := "check-migrate"
	short := "Inspect an existing SQLite database for offline format conversion"
	if convert {
		name = "migrate"
		short = "Convert an existing SQLite database after stopping every writer and backing it up"
	}
	return &cobra.Command{
		Use:   name + " --db PATH",
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("db") && !cmd.InheritedFlags().Changed("db") {
				return fmt.Errorf("%s requires an explicit --db PATH", name)
			}
			path, err := cmd.Flags().GetString("db")
			if err != nil {
				return err
			}
			dsn, err := cmd.Flags().GetString("sqlite-dsn")
			if err != nil {
				return err
			}
			if strings.TrimSpace(dsn) != "" || strings.TrimSpace(os.Getenv(sqliteDSNEnvVar)) != "" {
				return fmt.Errorf("%s uses --db PATH; remove --sqlite-dsn and %s overrides", name, sqliteDSNEnvVar)
			}
			var report sqliteruntime.GoalMigrationReport
			var checkErr error
			if convert {
				report, checkErr = sqliteruntime.MigrateGoals(cmd.Context(), path)
			} else {
				report, checkErr = sqliteruntime.CheckGoalMigration(cmd.Context(), path)
			}
			encoder := json.NewEncoder(cmd.OutOrStdout())
			encoder.SetIndent("", "  ")
			if err := encoder.Encode(report); err != nil {
				return err
			}
			return checkErr
		},
	}
}
