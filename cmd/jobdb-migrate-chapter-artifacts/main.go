package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/migration"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: jobdb-migrate-chapter-artifacts check|rewrite|verify --db PATH [--blob-dir PATH]; stop all JobDB users and back up database and blobs before rewrite")
	}
	opts := migration.Options{Mode: os.Args[1]}
	flags := flag.NewFlagSet(opts.Mode, flag.ContinueOnError)
	flags.StringVar(&opts.Database, "db", "", "existing SQLite database")
	flags.StringVar(&opts.BlobDir, "blob-dir", "", "local blob directory (defaults to <db>.blobs)")
	flags.Int64Var(&opts.MaxChapterApplicationPayloadBytes, "max-chapter-application-payload-bytes", jobdb.DefaultMaxChapterApplicationPayloadBytes, "application bytes per chapter")
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	report, err := migration.Run(ctx, opts)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if writeErr := enc.Encode(report); writeErr != nil {
		return writeErr
	}
	return err
}
