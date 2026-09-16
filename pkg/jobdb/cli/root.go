package cli

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/spf13/cobra"

	_ "github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk"
)

const (
	defaultListenAddr      = "127.0.0.1:9047"
	sqliteDSNEnvVar        = "JOBDB_SQLITE_DSN"
	defaultShutdownTimeout = 10 * time.Second
)

var serveHTTPFunc = ServeHTTP

func NewRootCmd() *cobra.Command { return newRootCmd() }

func newRootCmd() *cobra.Command {
	var listenAddr string
	var dbPath string
	var sqliteDSN string
	var blobDir string
	var blobStoreURI string

	cmd := &cobra.Command{
		Use:          "jobdb",
		Short:        "Run local jobdb runtime servers",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSQLite(cmd.Context(), listenAddr, sqliteConfigFromFlags(dbPath, sqliteDSN, blobDir, blobStoreURI))
		},
	}

	cmd.AddCommand(
		newSQLiteCmd(&listenAddr, &dbPath, &sqliteDSN, &blobDir, &blobStoreURI),
		newToyCmd(&listenAddr),
		newHealthcheckCmd(),
	)
	cmd.PersistentFlags().StringVar(&listenAddr, "listen", defaultListenAddr, "listen address for the HTTP API")
	cmd.PersistentFlags().StringVar(&dbPath, "db", "jobdb.db", "SQLite database path for the default embedded runtime")
	cmd.PersistentFlags().StringVar(&sqliteDSN, "sqlite-dsn", "", "SQLite DSN for the default embedded runtime (overrides --db and "+sqliteDSNEnvVar+")")
	cmd.PersistentFlags().StringVar(&blobDir, "blob-dir", "", "blobfs directory for large artifacts (defaults to <db>.blobs)")
	cmd.PersistentFlags().StringVar(&blobStoreURI, "blob-store-uri", "", "Go CDK blob bucket URL for large artifacts (overrides --blob-dir)")

	return cmd
}

func newSQLiteCmd(listenAddr *string, dbPath *string, sqliteDSN *string, blobDir *string, blobStoreURI *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sqlite",
		Short: "Run a SQLite-backed embedded workflow runtime over HTTP",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSQLite(cmd.Context(), *listenAddr, sqliteConfigFromFlags(*dbPath, *sqliteDSN, *blobDir, *blobStoreURI))
		},
	}
	return cmd
}

func newToyCmd(listenAddr *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "toy",
		Short: "Run a toy workflow runtime over HTTP",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runToy(cmd.Context(), *listenAddr)
		},
	}

	return cmd
}

func runToy(ctx context.Context, listenAddr string) error {
	runtime := toyruntime.New()
	return serveHTTPFunc(ctx, listenAddr, remoteruntime.NewServer(runtime), nil)
}

func runSQLite(ctx context.Context, listenAddr string, cfg sqliteruntime.Config) error {
	runtime, err := sqliteruntime.NewFromConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("build SQLite runtime: %w", err)
	}
	log.Printf("using SQLite runtime")
	return serveHTTPFunc(ctx, listenAddr, remoteruntime.NewServer(runtime), runtime.Close)
}

func sqliteConfigFromFlags(dbPath string, sqliteDSN string, blobDir string, blobStoreURI string) sqliteruntime.Config {
	cfg := sqliteruntime.Config{
		DBPath:       dbPath,
		BlobDir:      blobDir,
		BlobStoreURI: blobStoreURI,
	}
	if sqliteDSN != "" {
		cfg.DSN = sqliteDSN
		cfg.DBPath = ""
		return cfg
	}
	if envValue := os.Getenv(sqliteDSNEnvVar); envValue != "" {
		cfg.DSN = envValue
		cfg.DBPath = ""
	}
	return cfg
}

// ServeHTTP runs the runtime HTTP server with a health endpoint and graceful shutdown.
func ServeHTTP(ctx context.Context, listenAddr string, handler http.Handler, cleanup func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listenAddr, err)
	}
	defer listener.Close()

	server := &http.Server{Handler: withHealthCheck(handler)}
	stopShutdown := make(chan struct{})
	defer close(stopShutdown)

	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		case <-stopShutdown:
		}
	}()

	log.Printf("serving runtime API on http://%s", listener.Addr().String())
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}

	if cleanup != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		err = errors.Join(err, cleanup(cleanupCtx))
	}

	return err
}

func withHealthCheck(handler http.Handler) http.Handler {
	if handler == nil {
		handler = http.NotFoundHandler()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/", handler)
	return mux
}
