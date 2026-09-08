package main

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	blobStoreURIEnvVar             = "JOBDB_BLOB_STORE_URI"
	leaseTokenSigningKeyEnvVar     = "JOBDB_LEASE_TOKEN_SIGNING_KEY"
	leaseTokenSigningKeyFileEnvVar = "JOBDB_LEASE_TOKEN_SIGNING_KEY_FILE"
	maxInlineArtifactBytesEnvVar   = "JOBDB_MAX_INLINE_ARTIFACT_BYTES"
	defaultServeListenAddr         = "0.0.0.0:8080"
	defaultServeMaxInlineBytes     = int64(4096)
	minimumServeSigningKeyBytes    = 32
)

var prohibitedServeEnvVars = []string{
	"JOBDB_BACKEND",
	"JOBDB_DB_PATH",
	sqliteDSNEnvVar,
	"JOBDB_BLOB_DIR",
}

type serveConfig struct {
	ListenAddr             string
	PostgresDSN            string
	BlobStoreURI           string
	MaxInlineArtifactBytes int64
	LeaseTokenSigningKey   []byte
}

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the stateless Postgres and remote blob-store service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rejectServeFlags(cmd); err != nil {
				return err
			}
			cfg, err := serveConfigFromEnv()
			if err != nil {
				return err
			}
			return runDirectServerFunc(cmd.Context(), directServerConfig{
				ListenAddr:             cfg.ListenAddr,
				PostgresDSN:            cfg.PostgresDSN,
				BlobStoreURI:           cfg.BlobStoreURI,
				MaxInlineArtifactBytes: cfg.MaxInlineArtifactBytes,
				LeaseTokenSigningKey:   cfg.LeaseTokenSigningKey,
			})
		},
	}
}

func rejectServeFlags(cmd *cobra.Command) error {
	changed := make(map[string]struct{})
	visit := func(flag *pflag.Flag) {
		changed[flag.Name] = struct{}{}
	}
	cmd.Flags().Visit(visit)
	cmd.InheritedFlags().Visit(visit)
	if len(changed) == 0 {
		return nil
	}

	names := make([]string, 0, len(changed))
	for name := range changed {
		names = append(names, "--"+name)
	}
	sort.Strings(names)
	return fmt.Errorf("jobdb serve does not accept flags (%s); use its documented environment variables", strings.Join(names, ", "))
}

func serveConfigFromEnv() (serveConfig, error) {
	for _, name := range prohibitedServeEnvVars {
		if os.Getenv(name) != "" {
			return serveConfig{}, fmt.Errorf("%s is not supported by jobdb serve", name)
		}
	}

	postgresDSN := strings.TrimSpace(os.Getenv(postgresDSNEnvVar))
	if postgresDSN == "" {
		return serveConfig{}, fmt.Errorf("%s is required", postgresDSNEnvVar)
	}

	blobStoreURI, err := remoteBlobStoreURI(os.Getenv(blobStoreURIEnvVar))
	if err != nil {
		return serveConfig{}, err
	}

	leaseTokenSigningKey, err := serveSigningKeyFromEnv()
	if err != nil {
		return serveConfig{}, err
	}

	listenAddr := strings.TrimSpace(os.Getenv(listenAddrEnvVar))
	if listenAddr == "" {
		listenAddr = defaultServeListenAddr
	}
	if err := validateServeListenAddr(listenAddr); err != nil {
		return serveConfig{}, err
	}

	maxInlineArtifactBytes := defaultServeMaxInlineBytes
	if value := strings.TrimSpace(os.Getenv(maxInlineArtifactBytesEnvVar)); value != "" {
		maxInlineArtifactBytes, err = strconv.ParseInt(value, 10, 64)
		if err != nil || maxInlineArtifactBytes <= 0 {
			return serveConfig{}, fmt.Errorf("%s must be a positive base-10 integer", maxInlineArtifactBytesEnvVar)
		}
	}

	return serveConfig{
		ListenAddr:             listenAddr,
		PostgresDSN:            postgresDSN,
		BlobStoreURI:           blobStoreURI,
		MaxInlineArtifactBytes: maxInlineArtifactBytes,
		LeaseTokenSigningKey:   leaseTokenSigningKey,
	}, nil
}

func remoteBlobStoreURI(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s is required", blobStoreURIEnvVar)
	}
	u, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", blobStoreURIEnvVar, err)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	switch u.Scheme {
	case "s3", "gs", "azblob":
	default:
		return "", fmt.Errorf("%s must use s3, gs, or azblob", blobStoreURIEnvVar)
	}
	if u.Host == "" {
		return "", fmt.Errorf("%s must identify a bucket or container", blobStoreURIEnvVar)
	}
	if u.Fragment != "" {
		return "", fmt.Errorf("%s must not contain a fragment", blobStoreURIEnvVar)
	}
	return u.String(), nil
}

func serveSigningKeyFromEnv() ([]byte, error) {
	directValue := strings.TrimSpace(os.Getenv(leaseTokenSigningKeyEnvVar))
	fileName := strings.TrimSpace(os.Getenv(leaseTokenSigningKeyFileEnvVar))
	if directValue != "" && fileName != "" {
		return nil, fmt.Errorf("set exactly one of %s and %s", leaseTokenSigningKeyEnvVar, leaseTokenSigningKeyFileEnvVar)
	}
	if directValue == "" && fileName == "" {
		return nil, fmt.Errorf("one of %s or %s is required", leaseTokenSigningKeyEnvVar, leaseTokenSigningKeyFileEnvVar)
	}

	encoded := directValue
	if fileName != "" {
		contents, err := os.ReadFile(fileName)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", leaseTokenSigningKeyFileEnvVar, err)
		}
		encoded = strings.TrimSpace(string(contents))
	}

	key, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("configured lease token signing key must be valid base64")
	}
	if len(key) < minimumServeSigningKeyBytes {
		return nil, fmt.Errorf("configured lease token signing key must decode to at least %d bytes", minimumServeSigningKeyBytes)
	}
	return key, nil
}

func validateServeListenAddr(value string) error {
	_, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("parse %s: %w", listenAddrEnvVar, err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("%s must contain a TCP port from 1 through 65535", listenAddrEnvVar)
	}
	return nil
}
