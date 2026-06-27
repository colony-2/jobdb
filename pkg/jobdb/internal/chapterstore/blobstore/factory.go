package blobstore

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
	"gocloud.dev/blob"
)

func OpenURI(uri string) (storage.BlobStore, error) {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return nil, fmt.Errorf("blobstore: uri is required")
	}
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("blobstore: parse uri: %w", err)
	}
	switch u.Scheme {
	case "blobfs":
		path := u.Path
		if u.Host != "" {
			path = filepath.Join(string(filepath.Separator)+u.Host, u.Path)
		}
		if path == "" {
			return nil, fmt.Errorf("blobstore: blobfs path is required")
		}
		return NewFS(filepath.Clean(path))
	case "file":
		if err := ensureFileBucketDir(u); err != nil {
			return nil, err
		}
	default:
		if u.Scheme == "" {
			return nil, fmt.Errorf("blobstore: scheme is required")
		}
	}
	bucket, err := blob.OpenBucket(context.Background(), uri)
	if err != nil {
		return nil, fmt.Errorf("blobstore: open Go CDK bucket: %w", err)
	}
	store, err := newCDK(bucket)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	return store, nil
}

func ensureFileBucketDir(u *url.URL) error {
	path := u.Path
	if u.Host == "." || os.PathSeparator != '/' {
		path = strings.TrimPrefix(path, "/")
	}
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("blobstore: file path is required")
	}
	if err := os.MkdirAll(filepath.FromSlash(path), 0o755); err != nil {
		return fmt.Errorf("blobstore: ensure file bucket directory: %w", err)
	}
	return nil
}
