package gocdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	internalblobstore "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/blobstore"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
	"github.com/google/uuid"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
)

func init() {
	internalblobstore.RegisterOpener(openURI)
}

func openURI(ctx context.Context, uri string) (storage.BlobStore, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("blobstore: parse uri: %w", err)
	}
	if !blob.DefaultURLMux().ValidBucketScheme(u.Scheme) {
		return nil, internalblobstore.UnsupportedSchemeError{Scheme: u.Scheme}
	}
	if u.Scheme == "file" {
		if err := ensureFileBucketDir(u); err != nil {
			return nil, err
		}
	}
	bucket, err := blob.OpenBucket(ctx, uri)
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

type cdkStore struct {
	bucket *blob.Bucket
}

func newCDK(bucket *blob.Bucket) (*cdkStore, error) {
	if bucket == nil {
		return nil, fmt.Errorf("blobstore: Go CDK bucket is required")
	}
	return &cdkStore{bucket: bucket}, nil
}

func (c *cdkStore) Save(ctx context.Context, body io.Reader) (string, error) {
	if c == nil || c.bucket == nil {
		return "", fmt.Errorf("blobstore: Go CDK bucket not initialized")
	}
	if body == nil {
		return "", fmt.Errorf("blobstore: body is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := uuid.NewString()
	writeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	writer, err := c.bucket.NewWriter(writeCtx, key, nil)
	if err != nil {
		return "", fmt.Errorf("blobstore: create object writer: %w", err)
	}
	if _, err := io.Copy(writer, body); err != nil {
		cancel()
		closeErr := writer.Close()
		deleteErr := c.bucket.Delete(context.Background(), key)
		return "", errors.Join(fmt.Errorf("blobstore: write body: %w", err), closeErr, ignoreNotFound(deleteErr))
	}
	if err := writer.Close(); err != nil {
		deleteErr := c.bucket.Delete(context.Background(), key)
		return "", errors.Join(fmt.Errorf("blobstore: finalize object: %w", err), ignoreNotFound(deleteErr))
	}
	return key, nil
}

func (c *cdkStore) Open(blobPath string) (io.ReadCloser, error) {
	if c == nil || c.bucket == nil {
		return nil, fmt.Errorf("blobstore: Go CDK bucket not initialized")
	}
	key, err := validateBlobPath(blobPath)
	if err != nil {
		return nil, err
	}
	reader, err := c.bucket.NewReader(context.Background(), key, nil)
	if err != nil {
		return nil, fmt.Errorf("blobstore: open %s: %w", blobPath, err)
	}
	return reader, nil
}

func (c *cdkStore) Delete(blobPath string) error {
	if c == nil || c.bucket == nil || blobPath == "" {
		return nil
	}
	key, err := validateBlobPath(blobPath)
	if err != nil {
		return err
	}
	if err := c.bucket.Delete(context.Background(), key); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil
		}
		return fmt.Errorf("blobstore: delete %s: %w", blobPath, err)
	}
	return nil
}

func (c *cdkStore) Close() error {
	if c == nil || c.bucket == nil {
		return nil
	}
	return c.bucket.Close()
}

func validateBlobPath(blobPath string) (string, error) {
	if blobPath == "" {
		return "", fmt.Errorf("blobstore: path is required")
	}
	clean := path.Clean(blobPath)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "/") || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("blobstore: invalid path %q", blobPath)
	}
	return clean, nil
}

func ignoreNotFound(err error) error {
	if err == nil || gcerrors.Code(err) == gcerrors.NotFound {
		return nil
	}
	return err
}
