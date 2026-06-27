package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/google/uuid"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
	"gocloud.dev/gcerrors"
)

// CDK stores large artifacts in any Go CDK blob bucket.
type CDK struct {
	bucket *blob.Bucket
}

func newCDK(bucket *blob.Bucket) (*CDK, error) {
	if bucket == nil {
		return nil, fmt.Errorf("blobstore: Go CDK bucket is required")
	}
	return &CDK{bucket: bucket}, nil
}

// Save writes the provided bytes to a unique object and returns the object key.
func (c *CDK) Save(ctx context.Context, body io.Reader) (string, error) {
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

// Open exposes the stored bytes for reading.
func (c *CDK) Open(blobPath string) (io.ReadCloser, error) {
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

// Delete removes the referenced blob when it exists.
func (c *CDK) Delete(blobPath string) error {
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

// Close releases the underlying Go CDK bucket resources.
func (c *CDK) Close() error {
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
