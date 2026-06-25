package blobstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// FS stores large artifacts on the local filesystem.
type FS struct {
	basePath string
}

// NewFS provisions a filesystem-backed blob store rooted at basePath.
func NewFS(basePath string) (*FS, error) {
	if err := os.MkdirAll(basePath, 0o755); err != nil {
		return nil, fmt.Errorf("blobstore: ensure base directory: %w", err)
	}
	return &FS{basePath: filepath.Clean(basePath)}, nil
}

// Save writes the provided bytes to a unique file and returns the relative path.
func (fs *FS) Save(ctx context.Context, body io.Reader) (string, error) {
	if fs == nil {
		return "", fmt.Errorf("blobstore: FS not initialized")
	}
	if body == nil {
		return "", fmt.Errorf("blobstore: body is required")
	}
	id := uuid.NewString()
	target := filepath.Join(fs.basePath, id)

	tmp := target + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return "", fmt.Errorf("blobstore: create temp file: %w", err)
	}

	if _, err := io.Copy(f, body); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return "", fmt.Errorf("blobstore: write body: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("blobstore: close temp file: %w", err)
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("blobstore: finalize artifact: %w", err)
	}
	return id, nil
}

// Copy duplicates an existing blob into a new file and returns its relative path.
func (fs *FS) Copy(ctx context.Context, blobPath string) (string, error) {
	reader, err := fs.Open(blobPath)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	return fs.Save(ctx, reader)
}

// Open exposes the stored bytes for reading.
func (fs *FS) Open(blobPath string) (io.ReadCloser, error) {
	if fs == nil {
		return nil, fmt.Errorf("blobstore: FS not initialized")
	}
	path, err := fs.resolve(blobPath)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("blobstore: open %s: %w", blobPath, err)
	}
	return f, nil
}

// Delete removes the referenced blob when it exists.
func (fs *FS) Delete(blobPath string) error {
	if fs == nil || blobPath == "" {
		return nil
	}
	path, err := fs.resolve(blobPath)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("blobstore: delete %s: %w", blobPath, err)
	}
	return nil
}

func (fs *FS) resolve(blobPath string) (string, error) {
	if blobPath == "" {
		return "", fmt.Errorf("blobstore: path is required")
	}
	clean := filepath.Clean(blobPath)
	if clean == "." || clean == ".." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("blobstore: invalid path %q", blobPath)
	}
	return filepath.Join(fs.basePath, clean), nil
}
