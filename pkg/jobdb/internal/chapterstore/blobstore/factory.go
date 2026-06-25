package blobstore

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
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
	case "s3":
		return nil, fmt.Errorf("blobstore: s3 is not implemented yet")
	default:
		return nil, fmt.Errorf("blobstore: unsupported scheme %q", u.Scheme)
	}
}
