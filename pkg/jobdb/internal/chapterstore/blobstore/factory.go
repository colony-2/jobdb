package blobstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
)

// Opener opens a blob store URI. Optional provider packages register openers
// for non-local schemes.
type Opener func(ctx context.Context, uri string) (storage.BlobStore, error)

// UnsupportedSchemeError reports a URI scheme that no registered opener owns.
type UnsupportedSchemeError struct {
	Scheme string
}

func (e UnsupportedSchemeError) Error() string {
	if e.Scheme == "" {
		return "blobstore: scheme is required"
	}
	return fmt.Sprintf("blobstore: unsupported blob store scheme %q: import/configure github.com/colony-2/jobdb/pkg/jobdb/blobstore/gocdk", e.Scheme)
}

var (
	openerMu sync.RWMutex
	openers  []Opener
)

// RegisterOpener installs an optional blobstore opener. It is intended for
// provider packages that are explicitly imported by executable/server code.
func RegisterOpener(opener Opener) {
	if opener == nil {
		panic("blobstore: opener is nil")
	}
	openerMu.Lock()
	defer openerMu.Unlock()
	openers = append(openers, opener)
}

func OpenURI(uri string) (storage.BlobStore, error) {
	return OpenURIContext(context.Background(), uri)
}

func OpenURIContext(ctx context.Context, uri string) (storage.BlobStore, error) {
	if ctx == nil {
		ctx = context.Background()
	}
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
		return openBlobFS(u)
	default:
		if u.Scheme == "" {
			return nil, UnsupportedSchemeError{}
		}
	}

	for _, opener := range registeredOpeners() {
		store, err := opener(ctx, uri)
		if err == nil {
			return store, nil
		}
		if isUnsupportedScheme(err) {
			continue
		}
		return nil, err
	}

	return nil, UnsupportedSchemeError{Scheme: u.Scheme}
}

func openBlobFS(u *url.URL) (storage.BlobStore, error) {
	path := u.Path
	if u.Host != "" {
		path = filepath.Join(string(filepath.Separator)+u.Host, u.Path)
	}
	if path == "" {
		return nil, fmt.Errorf("blobstore: blobfs path is required")
	}
	return NewFS(filepath.Clean(path))
}

func registeredOpeners() []Opener {
	openerMu.RLock()
	defer openerMu.RUnlock()
	if len(openers) == 0 {
		return nil
	}
	copied := make([]Opener, len(openers))
	copy(copied, openers)
	return copied
}

func isUnsupportedScheme(err error) bool {
	var unsupported UnsupportedSchemeError
	return errors.As(err, &unsupported)
}
