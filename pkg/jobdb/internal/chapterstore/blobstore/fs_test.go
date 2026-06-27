package blobstore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/blobstore"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storagetest"
	"gocloud.dev/blob"
)

func TestFSContract(t *testing.T) {
	storagetest.RunBlobStoreSuite(t, func(t testing.TB) storagetest.BlobStoreFixture {
		t.Helper()
		baseDir := t.TempDir()
		store, err := blobstore.NewFS(baseDir)
		if err != nil {
			t.Fatalf("NewFS: %v", err)
		}
		return storagetest.BlobStoreFixture{Store: store, BaseDir: baseDir}
	})
}

func TestOpenURIFileContract(t *testing.T) {
	storagetest.RunBlobStoreSuite(t, func(t testing.TB) storagetest.BlobStoreFixture {
		t.Helper()
		baseDir := t.TempDir()
		store, err := blobstore.OpenURI("file://" + baseDir + "?metadata=skip")
		if err != nil {
			t.Fatalf("OpenURI file: %v", err)
		}
		return storagetest.BlobStoreFixture{Store: store, BaseDir: baseDir}
	})
}

func TestGoCDKProviderSchemesRegistered(t *testing.T) {
	for _, scheme := range []string{"gs", "s3", "azblob", "file", "mem"} {
		if !blob.DefaultURLMux().ValidBucketScheme(scheme) {
			t.Fatalf("Go CDK bucket scheme %q is not registered", scheme)
		}
	}
}

func TestOpenURIFileCreatesDirectory(t *testing.T) {
	baseDir := t.TempDir()
	target := baseDir + "/nested/blobs"
	store, err := blobstore.OpenURI("file://" + target + "?metadata=skip")
	if err != nil {
		t.Fatalf("OpenURI file: %v", err)
	}
	t.Cleanup(func() {
		if closer, ok := store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	})

	path, err := store.Save(context.Background(), strings.NewReader("ok"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if path == "" {
		t.Fatal("Save returned empty path")
	}
}

func TestOpenURIBlobFSCompatibility(t *testing.T) {
	baseDir := t.TempDir()
	store, err := blobstore.OpenURI(fmt.Sprintf("blobfs://%s", baseDir))
	if err != nil {
		t.Fatalf("OpenURI blobfs: %v", err)
	}
	path, err := store.Save(context.Background(), strings.NewReader("ok"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if path == "" {
		t.Fatal("Save returned empty path")
	}
}
