package blobstore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/blobstore"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storagetest"
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

func TestOpenURIProviderSchemeRequiresExplicitImport(t *testing.T) {
	_, err := blobstore.OpenURI("s3://jobdb-artifacts?region=us-east-1")
	if err == nil {
		t.Fatal("OpenURI s3 returned nil error, want unsupported scheme")
	}
	want := `unsupported blob store scheme "s3"`
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("OpenURI error = %q, want to contain %q", err.Error(), want)
	}
}
