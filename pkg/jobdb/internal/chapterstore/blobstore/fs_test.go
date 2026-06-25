package blobstore_test

import (
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
