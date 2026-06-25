package storagetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
)

// BlobStoreFixture provides an isolated blobstore for a contract test.
type BlobStoreFixture struct {
	Store   storage.BlobStore
	BaseDir string
	Cleanup func()
}

// RunBlobStoreSuite validates storage.BlobStore behavior.
func RunBlobStoreSuite(t *testing.T, newStore func(testing.TB) BlobStoreFixture) {
	t.Helper()

	t.Run("save open delete", func(t *testing.T) {
		fixture := newBlobFixture(t, newStore)
		defer fixture.cleanup()

		payload := []byte("blob payload")
		path, err := fixture.Store.Save(context.Background(), bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Save: %v", err)
		}
		if path == "" {
			t.Fatalf("Save returned empty path")
		}

		got := readBlob(t, fixture.Store, path)
		if !bytes.Equal(got, payload) {
			t.Fatalf("Open payload mismatch: got %q want %q", got, payload)
		}

		if err := fixture.Store.Delete(path); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := fixture.Store.Open(path); err == nil {
			t.Fatalf("Open after Delete succeeded")
		}
		if err := fixture.Store.Delete(path); err != nil {
			t.Fatalf("Delete missing object should be idempotent: %v", err)
		}
	})

	t.Run("empty payload", func(t *testing.T) {
		fixture := newBlobFixture(t, newStore)
		defer fixture.cleanup()

		path, err := fixture.Store.Save(context.Background(), bytes.NewReader(nil))
		if err != nil {
			t.Fatalf("Save empty payload: %v", err)
		}
		got := readBlob(t, fixture.Store, path)
		if len(got) != 0 {
			t.Fatalf("expected empty payload, got %d bytes", len(got))
		}
	})

	t.Run("binary payload", func(t *testing.T) {
		fixture := newBlobFixture(t, newStore)
		defer fixture.cleanup()

		payload := bytes.Repeat([]byte{0x00, 0xff, 0x42, 0x7f}, 2048)
		path, err := fixture.Store.Save(context.Background(), bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Save binary payload: %v", err)
		}
		first := readBlob(t, fixture.Store, path)
		second := readBlob(t, fixture.Store, path)
		if !bytes.Equal(first, payload) || !bytes.Equal(second, payload) {
			t.Fatalf("repeated Open returned different payload")
		}
	})

	t.Run("unique paths", func(t *testing.T) {
		fixture := newBlobFixture(t, newStore)
		defer fixture.cleanup()

		payload := []byte("same payload")
		first, err := fixture.Store.Save(context.Background(), bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Save first: %v", err)
		}
		second, err := fixture.Store.Save(context.Background(), bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("Save second: %v", err)
		}
		if first == second {
			t.Fatalf("expected unique blob paths, got %q", first)
		}
		if !bytes.Equal(readBlob(t, fixture.Store, first), payload) {
			t.Fatalf("first payload mismatch")
		}
		if !bytes.Equal(readBlob(t, fixture.Store, second), payload) {
			t.Fatalf("second payload mismatch")
		}
	})

	t.Run("missing object", func(t *testing.T) {
		fixture := newBlobFixture(t, newStore)
		defer fixture.cleanup()

		if _, err := fixture.Store.Open("missing"); err == nil {
			t.Fatalf("Open missing object succeeded")
		}
		if err := fixture.Store.Delete("missing"); err != nil {
			t.Fatalf("Delete missing object: %v", err)
		}
	})

	t.Run("invalid paths", func(t *testing.T) {
		fixture := newBlobFixture(t, newStore)
		defer fixture.cleanup()

		for _, path := range []string{"", "../escape", "../../escape", "/tmp/escape"} {
			if _, err := fixture.Store.Open(path); err == nil {
				t.Fatalf("Open invalid path %q succeeded", path)
			}
		}
		for _, path := range []string{"../escape", "../../escape", "/tmp/escape"} {
			if err := fixture.Store.Delete(path); err == nil {
				t.Fatalf("Delete invalid path %q succeeded", path)
			}
		}
		if err := fixture.Store.Delete(""); err != nil {
			t.Fatalf("Delete empty path should be idempotent: %v", err)
		}
	})

	t.Run("save rejects nil reader", func(t *testing.T) {
		fixture := newBlobFixture(t, newStore)
		defer fixture.cleanup()

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Save nil reader panicked: %v", r)
			}
		}()
		if _, err := fixture.Store.Save(context.Background(), nil); err == nil {
			t.Fatalf("Save nil reader succeeded")
		}
	})

	t.Run("save reader error is not committed", func(t *testing.T) {
		fixture := newBlobFixture(t, newStore)
		defer fixture.cleanup()

		before := dirEntryCount(t, fixture.BaseDir)
		if _, err := fixture.Store.Save(context.Background(), failingReader{}); !errors.Is(err, errFailingReader) {
			t.Fatalf("Save failing reader error=%v, want errFailingReader", err)
		}
		after := dirEntryCount(t, fixture.BaseDir)
		if fixture.BaseDir != "" && after != before {
			t.Fatalf("failing Save left files behind: before=%d after=%d", before, after)
		}
	})
}

type blobFixture struct {
	Store   storage.BlobStore
	BaseDir string
	done    func()
}

func newBlobFixture(t testing.TB, newStore func(testing.TB) BlobStoreFixture) blobFixture {
	t.Helper()
	fixture := newStore(t)
	if fixture.Store == nil {
		t.Fatalf("fixture returned nil blobstore")
	}
	return blobFixture{Store: fixture.Store, BaseDir: fixture.BaseDir, done: fixture.Cleanup}
}

func (f blobFixture) cleanup() {
	if f.done != nil {
		f.done()
	}
}

func readBlob(t testing.TB, store storage.BlobStore, path string) []byte {
	t.Helper()
	reader, err := store.Open(path)
	if err != nil {
		t.Fatalf("Open %q: %v", path, err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll %q: %v", path, err)
	}
	return got
}

var errFailingReader = errors.New("reader failed")

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errFailingReader
}

func dirEntryCount(t testing.TB, dir string) int {
	t.Helper()
	if dir == "" {
		return 0
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %q: %v", dir, err)
	}
	return len(entries)
}
