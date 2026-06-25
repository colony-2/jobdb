package storagetest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
)

// RowStoreFixture provides an isolated rowstore for a contract test.
type RowStoreFixture struct {
	Store   storage.RowStore
	Cleanup func()
}

// RunRowStoreSuite validates storage.RowStore behavior.
func RunRowStoreSuite(t *testing.T, newStore func(testing.TB) RowStoreFixture) {
	t.Helper()

	t.Run("health", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		if err := fixture.Store.Health(context.Background()); err != nil {
			t.Fatalf("Health: %v", err)
		}
	})

	t.Run("story lifecycle", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		key := storage.StoryKey{AnthologyID: "anth", StoryID: "story"}
		created, err := fixture.Store.CreateStory(ctx, key, testNow(0))
		if err != nil {
			t.Fatalf("CreateStory: %v", err)
		}
		if created.Key != key || created.LatestOrdinal != -1 || created.ChapterCount != 0 {
			t.Fatalf("unexpected created story: %+v", created)
		}

		loaded, err := fixture.Store.GetStory(ctx, key)
		if err != nil {
			t.Fatalf("GetStory: %v", err)
		}
		if loaded.Key != key {
			t.Fatalf("loaded wrong story: %+v", loaded)
		}

		items, hasMore, err := fixture.Store.ListStories(ctx, key.AnthologyID, "", 10)
		if err != nil {
			t.Fatalf("ListStories: %v", err)
		}
		if hasMore || len(items) != 1 || items[0].Key != key {
			t.Fatalf("unexpected story list: items=%+v hasMore=%v", items, hasMore)
		}

		if err := storage.MarkStoryDeleted(ctx, fixture.Store, key, testNow(1)); err != nil {
			t.Fatalf("MarkStoryDeleted: %v", err)
		}
		if _, err := fixture.Store.GetStory(ctx, key); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("GetStory after delete error=%v, want ErrStoryNotFound", err)
		}
	})

	t.Run("duplicate story", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		key := storage.StoryKey{AnthologyID: "anth", StoryID: "dupe"}
		if _, err := fixture.Store.CreateStory(ctx, key, testNow(0)); err != nil {
			t.Fatalf("CreateStory: %v", err)
		}
		if _, err := fixture.Store.CreateStory(ctx, key, testNow(1)); !errors.Is(err, storage.ErrStoryExists) {
			t.Fatalf("duplicate CreateStory error=%v, want ErrStoryExists", err)
		}
	})

	t.Run("missing story operations", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		key := storage.StoryKey{AnthologyID: "anth", StoryID: "missing"}
		if _, err := fixture.Store.GetStory(ctx, key); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("GetStory missing error=%v, want ErrStoryNotFound", err)
		}
		if _, err := fixture.Store.TombstoneStory(ctx, key, testNow(0)); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("TombstoneStory missing error=%v, want ErrStoryNotFound", err)
		}
		if _, err := fixture.Store.Publish(ctx, key, testNow(0)); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("Publish missing error=%v, want ErrStoryNotFound", err)
		}
		chapter := storage.ChapterRecord{Key: key, Ordinal: 0, Body: json.RawMessage(`{"missing":true}`)}
		if _, err := fixture.Store.AppendChapter(ctx, chapter, testNow(0)); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("AppendChapter missing error=%v, want ErrStoryNotFound", err)
		}
		if _, err := fixture.Store.ReadChapter(ctx, key, 0); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("ReadChapter missing story error=%v, want ErrStoryNotFound", err)
		}
		if _, _, err := fixture.Store.ListChapterOrdinals(ctx, key, 0, 10); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("ListChapterOrdinals missing error=%v, want ErrStoryNotFound", err)
		}
		if err := storage.MarkStoryDeleted(ctx, fixture.Store, key, testNow(0)); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("MarkStoryDeleted missing error=%v, want ErrStoryNotFound", err)
		}
	})

	t.Run("tombstone requires finalized story", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		key := storage.StoryKey{AnthologyID: "anth", StoryID: "tombstone-direct"}
		if _, err := fixture.Store.CreateStory(ctx, key, testNow(0)); err != nil {
			t.Fatalf("CreateStory: %v", err)
		}
		if _, err := fixture.Store.TombstoneStory(ctx, key, testNow(1)); !errors.Is(err, storage.ErrInvalidRecord) {
			t.Fatalf("TombstoneStory unfinalized error=%v, want ErrInvalidRecord", err)
		}
		published, err := fixture.Store.Publish(ctx, key, testNow(2))
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if !published.Finalized || published.Deleted {
			t.Fatalf("unexpected published metadata: %+v", published)
		}
		deleted, err := fixture.Store.TombstoneStory(ctx, key, testNow(3))
		if err != nil {
			t.Fatalf("TombstoneStory finalized: %v", err)
		}
		if !deleted.Deleted || !deleted.Finalized || deleted.DeletedAt.IsZero() {
			t.Fatalf("unexpected tombstone metadata: %+v", deleted)
		}
		if _, err := fixture.Store.TombstoneStory(ctx, key, testNow(4)); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("second TombstoneStory error=%v, want ErrStoryNotFound", err)
		}
	})

	t.Run("chapters artifacts and publish", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		key := storage.StoryKey{AnthologyID: "anth", StoryID: "chapters"}
		if _, err := fixture.Store.CreateStory(ctx, key, testNow(0)); err != nil {
			t.Fatalf("CreateStory: %v", err)
		}

		chapter := storage.ChapterRecord{
			Key:     key,
			Ordinal: 0,
			Body:    json.RawMessage(`{"step":0}`),
			Artifacts: []storage.ArtifactRecord{
				inlineArtifact("inline.txt", []byte("inline")),
				inlineArtifact("empty.txt", nil),
				blobArtifact("remote.bin", "blob/path", 128),
			},
		}
		meta, err := fixture.Store.AppendChapter(ctx, chapter, testNow(1))
		if err != nil {
			t.Fatalf("AppendChapter: %v", err)
		}
		if meta.ChapterCount != 1 || meta.LatestOrdinal != 0 {
			t.Fatalf("unexpected story metadata after append: %+v", meta)
		}

		read, err := fixture.Store.ReadChapter(ctx, key, 0)
		if err != nil {
			t.Fatalf("ReadChapter: %v", err)
		}
		if string(read.Body) != string(chapter.Body) || len(read.Artifacts) != 3 {
			t.Fatalf("unexpected chapter read: %+v", read)
		}

		ordinals, hasMore, err := fixture.Store.ListChapterOrdinals(ctx, key, 0, 1)
		if err != nil {
			t.Fatalf("ListChapterOrdinals: %v", err)
		}
		if hasMore || len(ordinals) != 1 || ordinals[0] != 0 {
			t.Fatalf("unexpected ordinals: %v hasMore=%v", ordinals, hasMore)
		}

		if _, err := fixture.Store.AppendChapter(ctx, chapter, testNow(2)); !errors.Is(err, storage.ErrChapterExists) {
			t.Fatalf("duplicate AppendChapter error=%v, want ErrChapterExists", err)
		}

		published, err := fixture.Store.Publish(ctx, key, testNow(3))
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if !published.Finalized {
			t.Fatalf("Publish did not finalize story: %+v", published)
		}
		next := storage.ChapterRecord{Key: key, Ordinal: 1, Body: json.RawMessage(`{"step":1}`)}
		if _, err := fixture.Store.AppendChapter(ctx, next, testNow(4)); !errors.Is(err, storage.ErrStoryFinalized) {
			t.Fatalf("append after publish error=%v, want ErrStoryFinalized", err)
		}
		if _, err := fixture.Store.Publish(ctx, key, testNow(5)); !errors.Is(err, storage.ErrStoryFinalized) {
			t.Fatalf("second Publish error=%v, want ErrStoryFinalized", err)
		}
	})

	t.Run("chapter pagination", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		key := storage.StoryKey{AnthologyID: "anth", StoryID: "chapter-pages"}
		if _, err := fixture.Store.CreateStory(ctx, key, testNow(0)); err != nil {
			t.Fatalf("CreateStory: %v", err)
		}
		for ord := int64(0); ord < 3; ord++ {
			chapter := storage.ChapterRecord{Key: key, Ordinal: ord, Body: json.RawMessage(fmt.Sprintf(`{"ord":%d}`, ord))}
			if _, err := fixture.Store.AppendChapter(ctx, chapter, testNow(int(ord)+1)); err != nil {
				t.Fatalf("AppendChapter %d: %v", ord, err)
			}
		}

		first, hasMore, err := fixture.Store.ListChapterOrdinals(ctx, key, 0, 2)
		if err != nil {
			t.Fatalf("ListChapterOrdinals first page: %v", err)
		}
		if !hasMore || !equalOrdinals(first, []int64{0, 1}) {
			t.Fatalf("unexpected first page: %v hasMore=%v", first, hasMore)
		}

		second, hasMore, err := fixture.Store.ListChapterOrdinals(ctx, key, 2, 2)
		if err != nil {
			t.Fatalf("ListChapterOrdinals second page: %v", err)
		}
		if hasMore || !equalOrdinals(second, []int64{2}) {
			t.Fatalf("unexpected second page: %v hasMore=%v", second, hasMore)
		}
	})

	t.Run("append rejects invalid chapters without mutation", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		key := storage.StoryKey{AnthologyID: "anth", StoryID: "invalid-chapters"}
		if _, err := fixture.Store.CreateStory(ctx, key, testNow(0)); err != nil {
			t.Fatalf("CreateStory: %v", err)
		}

		gap := storage.ChapterRecord{Key: key, Ordinal: 1, Body: json.RawMessage(`{"gap":true}`)}
		if _, err := fixture.Store.AppendChapter(ctx, gap, testNow(1)); !errors.Is(err, storage.ErrInvalidRecord) {
			t.Fatalf("gap AppendChapter error=%v, want ErrInvalidRecord", err)
		}
		negative := storage.ChapterRecord{Key: key, Ordinal: -1, Body: json.RawMessage(`{"negative":true}`)}
		if _, err := fixture.Store.AppendChapter(ctx, negative, testNow(2)); !errors.Is(err, storage.ErrInvalidRecord) {
			t.Fatalf("negative AppendChapter error=%v, want ErrInvalidRecord", err)
		}
		loaded, err := fixture.Store.GetStory(ctx, key)
		if err != nil {
			t.Fatalf("GetStory after invalid append: %v", err)
		}
		if loaded.ChapterCount != 0 || loaded.LatestOrdinal != -1 {
			t.Fatalf("invalid append mutated story metadata: %+v", loaded)
		}
		if _, err := fixture.Store.ReadChapter(ctx, key, 1); !errors.Is(err, storage.ErrChapterNotFound) {
			t.Fatalf("ReadChapter after invalid append error=%v, want ErrChapterNotFound", err)
		}

		first := storage.ChapterRecord{Key: key, Ordinal: 0, Body: json.RawMessage(`{"ok":true}`)}
		if _, err := fixture.Store.AppendChapter(ctx, first, testNow(3)); err != nil {
			t.Fatalf("AppendChapter 0: %v", err)
		}
		nextGap := storage.ChapterRecord{Key: key, Ordinal: 2, Body: json.RawMessage(`{"gap":2}`)}
		if _, err := fixture.Store.AppendChapter(ctx, nextGap, testNow(4)); !errors.Is(err, storage.ErrInvalidRecord) {
			t.Fatalf("later gap AppendChapter error=%v, want ErrInvalidRecord", err)
		}
		loaded, err = fixture.Store.GetStory(ctx, key)
		if err != nil {
			t.Fatalf("GetStory after later gap: %v", err)
		}
		if loaded.ChapterCount != 1 || loaded.LatestOrdinal != 0 {
			t.Fatalf("later gap mutated story metadata: %+v", loaded)
		}
	})

	t.Run("append rejects invalid artifacts without mutation", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		cases := []struct {
			name      string
			artifacts []storage.ArtifactRecord
		}{
			{
				name: "missing name",
				artifacts: []storage.ArtifactRecord{func() storage.ArtifactRecord {
					art := inlineArtifact("bad.txt", []byte("bad"))
					art.Name = ""
					return art
				}()},
			},
			{
				name: "duplicate names",
				artifacts: []storage.ArtifactRecord{
					inlineArtifact("dupe.txt", []byte("a")),
					inlineArtifact("dupe.txt", []byte("b")),
				},
			},
			{
				name: "negative size",
				artifacts: []storage.ArtifactRecord{func() storage.ArtifactRecord {
					art := inlineArtifact("bad.txt", []byte("bad"))
					art.SizeBytes = -1
					return art
				}()},
			},
			{
				name: "missing sha",
				artifacts: []storage.ArtifactRecord{func() storage.ArtifactRecord {
					art := inlineArtifact("bad.txt", []byte("bad"))
					art.Sha256 = ""
					return art
				}()},
			},
			{
				name: "inline size mismatch",
				artifacts: []storage.ArtifactRecord{func() storage.ArtifactRecord {
					art := inlineArtifact("bad.txt", []byte("bad"))
					art.SizeBytes++
					return art
				}()},
			},
			{
				name: "inline and blob path",
				artifacts: []storage.ArtifactRecord{func() storage.ArtifactRecord {
					art := inlineArtifact("bad.txt", []byte("bad"))
					art.BlobPath = "blob/path"
					return art
				}()},
			},
			{
				name: "missing blob path",
				artifacts: []storage.ArtifactRecord{func() storage.ArtifactRecord {
					art := blobArtifact("bad.bin", "blob/path", 3)
					art.BlobPath = ""
					return art
				}()},
			},
		}

		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				key := storage.StoryKey{AnthologyID: "anth", StoryID: fmt.Sprintf("invalid-artifact-%d", i)}
				if _, err := fixture.Store.CreateStory(ctx, key, testNow(i)); err != nil {
					t.Fatalf("CreateStory: %v", err)
				}
				chapter := storage.ChapterRecord{
					Key:       key,
					Ordinal:   0,
					Body:      json.RawMessage(`{"artifact":true}`),
					Artifacts: tc.artifacts,
				}
				if _, err := fixture.Store.AppendChapter(ctx, chapter, testNow(i+10)); !errors.Is(err, storage.ErrInvalidRecord) {
					t.Fatalf("AppendChapter error=%v, want ErrInvalidRecord", err)
				}
				loaded, err := fixture.Store.GetStory(ctx, key)
				if err != nil {
					t.Fatalf("GetStory after invalid artifact: %v", err)
				}
				if loaded.ChapterCount != 0 || loaded.LatestOrdinal != -1 {
					t.Fatalf("invalid artifact mutated story metadata: %+v", loaded)
				}
				if _, err := fixture.Store.ReadChapter(ctx, key, 0); !errors.Is(err, storage.ErrChapterNotFound) {
					t.Fatalf("ReadChapter after invalid artifact error=%v, want ErrChapterNotFound", err)
				}
			})
		}
	})

	t.Run("linked story append invariants", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		base := storage.StoryKey{AnthologyID: "anth", StoryID: "linked-base"}
		clone := storage.StoryKey{AnthologyID: "anth", StoryID: "linked-clone"}
		if _, err := fixture.Store.CreateStory(ctx, base, testNow(0)); err != nil {
			t.Fatalf("CreateStory base: %v", err)
		}
		for ord := int64(0); ord < 3; ord++ {
			chapter := storage.ChapterRecord{Key: base, Ordinal: ord, Body: json.RawMessage(fmt.Sprintf(`{"base":%d}`, ord))}
			if _, err := fixture.Store.AppendChapter(ctx, chapter, testNow(int(ord)+1)); err != nil {
				t.Fatalf("AppendChapter base %d: %v", ord, err)
			}
		}

		linked, err := fixture.Store.CreateLinkedStory(ctx, clone, storage.StoryLink{Key: base, ForkOrdinal: 1}, testNow(10))
		if err != nil {
			t.Fatalf("CreateLinkedStory: %v", err)
		}
		if linked.Link == nil || linked.Link.Key != base || linked.Link.ForkOrdinal != 1 {
			t.Fatalf("unexpected link metadata: %+v", linked.Link)
		}
		if linked.ChapterCount != 2 || linked.LatestOrdinal != 1 {
			t.Fatalf("unexpected linked story counters: %+v", linked)
		}

		if _, err := fixture.Store.ReadChapter(ctx, clone, 0); !errors.Is(err, storage.ErrChapterNotFound) {
			t.Fatalf("ReadChapter inherited local row error=%v, want ErrChapterNotFound", err)
		}
		ordinals, hasMore, err := fixture.Store.ListChapterOrdinals(ctx, clone, 0, 10)
		if err != nil {
			t.Fatalf("ListChapterOrdinals linked before append: %v", err)
		}
		if hasMore || len(ordinals) != 0 {
			t.Fatalf("linked story should not copy inherited rows: ordinals=%v hasMore=%v", ordinals, hasMore)
		}

		if _, err := fixture.Store.AppendChapter(ctx, storage.ChapterRecord{Key: clone, Ordinal: 0, Body: json.RawMessage(`{"bad":0}`)}, testNow(11)); !errors.Is(err, storage.ErrInvalidRecord) {
			t.Fatalf("AppendChapter before fork error=%v, want ErrInvalidRecord", err)
		}
		if _, err := fixture.Store.AppendChapter(ctx, storage.ChapterRecord{Key: clone, Ordinal: 2, Body: json.RawMessage(`{"clone":2}`)}, testNow(12)); err != nil {
			t.Fatalf("AppendChapter first post-fork: %v", err)
		}
		if _, err := fixture.Store.AppendChapter(ctx, storage.ChapterRecord{Key: clone, Ordinal: 4, Body: json.RawMessage(`{"gap":4}`)}, testNow(13)); !errors.Is(err, storage.ErrInvalidRecord) {
			t.Fatalf("AppendChapter linked gap error=%v, want ErrInvalidRecord", err)
		}
		if _, err := fixture.Store.AppendChapter(ctx, storage.ChapterRecord{Key: clone, Ordinal: 2, Body: json.RawMessage(`{"dupe":2}`)}, testNow(14)); !errors.Is(err, storage.ErrChapterExists) {
			t.Fatalf("AppendChapter linked duplicate error=%v, want ErrChapterExists", err)
		}
		ordinals, hasMore, err = fixture.Store.ListChapterOrdinals(ctx, clone, 0, 10)
		if err != nil {
			t.Fatalf("ListChapterOrdinals linked after append: %v", err)
		}
		if hasMore || !equalOrdinals(ordinals, []int64{2}) {
			t.Fatalf("unexpected linked local ordinals: %v hasMore=%v", ordinals, hasMore)
		}

		if _, err := fixture.Store.CreateLinkedStory(ctx, storage.StoryKey{AnthologyID: "anth", StoryID: "missing-base-clone"}, storage.StoryLink{Key: storage.StoryKey{AnthologyID: "anth", StoryID: "missing"}, ForkOrdinal: 0}, testNow(15)); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("CreateLinkedStory missing base error=%v, want ErrStoryNotFound", err)
		}
		if _, err := fixture.Store.CreateLinkedStory(ctx, storage.StoryKey{AnthologyID: "anth", StoryID: "beyond-fork"}, storage.StoryLink{Key: base, ForkOrdinal: 99}, testNow(16)); !errors.Is(err, storage.ErrInvalidRecord) {
			t.Fatalf("CreateLinkedStory fork beyond base error=%v, want ErrInvalidRecord", err)
		}
		if _, err := fixture.Store.CreateLinkedStory(ctx, clone, storage.StoryLink{Key: base, ForkOrdinal: 1}, testNow(17)); !errors.Is(err, storage.ErrStoryExists) {
			t.Fatalf("CreateLinkedStory duplicate destination error=%v, want ErrStoryExists", err)
		}
		if _, err := fixture.Store.CreateLinkedStory(ctx, base, storage.StoryLink{Key: base, ForkOrdinal: 1}, testNow(18)); !errors.Is(err, storage.ErrInvalidRecord) {
			t.Fatalf("CreateLinkedStory self-link error=%v, want ErrInvalidRecord", err)
		}
	})

	t.Run("story pagination", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		for i, id := range []string{"a", "b", "c"} {
			key := storage.StoryKey{AnthologyID: "anth", StoryID: id}
			if _, err := fixture.Store.CreateStory(ctx, key, testNow(i)); err != nil {
				t.Fatalf("CreateStory %s: %v", id, err)
			}
		}

		first, hasMore, err := fixture.Store.ListStories(ctx, "anth", "", 2)
		if err != nil {
			t.Fatalf("ListStories first page: %v", err)
		}
		if !hasMore || len(first) != 2 || first[0].Key.StoryID != "a" || first[1].Key.StoryID != "b" {
			t.Fatalf("unexpected first page: %+v hasMore=%v", first, hasMore)
		}

		second, hasMore, err := fixture.Store.ListStories(ctx, "anth", "b", 2)
		if err != nil {
			t.Fatalf("ListStories second page: %v", err)
		}
		if hasMore || len(second) != 1 || second[0].Key.StoryID != "c" {
			t.Fatalf("unexpected second page: %+v hasMore=%v", second, hasMore)
		}
	})

	t.Run("delete marks story unavailable", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		key := storage.StoryKey{AnthologyID: "anth", StoryID: "delete"}
		if _, err := fixture.Store.CreateStory(ctx, key, testNow(0)); err != nil {
			t.Fatalf("CreateStory: %v", err)
		}
		chapter := storage.ChapterRecord{Key: key, Ordinal: 0, Body: json.RawMessage(`{"delete":true}`)}
		if _, err := fixture.Store.AppendChapter(ctx, chapter, testNow(1)); err != nil {
			t.Fatalf("AppendChapter: %v", err)
		}
		if err := storage.MarkStoryDeleted(ctx, fixture.Store, key, testNow(2)); err != nil {
			t.Fatalf("MarkStoryDeleted: %v", err)
		}
		if _, err := fixture.Store.GetStory(ctx, key); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("GetStory after delete error=%v, want ErrStoryNotFound", err)
		}
		deleted, err := fixture.Store.GetStoryIncludingDeleted(ctx, key)
		if err != nil {
			t.Fatalf("GetStoryIncludingDeleted after delete: %v", err)
		}
		if !deleted.Deleted || deleted.DeletedAt.IsZero() || !deleted.Finalized {
			t.Fatalf("deleted metadata not retained: %+v", deleted)
		}
		if _, err := fixture.Store.ReadChapter(ctx, key, 0); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("ReadChapter after delete error=%v, want ErrStoryNotFound", err)
		}
		if _, err := fixture.Store.ReadChapterIncludingDeleted(ctx, key, 0); err != nil {
			t.Fatalf("ReadChapterIncludingDeleted after delete: %v", err)
		}
		if _, _, err := fixture.Store.ListChapterOrdinals(ctx, key, 0, 10); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("ListChapterOrdinals after delete error=%v, want ErrStoryNotFound", err)
		}
		items, hasMore, err := fixture.Store.ListStories(ctx, key.AnthologyID, "", 10)
		if err != nil {
			t.Fatalf("ListStories after delete: %v", err)
		}
		if hasMore || len(items) != 0 {
			t.Fatalf("deleted story visible in list: items=%+v hasMore=%v", items, hasMore)
		}
		if _, err := fixture.Store.CreateStory(ctx, key, testNow(3)); !errors.Is(err, storage.ErrStoryExists) {
			t.Fatalf("CreateStory after delete error=%v, want ErrStoryExists", err)
		}
		if err := storage.MarkStoryDeleted(ctx, fixture.Store, key, testNow(4)); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("MarkStoryDeleted missing error=%v, want ErrStoryNotFound", err)
		}
	})

	t.Run("delete finalized story", func(t *testing.T) {
		fixture := newRowFixture(t, newStore)
		defer fixture.cleanup()

		ctx := context.Background()
		key := storage.StoryKey{AnthologyID: "anth", StoryID: "delete-finalized"}
		if _, err := fixture.Store.CreateStory(ctx, key, testNow(0)); err != nil {
			t.Fatalf("CreateStory: %v", err)
		}
		if _, err := fixture.Store.Publish(ctx, key, testNow(1)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		if err := storage.MarkStoryDeleted(ctx, fixture.Store, key, testNow(2)); err != nil {
			t.Fatalf("MarkStoryDeleted finalized story: %v", err)
		}
		if _, err := fixture.Store.GetStory(ctx, key); !errors.Is(err, storage.ErrStoryNotFound) {
			t.Fatalf("GetStory after finalized delete error=%v, want ErrStoryNotFound", err)
		}
	})
}

type rowFixture struct {
	Store storage.RowStore
	done  func()
}

func newRowFixture(t testing.TB, newStore func(testing.TB) RowStoreFixture) rowFixture {
	t.Helper()
	fixture := newStore(t)
	if fixture.Store == nil {
		t.Fatalf("fixture returned nil rowstore")
	}
	return rowFixture{Store: fixture.Store, done: fixture.Cleanup}
}

func (f rowFixture) cleanup() {
	if f.done != nil {
		f.done()
	}
}

func testNow(offset int) time.Time {
	return time.Unix(1_700_000_000+int64(offset), 0).UTC()
}

func equalOrdinals(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func inlineArtifact(name string, data []byte) storage.ArtifactRecord {
	sum := sha256.Sum256(data)
	return storage.ArtifactRecord{
		ID:          name + "-id",
		Name:        name,
		ContentType: "text/plain",
		SizeBytes:   int64(len(data)),
		Sha256:      fmt.Sprintf("%x", sum[:]),
		InlineData:  append([]byte(nil), data...),
	}
}

func blobArtifact(name, path string, size int64) storage.ArtifactRecord {
	data := []byte(fmt.Sprintf("%s:%d", path, size))
	sum := sha256.Sum256(data)
	return storage.ArtifactRecord{
		ID:          name + "-id",
		Name:        name,
		ContentType: "application/octet-stream",
		SizeBytes:   size,
		Sha256:      fmt.Sprintf("%x", sum[:]),
		BlobPath:    path,
	}
}
