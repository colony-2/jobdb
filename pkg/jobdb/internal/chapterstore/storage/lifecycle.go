package storage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MarkStoryDeleted finalizes a story when needed, then records its deletion
// tombstone through the rowstore metadata API.
func MarkStoryDeleted(ctx context.Context, rows RowStore, key StoryKey, now time.Time) error {
	if rows == nil {
		return fmt.Errorf("chapterstore: store is required")
	}

	rec, err := rows.GetStory(ctx, key)
	if err != nil {
		return err
	}
	if !rec.Finalized {
		if _, err := rows.Publish(ctx, key, now); err != nil {
			if !errors.Is(err, ErrStoryFinalized) {
				return err
			}
		}
	}

	_, err = rows.TombstoneStory(ctx, key, now)
	return err
}
