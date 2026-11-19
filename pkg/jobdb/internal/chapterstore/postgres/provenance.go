package postgres

import (
	"context"
	"database/sql"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
)

// ReadProvenanceBody checks the stored length before fetching a body. The second
// query repeats the length predicate so a concurrent replacement cannot escape
// the bound. It never reads artifact tables.
func (s *Store) ReadProvenanceBody(ctx context.Context, key storage.StoryKey, ordinal int64, limit int64) ([]byte, int64, bool, error) {
	var size int64
	err := s.db.QueryRowContext(ctx, `SELECT octet_length(body) FROM jobdb_chapter_chapters WHERE anthology_id=$1 AND story_id=$2 AND ordinal=$3`, key.AnthologyID, key.StoryID, ordinal).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	if size > limit {
		return nil, size, true, nil
	}
	var body []byte
	err = s.db.QueryRowContext(ctx, `SELECT body FROM jobdb_chapter_chapters WHERE anthology_id=$1 AND story_id=$2 AND ordinal=$3 AND octet_length(body)<=$4`, key.AnthologyID, key.StoryID, ordinal, limit).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, false, nil
	}
	return body, size, true, err
}
