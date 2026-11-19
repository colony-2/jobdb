package sqlite

import (
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
)

// Called only around owned storage operations, after request/payload validation.
// Private chapter-store semantic errors retain their reconciliation behavior.
func submissionStorageError(err error, operation string, key jobdb.JobKey, ordinal *int64) error {
	if errors.Is(err, core.ErrConflict) || errors.Is(err, core.ErrNotFound) || errors.Is(err, storage.ErrChapterNotFound) || errors.Is(err, storage.ErrStoryNotFound) {
		return err
	}
	return jobdb.ClassifyRuntimeIO(err, operation, &key, ordinal)
}
