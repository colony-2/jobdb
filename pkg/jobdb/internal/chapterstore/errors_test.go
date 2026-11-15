package chapterstore

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/storage"
	"github.com/stretchr/testify/require"
)

func TestTranslateRowErrorPreservesMissingChapter(t *testing.T) {
	for _, err := range []error{storage.ErrChapterNotFound, fmt.Errorf("read: %w", storage.ErrChapterNotFound)} {
		translated := translateRowError(err)
		require.ErrorIs(t, translated, storage.ErrChapterNotFound)
		require.ErrorIs(t, translated, core.ErrNotFound)
	}
	translated := translateRowError(storage.ErrStoryNotFound)
	require.ErrorIs(t, translated, core.ErrNotFound)
	require.False(t, errors.Is(translated, storage.ErrChapterNotFound))
	for _, err := range []error{storage.ErrStoryExists, storage.ErrChapterExists, storage.ErrStoryFinalized, storage.ErrInvalidRecord} {
		require.ErrorIs(t, translateRowError(err), core.ErrConflict)
	}
	require.ErrorIs(t, translateRowError(io.ErrUnexpectedEOF), io.ErrUnexpectedEOF)
	require.NoError(t, translateRowError(nil))
}
