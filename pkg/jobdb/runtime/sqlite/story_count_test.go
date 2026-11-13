package sqlite

import (
	"context"
	"errors"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
)

func (r *Runtime) storyChapterCount(ctx context.Context, jobKey jobdb.JobKey) (int64, error) {
	st, err := r.chapterStore.Story(chapterContext(ctx), storyKeyForJob(jobKey))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return 0, jobdb.ErrJobNotFound
		}
		return 0, err
	}
	return st.ChapterCount(), nil
}
