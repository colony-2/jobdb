package sqlite

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestProvenanceSQLitePhysicalPagesAndAuthorizationProjection(t *testing.T) {
	ctx := celltest.Context(context.Background())
	r, err := NewFromConfig(ctx, Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "jobs.db")})
	require.NoError(t, err)
	defer r.Close(ctx)
	h, err := celltest.Submit(r, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "provenance", Cell: "test", CellRepo: "example.test/test", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	key := runtimecore.ChapterLogKey{JobKey: h.JobKey}
	// Raw stored bodies can contain unrecognized application data. No strict
	// decoder or artifact table is needed for this body-only source.
	body := protowire.AppendVarint(protowire.AppendTag(nil, 20, protowire.VarintType), 2)
	body = protowire.AppendBytes(protowire.AppendTag(body, 30, protowire.BytesType), nil)
	padding := protowire.AppendBytes(protowire.AppendTag(append([]byte(nil), body...), 100, protowire.BytesType), bytes.Repeat([]byte("x"), 600000))
	for n := int64(1); n <= 260; n++ {
		b := body
		if n == 1 || n == 2 {
			b = padding
		}
		if n == 3 {
			b = bytes.Repeat([]byte{0xff}, (1<<20)+1)
		}
		_, err = r.db.Exec(`INSERT INTO jobdb_chapter_chapters(anthology_id,story_id,ordinal,body,created_at_ns) VALUES (?,?,?,?,?)`, h.JobKey.TenantId, h.JobKey.JobId, n, b, time.Now().UnixNano())
		require.NoError(t, err)
	}
	_, err = r.db.Exec(`UPDATE jobdb_chapter_stories SET latest_ordinal=260,chapter_count=261 WHERE story_id=?`, h.JobKey.JobId)
	require.NoError(t, err)
	ceiling, err := r.chapterStore.ExecutionProvenanceCeiling(ctx, key)
	require.NoError(t, err)
	require.EqualValues(t, 260, *ceiling)
	q := runtimecore.ProvenanceChapterPageRequest{Key: key, ThroughOrdinal: *ceiling, StartOrdinal: 1, Records: 256, Bytes: 1 << 20}
	page, err := r.chapterStore.ReadProvenanceChapterPage(ctx, q)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	require.EqualValues(t, 2, page.NextOrdinal)
	require.Len(t, page.Entries[0].Body, len(padding))
	q.StartOrdinal = page.NextOrdinal
	page, err = r.chapterStore.ReadProvenanceChapterPage(ctx, q)
	require.NoError(t, err)
	require.Len(t, page.Entries, 256)
	require.Equal(t, jobdb.ExecutionProvenanceOversizedRecord, page.Entries[1].Issue)
	require.Empty(t, page.Entries[1].Body)
	// A physical hole does not lower the committed ceiling or become complete.
	_, err = r.db.Exec(`DELETE FROM jobdb_chapter_chapters WHERE story_id=? AND ordinal=4`, h.JobKey.JobId)
	require.NoError(t, err)
	// Artifact access would now fail. Neither raw scanning nor authorization
	// projection failure should enumerate, hash or open these tables.
	_, err = r.db.Exec(`DROP TABLE jobdb_chapter_artifacts`)
	require.NoError(t, err)
	req := jobdb.ReadExecutionProvenanceRequest{JobKey: h.JobKey, Consumer: "c2j"}
	out, err := r.ReadExecutionProvenance(ctx, req)
	require.NoError(t, err)
	require.NotEqual(t, jobdb.ExecutionProvenanceComplete, out.Status)
	require.EqualValues(t, 260, *out.ThroughOrdinal)
	require.GreaterOrEqual(t, out.Usage.Pages, 3)
	var gap, oversized bool
	for _, issue := range out.Issues {
		gap = gap || issue.Reason == jobdb.ExecutionProvenanceMissingRecord
		oversized = oversized || issue.Reason == jobdb.ExecutionProvenanceOversizedRecord
	}
	require.True(t, gap)
	require.True(t, oversized)
	_, err = r.db.Exec(`UPDATE jobdb_jobs SET payload=? WHERE job_id=?`, []byte{0xff}, h.JobKey.JobId)
	require.NoError(t, err)
	out, err = r.ReadExecutionProvenance(ctx, req)
	require.NoError(t, err)
	require.Equal(t, jobdb.ExecutionProvenanceUnavailable, out.Status)
	require.Empty(t, out.Pairs)
	require.Zero(t, out.Usage.Pages)
	require.Zero(t, out.Usage.ScannedBytes)
	_, err = r.db.Exec(`UPDATE jobdb_jobs SET hidden_at_ns=? WHERE job_id=?`, time.Now().UnixNano(), h.JobKey.JobId)
	require.NoError(t, err)
	_, err = r.ReadExecutionProvenance(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrJobNotFound)
}
