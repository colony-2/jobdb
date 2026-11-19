package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type appendPublicationFault struct {
	appendScheduler
	fail    bool
	winning runtimecore.CreateJobRequest
}

func (s *appendPublicationFault) CreateJob(ctx context.Context, q runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	if s.fail {
		s.fail = false
		s.winning = q
		return runtimecore.StoredJob{}, errors.New("publication interrupted")
	}
	return s.appendScheduler.CreateJob(ctx, q)
}
func sqliteAppendRequest() jobdb.StartOrAppendJobRequest {
	return jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "key", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("winner", jobdb.NewArtifactFromBytes("piece", []byte("saved artifact")))}, Append: json.RawMessage(`1`)}}
}

// Seed the old on-disk contract, without a production option for old writers.
// The start chapter and publication facts use the actual legacy key and ports.
func seedLegacyAppend(t *testing.T, ctx context.Context, r *Runtime, gen int64, predecessor string, publish bool, created time.Time) runtimecore.CreateJobRequest {
	t.Helper()
	cell, err := (appendScheduler{r}).ResolveAppendCell(ctx, sqliteAppendRequest().Job.Job)
	require.NoError(t, err)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%q/%q/%q", cell.CellID, "batch", "key")))
	key := jobdb.JobKey{TenantId: "tenant", JobId: fmt.Sprintf("ak-%x-%019d", sum[:16], gen)}
	schema, err := r.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "tenant", Schema: json.RawMessage(`{"chapterShape":true,"appendShape":{"type":"integer"}}`)})
	require.NoError(t, err)
	facts := runtimecore.CreateJobRequest{JobKey: key, Cell: cell, JobType: "batch", AppendKey: "key", AppendGeneration: gen, CreatedAt: created, WorkerID: "old-winner", AppMetadata: json.RawMessage(`{"owner":"winner"}`), RunPolicy: jobdb.DefaultRunPolicy(), SchemaHash: schema.SchemaHash, ClientPayload: json.RawMessage(`{"winner":true}`), ClientPayloadRevision: 1}
	facts.InitialPayloadDigest, err = clientpayload.Digest(facts.ClientPayload)
	require.NoError(t, err)
	inputHash, err := computeInputHash(ctx, sqliteAppendRequest().Job.Job.Data)
	require.NoError(t, err)
	var prereqs []jobdb.JobPrerequisite
	if predecessor != "" {
		facts.WaitForJobIDs = []string{predecessor}
		prereqs = []jobdb.JobPrerequisite{{JobID: predecessor, Condition: jobdb.JobPrereqComplete}}
	}
	publication, err := json.Marshal(facts)
	require.NoError(t, err)
	meta, err := json.Marshal(runtimecodec.ChapterMeta{Version: runtimecodec.EnvelopeVersion, Ordinal: 0, TaskType: "batch", CreatedAt: created, InputHash: inputHash, RunPolicy: &facts.RunPolicy, Prerequisites: prereqs, AppendPublication: publication})
	require.NoError(t, err)
	metadata, err := runtimecodec.ChapterMetadataFromJSON(meta)
	require.NoError(t, err)
	ch, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: jobdb.Chapter{Ordinal: 0, TaskType: "batch", CreatedAt: created, InputHash: inputHash, Metadata: metadata, Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`"winner"`)}, Append: &jobdb.ApplicationInputBytes{Data: []byte(`1`)}}}, ArtifactUploads: []jobdb.ArtifactUpload{{Name: "piece", Size: 14, Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("saved artifact")), nil }}}})
	require.NoError(t, err)
	require.NoError(t, (appendChapters{r}).Create(ctx, runtimecore.ChapterLogKey{JobKey: key}, ch))
	if publish {
		prepared, err := (appendScheduler{r}).PrepareJob(ctx, runtimecore.JobPreparation{Key: key, Cell: cell.Cell, CellRepo: cell.CellRepo, Target: cell, Descriptor: fmt.Sprintf(`{"kind":"start-or-append","id":%q}`, key.JobId)})
		require.NoError(t, err)
		facts.GoalID = prepared.GoalID
		_, err = (appendScheduler{r}).CreateJob(ctx, facts)
		require.NoError(t, err)
	}
	return facts
}

func TestAppendRecoveryAfterReopen(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, successor := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy=%v/successor=%v", legacy, successor), func(t *testing.T) {
				ctx := celltest.Context(context.Background())
				cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")}
				r, err := NewFromConfig(ctx, cfg)
				require.NoError(t, err)
				defer func() { require.NoError(t, r.Close(ctx)) }()
				_, err = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
				require.NoError(t, err)
				gen := int64(1)
				predecessor := ""
				created := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
				if successor {
					old := seedLegacyAppend(t, ctx, r, 1, "", true, created)
					predecessor = old.JobKey.JobId
					lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: old.JobKey, WorkerID: "predecessor-worker", Routes: []jobdb.Route{{JobType: "batch"}}, LeaseDuration: time.Hour})
					require.NoError(t, err)
					require.NotNil(t, lease)
					gen = 2
				}
				q := sqliteAppendRequest()
				q.RequestTime = created.Add(time.Hour)
				if successor {
					q.Job.MaxInputs = 1
				}
				var winning runtimecore.CreateJobRequest
				if legacy {
					winning = seedLegacyAppend(t, ctx, r, gen, predecessor, false, q.RequestTime)
				} else {
					q.Job.Job.Schema = &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{"chapterShape":true,"appendShape":{"type":"integer"}}`)}
					q.Job.Job.Metadata = json.RawMessage(`{"owner":"winner"}`)
					q.Job.Job.ClientPayloadUpdate = &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"winner":true}`)}
					scheduler := &appendPublicationFault{appendScheduler: appendScheduler{r}, fail: true}
					_, err = (runtimecore.AppendService{Scheduler: scheduler, Chapters: appendChapters{r}, Schemas: r}).StartOrAppendJob(ctx, q)
					require.EqualError(t, err, "publication interrupted")
					winning = scheduler.winning
				}
				key := winning.JobKey
				encodedBefore, err := (appendChapters{r}).Get(ctx, runtimecore.ChapterLogKey{JobKey: key}, 0)
				require.NoError(t, err)
				before, err := runtimecore.DecodeChapter(encodedBefore)
				require.NoError(t, err)
				require.NoError(t, r.Close(ctx))
				r, err = NewFromConfig(ctx, cfg)
				require.NoError(t, err)
				q.Job.MaxInputs = 50
				q.Job.Job.Data = jobdb.NewTaskDataOrPanic("loser")
				q.Job.Job.Metadata = json.RawMessage(`invalid`)
				q.Job.Job.Schema = &jobdb.JobSchemaSelector{Schema: json.RawMessage(`{"chapterShape":false}`)}
				q.Job.Append = json.RawMessage(`2`)
				h, err := r.StartOrAppendJob(ctx, q)
				require.NoError(t, err)
				require.Equal(t, key, h.JobKey)
				chapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
				require.NoError(t, err)
				require.Len(t, chapters, 2)
				require.Equal(t, before, chapters[0])
				require.Equal(t, `2`, string(chapters[1].Body.(jobdb.AdditionalInputChapter).Input.Data))
				assertStartArtifact(t, ctx, r, key, "piece", []byte("saved artifact"))
				jobs, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobKeys: []jobdb.JobKey{key}})
				require.NoError(t, err)
				require.Len(t, jobs.Jobs, 1)
				got := jobs.Jobs[0]
				require.Equal(t, winning.SchemaHash, got.SchemaHash)
				require.JSONEq(t, string(winning.AppMetadata), string(got.Metadata))
				require.Equal(t, winning.ClientPayload, got.ClientPayload)
				require.Equal(t, winning.ClientPayloadRevision, got.ClientPayloadRevision)
				require.Equal(t, winning.RunPolicy, got.ExecutionState.RunPolicy)
				require.True(t, winning.CreatedAt.Equal(got.CreatedAt), "created_at must come from winning chapter: want %s, got %s", winning.CreatedAt, got.CreatedAt)
				require.True(t, winning.CreatedAt.Equal(got.AvailableAt), "default availability must come from winning chapter")
				if successor {
					info, err := r.GetJob(ctx, key)
					require.NoError(t, err)
					require.Equal(t, jobdb.JobStatusPendingJobs, info.Status)
					require.Equal(t, []string{predecessor}, info.WaitFor)
					leaseReq := jobdb.GetJobLeaseRequest{JobKey: key, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "batch"}}, LeaseDuration: time.Minute}
					lease, err := r.GetJobLease(ctx, leaseReq)
					require.NoError(t, err)
					require.Nil(t, lease)
					require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: predecessor}}))
					lease, err = r.GetJobLease(ctx, leaseReq)
					require.NoError(t, err)
					require.NotNil(t, lease)
				}
			})
		}
	}
}

func TestAppendLegacyChainAndOrdering(t *testing.T) {
	ctx := celltest.Context(context.Background())
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "jobs.db")}
	r, err := NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close(ctx)) }()
	_, err = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
	require.NoError(t, err)
	legacy := seedLegacyAppend(t, ctx, r, 1, "", true, time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	q := sqliteAppendRequest()
	q.Job.Append = json.RawMessage(`2`)
	appended, err := r.StartOrAppendJob(ctx, q)
	require.NoError(t, err)
	require.Equal(t, legacy.JobKey, appended.JobKey)
	q.Job.MaxInputs = 1
	q.RequestTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	compact, err := r.StartOrAppendJob(ctx, q)
	require.NoError(t, err)
	require.Len(t, compact.JobKey.JobId, 27)
	// Lexical order and creation time disagree with generation order.
	require.True(t, compact.JobKey.JobId < legacy.JobKey.JobId)
	require.NoError(t, r.Close(ctx))
	r, err = NewFromConfig(ctx, cfg)
	require.NoError(t, err)
	tail, found, err := (appendScheduler{r}).GetAppendTail(ctx, runtimecore.AppendChain{TenantID: "tenant", CellID: legacy.Cell.CellID, JobType: "batch", AppendKey: "key"})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, compact.JobKey, tail.JobKey)
	require.Equal(t, int64(2), tail.AppendGeneration)
	q.Job.MaxInputs = 50
	appended, err = r.StartOrAppendJob(ctx, q)
	require.NoError(t, err)
	require.Equal(t, compact.JobKey, appended.JobKey)
	for _, key := range []jobdb.JobKey{legacy.JobKey, compact.JobKey} {
		info, err := r.GetJob(ctx, key)
		require.NoError(t, err)
		require.Equal(t, "key", info.AppendKey)
		chapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: key})
		require.NoError(t, err)
		require.Len(t, chapters, 2)
		assertStartArtifact(t, ctx, r, key, "piece", []byte("saved artifact"))
	}
	// Add a newer compact successor whose ID sorts below the legacy ID. The
	// earlier tail check exercises generation order against reversed chronology;
	// this listing exercises chronology against reversed lexical ID order.
	q.Job.MaxInputs = 1
	q.RequestTime = time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)
	newest, err := r.StartOrAppendJob(ctx, q)
	require.NoError(t, err)
	require.True(t, newest.JobKey.JobId < legacy.JobKey.JobId)
	// Listings are descending created_at, independently of IDs and append_gen.
	page, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, AppendKeys: []string{"key"}, PageSize: 1})
	require.NoError(t, err)
	require.Len(t, page.Jobs, 1)
	require.Equal(t, newest.JobKey, page.Jobs[0].JobKey)
	require.NotEmpty(t, page.NextPageToken)
	next, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, AppendKeys: []string{"key"}, PageSize: 1, PageToken: page.NextPageToken})
	require.NoError(t, err)
	require.Len(t, next.Jobs, 1)
	require.Equal(t, legacy.JobKey, next.Jobs[0].JobKey)
	require.NotEmpty(t, next.NextPageToken)
	last, err := r.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, AppendKeys: []string{"key"}, PageSize: 1, PageToken: next.NextPageToken})
	require.NoError(t, err)
	require.Len(t, last.Jobs, 1)
	require.Equal(t, compact.JobKey, last.Jobs[0].JobKey)
	require.Empty(t, last.NextPageToken)
	info, err := r.GetJob(ctx, compact.JobKey)
	require.NoError(t, err)
	require.Equal(t, []string{legacy.JobKey.JobId}, info.WaitFor)
	require.NoError(t, r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: legacy.JobKey}))
	lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: compact.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "batch"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	require.NotNil(t, lease)
}
