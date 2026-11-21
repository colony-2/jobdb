package runtimecore

import "github.com/colony-2/jobdb/pkg/jobdb/celltest"

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func TestAppendCompactIDVectors(t *testing.T) {
	for _, v := range []struct{ cell, typ, key, id, prefix string }{
		{"cell", "batch", "key", "Hf8hO0e442nzjOnvpy7z9iePAn3", "7bcb7cea82a2f1d2199170d7cb427e40"},
		{"other", "batch", "key", "AiePJocYP6hPwzzkGgVDj5DI8Qr", "4b21e289d7a69b58a85758fe16d105b9"},
		{"cell", "other", "key", "ZK3JP2wTMhRtniXpen2I3gal7RJ", "f79066a97f28d2912c6851b1c56da5dd"},
		{"cell", "batch", "other", "6xMY4cPHgbZephin22Gn2FLDyrp", "30c2ed88075bdc68d8caca4b9e95bf92"},
		{"cell/\"雪", "batch", "key/\"é", "DFuZtPnuFfLz6AU2vAhg8Ku37Kb", "5ce8be74ab4b4a4a577b10f2e55afda0"},
	} {
		t.Run(v.id, func(t *testing.T) {
			chain := AppendChain{TenantID: "tenant", CellID: v.cell, JobType: v.typ, AppendKey: v.key}
			for range 2 {
				id, err := compactAppendJobID(chain, 1)
				require.NoError(t, err)
				require.Equal(t, v.id, id)
				require.Regexp(t, `^[0-9A-Za-z]{27}$`, id)
				decoded, err := ksuid.Parse(id)
				require.NoError(t, err)
				require.Equal(t, v.prefix+"00000001", hex.EncodeToString(decoded.Bytes()))
			}
			// Tenant scopes storage keys; it is deliberately outside the digest.
			chain.TenantID = "another"
			id, err := compactAppendJobID(chain, 1)
			require.NoError(t, err)
			require.Equal(t, v.id, id)
		})
	}
}

func TestAppendCompactIDGenerationOrderAndBounds(t *testing.T) {
	chain := AppendChain{CellID: "cell", JobType: "batch", AppendKey: "key"}
	previous := ""
	for _, gen := range []int64{1, 255, 256, 65535, 65536, 16777215, 16777216, math.MaxUint32 - 1, math.MaxUint32} {
		id, err := compactAppendJobID(chain, gen)
		require.NoError(t, err)
		require.True(t, previous < id, "%q must precede %q", previous, id)
		parsed, err := ksuid.Parse(id)
		require.NoError(t, err)
		require.Equal(t, "7bcb7cea82a2f1d2199170d7cb427e40"+fmt.Sprintf("%08x", gen), hex.EncodeToString(parsed.Bytes()))
		previous = id
	}
	for _, gen := range []int64{-1, 0, math.MaxUint32 + 1, math.MaxInt64} {
		id, err := compactAppendJobID(chain, gen)
		require.ErrorIs(t, err, jobdb.ErrInvalidAppendRequest)
		require.Empty(t, id)
	}
}

// This fixture implements only the shared append ports. Chapter ordinal CAS and
// scheduler publication are synchronized, and unrelated operations cannot pass.
type appendFixture struct {
	mu                                  sync.Mutex
	cell                                jobdb.ResolvedCell
	tail                                StoredJob
	found                               bool
	logs                                map[jobdb.JobKey][]EncodedChapter
	facts                               map[jobdb.JobKey]CreateJobRequest
	preparations, publications, creates int
	failPublication                     bool
	legacyReadError                     error
	missing                             error
	// Used to force two creators past their initial read into the empty-container CAS.
	createBarrier *sync.WaitGroup
}

func newAppendFixture() (*appendFixture, AppendService, jobdb.StartOrAppendJobRequest) {
	f := &appendFixture{cell: jobdb.ResolvedCell{Cell: "test", CellID: "cell", CellRepo: "example.test/test"}, logs: map[jobdb.JobKey][]EncodedChapter{}, facts: map[jobdb.JobKey]CreateJobRequest{}, missing: jobdb.ErrJobNotFound}
	return f, AppendService{Projects: celltest.UnrestrictedProjects{}, Scheduler: f, Chapters: f}, jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "key", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("winner")}, Append: json.RawMessage(`1`)}}
}
func (f *appendFixture) ResolveAppendCell(context.Context, jobdb.SubmitJob) (jobdb.ResolvedCell, error) {
	return f.cell, nil
}
func (f *appendFixture) GetAppendTail(context.Context, AppendChain) (StoredJob, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tail, f.found, nil
}
func (f *appendFixture) ClaimAppend(_ context.Context, q ClaimAppendRequest) (LeaseIdentity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tail.LeaseExpiresAt != nil {
		return LeaseIdentity{}, jobdb.ErrConflict
	}
	at := q.Now.Add(q.Duration)
	f.tail.LeaseExpiresAt = &at
	f.tail.LeaseWorkerID = q.WorkerID
	return LeaseIdentity{JobKey: q.JobKey, WorkerID: q.WorkerID}, nil
}
func (f *appendFixture) ReleaseAppend(context.Context, LeaseIdentity) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tail.LeaseExpiresAt = nil
	f.tail.LeaseWorkerID = ""
	return nil
}
func (f *appendFixture) PrepareJob(_ context.Context, q JobPreparation) (PreparedJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.preparations++
	return PreparedJob{Key: q.Key, Target: f.cell, GoalID: "goal-" + q.Key.JobId}, nil
}
func (f *appendFixture) CreateJob(_ context.Context, q CreateJobRequest) (StoredJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publications++
	if f.failPublication {
		f.failPublication = false
		return StoredJob{}, errors.New("publication interrupted")
	}
	if _, ok := f.facts[q.JobKey]; !ok {
		f.facts[q.JobKey] = q
		if !f.found || f.tail.AppendGeneration < q.AppendGeneration {
			f.tail = StoredJob{JobKey: q.JobKey, Cell: q.Cell, JobType: q.JobType, GoalID: q.GoalID, AppendKey: q.AppendKey, AppendGeneration: q.AppendGeneration, SchemaHash: q.SchemaHash}
			f.found = true
		}
	}
	return StoredJob{JobKey: q.JobKey, Cell: q.Cell, GoalID: q.GoalID}, nil
}
func (f *appendFixture) Get(ctx context.Context, k ChapterLogKey, ordinal int64) (EncodedChapter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return EncodedChapter{}, err
	}
	if len(k.JobKey.JobId) > 3 && k.JobKey.JobId[:3] == "ak-" && f.legacyReadError != nil {
		return EncodedChapter{}, f.legacyReadError
	}
	log, ok := f.logs[k.JobKey]
	if !ok {
		return EncodedChapter{}, f.missing
	}
	if ordinal >= int64(len(log)) {
		return EncodedChapter{}, jobdb.ErrChapterNotFound
	}
	return log[ordinal], nil
}
func (f *appendFixture) Count(_ context.Context, k ChapterLogKey) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int64(len(f.logs[k.JobKey])), nil
}
func (f *appendFixture) Create(_ context.Context, k ChapterLogKey, ch EncodedChapter) error {
	if f.createBarrier != nil {
		f.createBarrier.Done()
		f.createBarrier.Wait()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	if _, ok := f.logs[k.JobKey]; ok {
		return jobdb.ErrConflict
	}
	f.logs[k.JobKey] = []EncodedChapter{ch}
	return nil
}
func (f *appendFixture) Append(_ context.Context, k ChapterLogKey, ch EncodedChapter) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	log, ok := f.logs[k.JobKey]
	if !ok {
		return jobdb.ErrJobNotFound
	}
	if ch.Ordinal != int64(len(log)) {
		return jobdb.ErrConflict
	}
	f.logs[k.JobKey] = append(log, ch)
	return nil
}
func (f *appendFixture) seedTail(gen int64, open bool) {
	f.found = true
	f.tail = StoredJob{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: fmt.Sprintf("ak-7bcb7cea82a2f1d2199170d7cb427e40-%019d", gen)}, Cell: f.cell, JobType: "batch", AppendKey: "key", AppendGeneration: gen}
	if !open {
		f.tail.CancelRequested = true
		return
	}
	ch, err := EncodeChapter(EncodeChapterRequest{Chapter: jobdb.Chapter{Ordinal: 0, TaskType: "batch", Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`null`)}, Append: &jobdb.ApplicationInputBytes{Data: []byte(`0`)}}}})
	if err != nil {
		panic(err)
	}
	f.logs[f.tail.JobKey] = []EncodedChapter{ch}
}

type appendTestSchema struct {
	jobdb.JobSchemaRegistry // Unexpected registry operations fail.
	info                    jobdb.JobSchemaInfo
}

func (r appendTestSchema) GetJobSchema(_ context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	if key.SchemaHash != r.info.SchemaHash {
		return jobdb.JobSchemaInfo{}, jobdb.ErrJobSchemaNotFound
	}
	return r.info, nil
}

func TestAppendGenerationExhaustion(t *testing.T) {
	for _, gen := range []int64{math.MaxUint32, math.MaxUint32 + 1, math.MaxInt64} {
		for _, open := range []bool{true, false} {
			t.Run(fmt.Sprintf("%d/open=%v", gen, open), func(t *testing.T) {
				f, s, q := newAppendFixture()
				f.seedTail(gen, open)
				h, err := s.StartOrAppendJob(context.Background(), q)
				if open {
					require.NoError(t, err)
					require.Equal(t, f.tail.JobKey, h.JobKey)
					require.Len(t, f.logs[h.JobKey], 2)
				} else {
					require.ErrorIs(t, err, jobdb.ErrInvalidAppendRequest)
					require.Contains(t, err.Error(), "append generation exhausted (maximum 4294967295)")
				}
				require.Zero(t, f.creates)
				require.Zero(t, f.preparations)
				require.Zero(t, f.publications)
			})
		}
	}
	f, s, q := newAppendFixture()
	f.seedTail(math.MaxUint32-1, false)
	h, err := s.StartOrAppendJob(context.Background(), q)
	require.NoError(t, err)
	id, err := ksuid.Parse(h.JobKey.JobId)
	require.NoError(t, err)
	require.Equal(t, "7bcb7cea82a2f1d2199170d7cb427e40ffffffff", hex.EncodeToString(id.Bytes()))
	require.Equal(t, int64(math.MaxUint32), f.facts[h.JobKey].AppendGeneration)
}

func TestAppendIDIgnoresRequestTimeAndWorker(t *testing.T) {
	for i := range 2 {
		_, s, q := newAppendFixture()
		q.WorkerID = fmt.Sprint(i)
		q.RequestTime = time.Unix(int64(i)*1000000, 0)
		h, err := s.StartOrAppendJob(context.Background(), q)
		require.NoError(t, err)
		require.Equal(t, "Hf8hO0e442nzjOnvpy7z9iePAn3", h.JobKey.JobId)
	}
}

func TestAppendRecoversWinningChapter(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, gen := range []int64{1, 2, math.MaxUint32} {
			t.Run(fmt.Sprintf("legacy=%v/generation=%d", legacy, gen), func(t *testing.T) {
				f, s, q := newAppendFixture()
				if gen > 1 {
					f.seedTail(gen-1, false)
				}
				predecessor := f.tail.JobKey.JobId
				chain := AppendChain{TenantID: "tenant", CellID: "cell", JobType: "batch", AppendKey: "key"}
				id, err := compactAppendJobID(chain, gen)
				require.NoError(t, err)
				if legacy {
					id = fmt.Sprintf("ak-7bcb7cea82a2f1d2199170d7cb427e40-%019d", gen)
				}
				key := jobdb.JobKey{TenantId: "tenant", JobId: id}
				at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
				q.RequestTime = at.Add(-time.Hour)
				q.WorkerID = "winning-worker"
				q.Job.Job.AvailableAt = &at
				q.Job.Job.Metadata = json.RawMessage(`{"owner":"winner"}`)
				q.Job.Job.ClientPayloadUpdate = &jobdb.ClientPayloadUpdate{Mode: "reset", Value: json.RawMessage(`{"winner":true}`)}
				q.Job.Job.RunPolicy = jobdb.DefaultRunPolicy()
				q.Job.Job.RunPolicy.Retry.MaximumAttempts = 7
				schema := json.RawMessage(`{"chapterShape":true,"appendShape":{"type":"integer"}}`)
				hash, canonical, err := jobdb.JobSchemaHash(schema)
				require.NoError(t, err)
				s.Schemas = appendTestSchema{info: jobdb.JobSchemaInfo{SchemaHash: hash, Schema: canonical, State: jobdb.JobSchemaStateActive}}
				q.Job.Job.Schema = &jobdb.JobSchemaSelector{Hash: hash}
				q.Job.AppendArtifacts = []jobdb.ArtifactKey{{JobId: "source", TaskOrdinal: 2, Name: "piece", SizeBytes: 10}}
				f.failPublication = true
				// Seed a genuine old-format start using the unchanged creation path, or
				// exercise the public path for compact creation.
				if legacy {
					_, _, err = s.create(context.Background(), q, q.Job, f.cell, key, f.tail, f.found, gen, time.Now())
				} else {
					_, err = s.StartOrAppendJob(context.Background(), q)
				}
				var failure *jobdb.ExecutionInfrastructureError
				require.ErrorAs(t, err, &failure)
				require.Equal(t, jobdb.ExecutionInfrastructureRuntimeIO, failure.Reason)
				require.Equal(t, "publish_append_job", failure.Operation)
				require.Equal(t, &key, failure.JobKey)
				require.Nil(t, failure.Ordinal)
				require.EqualError(t, failure.Err, "publication interrupted")
				require.Empty(t, f.facts)
				original := f.logs[key][0]
				ch, err := DecodeChapter(original)
				require.NoError(t, err)
				meta, err := initialChapterMetadata(ch.Metadata)
				require.NoError(t, err)
				var want CreateJobRequest
				require.NoError(t, json.Unmarshal(meta.AppendPublication, &want))
				// Recovery must never re-prepare or validate this invalid losing template.
				q.Job.Job.Data = jobdb.NewTaskDataOrPanic("loser")
				q.Job.Job.Metadata = json.RawMessage(`invalid`)
				q.Job.Job.CellRepo = "invalid"
				q.Job.Job.AvailableAt = nil
				q.Job.Job.ClientPayloadUpdate = nil
				q.Job.Job.Schema = &jobdb.JobSchemaSelector{Hash: "invalid-losing-schema"}
				q.Job.Job.RunPolicy = jobdb.RunPolicy{}
				q.Job.Append = json.RawMessage(`2`)
				h, err := s.StartOrAppendJob(context.Background(), q)
				require.NoError(t, err)
				require.Equal(t, key, h.JobKey)
				want.GoalID = "goal-" + id
				require.Equal(t, want, f.facts[key])
				require.Equal(t, original, f.logs[key][0])
				require.Len(t, f.logs[key], 2)
				next, err := DecodeChapter(f.logs[key][1])
				require.NoError(t, err)
				require.Equal(t, `2`, string(next.Body.(jobdb.AdditionalInputChapter).Input.Data))
				if gen > 1 {
					require.Equal(t, []string{predecessor}, want.WaitForJobIDs)
					require.Equal(t, predecessor, meta.Prerequisites[0].JobID)
				}
				require.Len(t, f.facts, 1)
				require.Equal(t, 1, f.creates)
			})
		}
	}
}

func TestAppendLegacyLookupFailures(t *testing.T) {
	for _, fault := range []error{errors.New("storage unavailable"), context.Canceled, context.DeadlineExceeded} {
		f, s, q := newAppendFixture()
		f.legacyReadError = fault
		_, err := s.StartOrAppendJob(context.Background(), q)
		require.ErrorIs(t, err, fault)
		require.Zero(t, f.creates)
		require.Zero(t, f.preparations)
		require.Zero(t, f.publications)
	}
	for _, missing := range []error{jobdb.ErrJobNotFound, jobdb.ErrChapterNotFound} {
		f, s, q := newAppendFixture()
		f.missing = missing
		h, err := s.StartOrAppendJob(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, h.JobKey.JobId, 27)
	}
	for _, publication := range []json.RawMessage{json.RawMessage(`{"AppendKey":"key"}`), json.RawMessage(`{"JobKey":{"TenantId":"tenant","JobId":"wrong"},"AppendKey":"key","AppendGeneration":1}`), nil} {
		f, s, q := newAppendFixture()
		meta := runtimecodec.ChapterMeta{Version: runtimecodec.EnvelopeVersion, AppendPublication: publication}
		raw, err := json.Marshal(meta)
		require.NoError(t, err)
		metadata, err := runtimecodec.ChapterMetadataFromJSON(raw)
		require.NoError(t, err)
		ch, err := EncodeChapter(EncodeChapterRequest{Chapter: jobdb.Chapter{Ordinal: 0, TaskType: "batch", Metadata: metadata, Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`null`)}}}})
		require.NoError(t, err)
		key := jobdb.JobKey{TenantId: "tenant", JobId: "ak-7bcb7cea82a2f1d2199170d7cb427e40-0000000000000000001"}
		f.logs[key] = []EncodedChapter{ch}
		_, err = s.StartOrAppendJob(context.Background(), q)
		require.Error(t, err)
		require.Zero(t, f.creates)
		require.Zero(t, f.preparations)
		require.Zero(t, f.publications)
	}
}

func TestAppendConcurrentEmptyContainer(t *testing.T) {
	f, s, q := newAppendFixture()
	key := jobdb.JobKey{TenantId: "tenant", JobId: "Hf8hO0e442nzjOnvpy7z9iePAn3"}
	f.logs[key] = nil
	f.createBarrier = &sync.WaitGroup{}
	f.createBarrier.Add(2)
	var wg sync.WaitGroup
	handles := make([]jobdb.JobHandle, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := q
			req.Job.Append = json.RawMessage(fmt.Sprint(i))
			handles[i], errs[i] = s.StartOrAppendJob(context.Background(), req)
		}(i)
	}
	wg.Wait()
	for i := range 2 {
		require.NoError(t, errs[i])
		require.Equal(t, key, handles[i].JobKey)
	}
	require.Len(t, f.logs[key], 2)
	require.Len(t, f.facts, 1)
	seen := map[string]bool{}
	for _, encoded := range f.logs[key] {
		ch, err := DecodeChapter(encoded)
		require.NoError(t, err)
		switch b := ch.Body.(type) {
		case jobdb.JobStartChapter:
			seen[string(b.Append.Data)] = true
		case jobdb.AdditionalInputChapter:
			seen[string(b.Input.Data)] = true
		default:
			t.Fatalf("unexpected chapter %T", b)
		}
	}
	require.Equal(t, map[string]bool{"0": true, "1": true}, seen)
}
