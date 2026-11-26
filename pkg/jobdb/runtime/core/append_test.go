package runtimecore

import "github.com/colony-2/jobdb/pkg/jobdb/celltest"

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
	readErrors                          map[string]error
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
	if err := f.readErrors[k.JobKey.JobId]; err != nil {
		return EncodedChapter{}, err
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
	require.Regexp(t, `^C2[0-9A-Za-z]{27}$`, h.JobKey.JobId)
	id, err := ksuid.Parse(h.JobKey.JobId[2:])
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
		require.Equal(t, "C2Hf8hO0e442nzjOnvpy7z9iePAn3", h.JobKey.JobId)
	}
}

func TestAppendRecoversWinningChapter(t *testing.T) {
	for _, encoding := range []string{"prefixed", "compact", "ak"} {
		for _, gen := range []int64{1, 2, math.MaxUint32} {
			t.Run(fmt.Sprintf("%s/generation=%d", encoding, gen), func(t *testing.T) {
				f, s, q := newAppendFixture()
				if gen > 1 {
					f.seedTail(gen-1, false)
				}
				predecessor := f.tail.JobKey.JobId
				chain := AppendChain{TenantID: "tenant", CellID: "cell", JobType: "batch", AppendKey: "key"}
				id, err := compactAppendJobID(chain, gen)
				require.NoError(t, err)
				switch encoding {
				case "ak":
					id = fmt.Sprintf("ak-7bcb7cea82a2f1d2199170d7cb427e40-%019d", gen)
				case "prefixed":
					id = "C2" + id
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
				if encoding != "prefixed" {
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
				require.Equal(t, 2, f.publications, "one interrupted attempt and one successful publication")
				require.Equal(t, 1, f.creates)
			})
		}
	}
}

var appendFirstIDs = []string{
	"ak-7bcb7cea82a2f1d2199170d7cb427e40-0000000000000000001",
	"Hf8hO0e442nzjOnvpy7z9iePAn3",
	"C2Hf8hO0e442nzjOnvpy7z9iePAn3",
}

func TestAppendLookupFailures(t *testing.T) {
	for _, id := range appendFirstIDs {
		for _, fault := range []error{errors.New("storage unavailable"), context.Canceled, context.DeadlineExceeded} {
			f, s, q := newAppendFixture()
			f.readErrors = map[string]error{id: fault}
			_, err := s.StartOrAppendJob(context.Background(), q)
			require.ErrorIs(t, err, fault)
			require.Zero(t, f.creates)
			require.Zero(t, f.preparations)
			require.Zero(t, f.publications)
		}
	}
	for _, missing := range []error{jobdb.ErrJobNotFound, jobdb.ErrChapterNotFound} {
		f, s, q := newAppendFixture()
		f.missing = missing
		h, err := s.StartOrAppendJob(context.Background(), q)
		require.NoError(t, err)
		require.Equal(t, appendFirstIDs[2], h.JobKey.JobId)
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

func TestAppendRejectsCompetingStarts(test *testing.T) {
	for first, id := range appendFirstIDs {
		for _, other := range appendFirstIDs[first+1:] {
			fixture, service, request := newAppendFixture()
			for _, saved := range []string{id, other} {
				key := jobdb.JobKey{TenantId: "tenant", JobId: saved}
				fixture.failPublication = true
				_, _, err := service.create(context.Background(), request, request.Job, fixture.cell, key, StoredJob{}, false, 1, time.Now())
				require.ErrorContains(test, err, "publication interrupted")
			}
			_, err := service.StartOrAppendJob(context.Background(), request)
			require.ErrorIs(test, err, jobdb.ErrConflict)
			require.Equal(test, 2, fixture.creates)
			require.Equal(test, 2, fixture.preparations)
			require.Equal(test, 2, fixture.publications)
			require.Empty(test, fixture.facts)
		}
	}
}

func TestAppendRejectsMismatchedStartFacts(test *testing.T) {
	for name, alter := range map[string]func(*CreateJobRequest){
		"tenant":     func(facts *CreateJobRequest) { facts.JobKey.TenantId = "other" },
		"key":        func(facts *CreateJobRequest) { facts.JobKey.JobId = "other" },
		"cell":       func(facts *CreateJobRequest) { facts.Cell.CellID = "other" },
		"type":       func(facts *CreateJobRequest) { facts.JobType = "other" },
		"append-key": func(facts *CreateJobRequest) { facts.AppendKey = "other" },
		"generation": func(facts *CreateJobRequest) { facts.AppendGeneration++ },
	} {
		test.Run(name, func(test *testing.T) {
			fixture, service, request := newAppendFixture()
			key := jobdb.JobKey{TenantId: "tenant", JobId: appendFirstIDs[1]}
			facts := CreateJobRequest{JobKey: key, Cell: fixture.cell, JobType: "batch", AppendKey: "key", AppendGeneration: 1}
			alter(&facts)
			publication, err := json.Marshal(facts)
			require.NoError(test, err)
			raw, err := json.Marshal(runtimecodec.ChapterMeta{Version: runtimecodec.EnvelopeVersion, AppendPublication: publication})
			require.NoError(test, err)
			metadata, err := runtimecodec.ChapterMetadataFromJSON(raw)
			require.NoError(test, err)
			chapter, err := EncodeChapter(EncodeChapterRequest{Chapter: jobdb.Chapter{TaskType: "batch", Metadata: metadata, Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`null`)}, Append: &jobdb.ApplicationInputBytes{Data: []byte(`1`)}}}})
			require.NoError(test, err)
			fixture.logs[key] = []EncodedChapter{chapter}
			_, err = service.StartOrAppendJob(context.Background(), request)
			require.ErrorIs(test, err, jobdb.ErrConflict)
			require.Zero(test, fixture.creates)
			require.Zero(test, fixture.preparations)
			require.Zero(test, fixture.publications)
		})
	}
}

func TestAppendConcurrentEmptyContainer(t *testing.T) {
	f, s, q := newAppendFixture()
	key := jobdb.JobKey{TenantId: "tenant", JobId: appendFirstIDs[2]}
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

type appendWaitScheduler struct {
	AppendScheduler
	getTail func(context.Context, AppendChain) (StoredJob, bool, error)
	claim   func(context.Context, ClaimAppendRequest) (LeaseIdentity, error)
}

func (scheduler appendWaitScheduler) GetAppendTail(ctx context.Context, chain AppendChain) (StoredJob, bool, error) {
	return scheduler.getTail(ctx, chain)
}

func (scheduler appendWaitScheduler) ClaimAppend(ctx context.Context, request ClaimAppendRequest) (LeaseIdentity, error) {
	if scheduler.claim != nil {
		return scheduler.claim(ctx, request)
	}
	return scheduler.AppendScheduler.ClaimAppend(ctx, request)
}

type appendWaitChapters struct {
	AppendChapterLog
	beforeAppend func()
}

func (chapters appendWaitChapters) Append(ctx context.Context, key ChapterLogKey, chapter EncodedChapter) error {
	chapters.beforeAppend()
	return chapters.AppendChapterLog.Append(ctx, key, chapter)
}

func TestAppendLiveClaimConcurrentFirstCreator(test *testing.T) {
	synctest.Test(test, func(test *testing.T) {
		const callers = 24
		fixture, service, request := newAppendFixture()
		request.Job.MaxInputs = 100
		type callerKey struct{}
		var firstAppend, holding atomic.Bool
		var observedMu sync.Mutex
		observed := map[int]bool{}
		service.Chapters = appendWaitChapters{fixture, func() {
			if firstAppend.CompareAndSwap(false, true) {
				holding.Store(true)
				time.Sleep(4 * time.Second)
				holding.Store(false)
			}
		}}
		service.Scheduler = appendWaitScheduler{AppendScheduler: fixture, getTail: func(ctx context.Context, chain AppendChain) (StoredJob, bool, error) {
			tail, found, err := fixture.GetAppendTail(ctx, chain)
			if holding.Load() && found && IsAppendWorker(tail.LeaseWorkerID) && tail.LeaseExpiresAt.After(time.Now()) {
				observedMu.Lock()
				observed[ctx.Value(callerKey{}).(int)] = true
				observedMu.Unlock()
			}
			return tail, found, err
		}}
		var group sync.WaitGroup
		var handles [callers]jobdb.JobHandle
		var errs [callers]error
		start := make(chan struct{})
		for index := range callers {
			group.Go(func() {
				<-start
				input := request
				input.Job.Append = json.RawMessage(fmt.Sprint(index))
				input.Job.Job.Data = jobdb.NewTaskDataOrPanic(index)
				handles[index], errs[index] = service.StartOrAppendJob(context.WithValue(test.Context(), callerKey{}, index), input)
			})
		}
		close(start)
		group.Wait()
		require.Len(test, observed, callers-2, "every caller except the creator and holder must see the held claim")
		busy := 0
		for _, err := range errs {
			if errors.Is(err, jobdb.ErrAppendBusy) {
				busy++
			}
		}
		test.Logf("callers=%d observed live hold=%d append busy=%d", callers, len(observed), busy)
		for index, handle := range handles {
			require.NoError(test, errs[index], "caller %d", index)
			require.Equal(test, handles[0], handle)
			require.Regexp(test, `^C2[0-9A-Za-z]{27}$`, handle.JobKey.JobId)
			_, err := ksuid.Parse(strings.TrimPrefix(handle.JobKey.JobId, "C2"))
			require.NoError(test, err)
		}
		require.Len(test, fixture.facts, 1)
		require.Len(test, fixture.logs, 1)
		require.EqualValues(test, 1, fixture.tail.AppendGeneration)
		chapters := fixture.logs[handles[0].JobKey]
		require.Len(test, chapters, callers)
		seen := map[string]bool{}
		for index, encoded := range chapters {
			chapter, err := DecodeChapter(encoded)
			require.NoError(test, err)
			require.EqualValues(test, index, chapter.Ordinal)
			if index == 0 {
				seen[string(chapter.Body.(jobdb.JobStartChapter).Append.Data)] = true
			} else {
				seen[string(chapter.Body.(jobdb.AdditionalInputChapter).Input.Data)] = true
			}
		}
		require.Len(test, seen, callers)
		for index := range callers {
			require.Contains(test, seen, fmt.Sprint(index))
		}
	})
}

func liveAppendTail(tail StoredJob, holder string, expires time.Time) StoredJob {
	tail.LeaseWorkerID = AppendWorkerPrefix + holder
	tail.LeaseExpiresAt = &expires
	return tail
}

func TestAppendLiveClaimWindowStartsAtObservation(test *testing.T) {
	synctest.Test(test, func(test *testing.T) {
		fixture, service, request := newAppendFixture()
		fixture.seedTail(1, true)
		schedulerTime := time.Now().Add(-time.Hour)
		service.Now = func() time.Time { return schedulerTime }
		start := time.Now()
		deadline := start.Add(13 * time.Second)
		reads := 0
		changed := false
		claims := 0
		service.Scheduler = appendWaitScheduler{AppendScheduler: fixture, getTail: func(ctx context.Context, _ AppendChain) (StoredJob, bool, error) {
			reads++
			tail := fixture.tail
			if reads == 1 {
				if _, ok := ctx.Deadline(); ok {
					test.Error("initial lookup has a wait deadline")
				}
				time.Sleep(3 * time.Second)
			} else if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) || !time.Now().Before(deadline) {
				test.Errorf("poll at %s has deadline %s, want %s and a poll strictly before it", time.Now(), got, deadline)
			}
			holder := "first"
			if time.Since(start) >= 8*time.Second {
				changed = true
				holder = "second"
			}
			return liveAppendTail(tail, holder, schedulerTime.Add(time.Minute)), true, nil
		}, claim: func(context.Context, ClaimAppendRequest) (LeaseIdentity, error) {
			claims++
			return LeaseIdentity{}, jobdb.ErrConflict
		}}
		_, err := service.StartOrAppendJob(test.Context(), request)
		require.ErrorIs(test, err, jobdb.ErrAppendBusy)
		require.Equal(test, deadline, time.Now())
		require.True(test, changed)
		require.Zero(test, claims)
		require.Zero(test, fixture.creates)
		require.Len(test, fixture.logs[fixture.tail.JobKey], 1)
	})
}

func TestAppendLiveClaimWindowSurvivesOrdinaryRetry(test *testing.T) {
	synctest.Test(test, func(test *testing.T) {
		fixture, service, request := newAppendFixture()
		fixture.seedTail(1, true)
		start := time.Now()
		reads, claims := 0, 0
		service.Scheduler = appendWaitScheduler{AppendScheduler: fixture, getTail: func(ctx context.Context, _ AppendChain) (StoredJob, bool, error) {
			reads++
			if reads == 1 || claims == 1 && reads == 3 {
				if ctx != test.Context() {
					test.Error("ordinary lookup did not use the caller context")
				}
			} else if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(start.Add(10*time.Second)) {
				test.Errorf("wait deadline reset after ordinary retry: %s", deadline)
			}
			if reads == 2 {
				return fixture.tail, true, nil
			}
			return liveAppendTail(fixture.tail, fmt.Sprint(claims), start.Add(time.Minute)), true, nil
		}, claim: func(ctx context.Context, _ ClaimAppendRequest) (LeaseIdentity, error) {
			claims++
			require.Equal(test, test.Context(), ctx)
			time.Sleep(7 * time.Second)
			return LeaseIdentity{}, jobdb.ErrConflict
		}}
		_, err := service.StartOrAppendJob(test.Context(), request)
		require.ErrorIs(test, err, jobdb.ErrAppendBusy)
		require.Equal(test, 10*time.Second, time.Since(start))
		require.Equal(test, 1, claims)
		require.Zero(test, fixture.creates)
		require.Len(test, fixture.logs[fixture.tail.JobKey], 1)
	})
}

func TestAppendLiveClaimCancelsBetweenPolls(test *testing.T) {
	synctest.Test(test, func(test *testing.T) {
		fixture, service, request := newAppendFixture()
		fixture.seedTail(1, true)
		tail := liveAppendTail(fixture.tail, "held", time.Now().Add(time.Minute))
		reads := 0
		service.Scheduler = appendWaitScheduler{AppendScheduler: fixture, getTail: func(context.Context, AppendChain) (StoredJob, bool, error) {
			reads++
			return tail, true, nil
		}}
		ctx, cancel := context.WithCancel(test.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := service.StartOrAppendJob(ctx, request)
			result <- err
		}()
		time.Sleep(time.Millisecond)
		synctest.Wait()
		require.Equal(test, 2, reads)
		canceledAt := time.Now()
		cancel()
		require.ErrorIs(test, <-result, context.Canceled)
		require.Equal(test, canceledAt, time.Now())
		synctest.Wait()
		require.Equal(test, 2, reads)
		require.Zero(test, fixture.creates)
		require.Len(test, fixture.logs[fixture.tail.JobKey], 1)
	})
}

func TestAppendLiveClaimBlockedRecheck(test *testing.T) {
	for _, tc := range []struct {
		name        string
		elapsed     time.Duration
		want        error
		wantReadErr error
	}{
		{"wait cutoff", 10 * time.Second, jobdb.ErrAppendBusy, context.DeadlineExceeded},
		{"caller cancellation", 2 * time.Second, context.Canceled, context.Canceled},
		{"caller deadline", 3 * time.Second, context.DeadlineExceeded, context.DeadlineExceeded},
	} {
		test.Run(tc.name, func(test *testing.T) {
			synctest.Test(test, func(test *testing.T) {
				fixture, service, request := newAppendFixture()
				fixture.seedTail(1, true)
				start := time.Now()
				ctx, cancel := context.WithCancel(test.Context())
				defer cancel()
				deadline := start.Add(10 * time.Second)
				if tc.want == context.DeadlineExceeded {
					deadline = start.Add(tc.elapsed)
					var cancelDeadline context.CancelFunc
					ctx, cancelDeadline = context.WithDeadline(ctx, deadline)
					defer cancelDeadline()
				}
				rechecks := make(chan context.Context, 1)
				unblock := make(chan struct{})
				defer func() { close(unblock) }()
				var reads atomic.Int32
				service.Scheduler = appendWaitScheduler{AppendScheduler: fixture, getTail: func(readCtx context.Context, _ AppendChain) (StoredJob, bool, error) {
					switch reads.Add(1) {
					case 1:
						if readCtx != ctx {
							test.Error("initial lookup did not use the caller context")
						}
						return liveAppendTail(fixture.tail, "held", start.Add(time.Minute)), true, nil
					case 2:
						rechecks <- readCtx
						<-unblock
						return fixture.tail, true, nil
					default:
						return StoredJob{}, false, errors.New("unexpected extra recheck")
					}
				}}
				result := make(chan error, 1)
				go func() {
					_, err := service.StartOrAppendJob(ctx, request)
					result <- err
				}()
				readCtx := <-rechecks
				readDeadline, ok := readCtx.Deadline()
				require.True(test, ok)
				require.Equal(test, deadline, readDeadline)
				if tc.want == context.Canceled {
					time.Sleep(tc.elapsed - time.Since(start))
					cancel()
				}
				require.ErrorIs(test, <-result, tc.want)
				require.Equal(test, tc.elapsed, time.Since(start))
				require.ErrorIs(test, readCtx.Err(), tc.wantReadErr)
				unblock <- struct{}{}
				synctest.Wait()
				require.EqualValues(test, 2, reads.Load())
				require.Zero(test, fixture.creates)
				require.Len(test, fixture.logs[fixture.tail.JobKey], 1)
			})
		})
	}
}

func TestAppendLiveClaimRecheckResult(test *testing.T) {
	storageError := errors.New("storage backend failed")
	storageTimeout := fmt.Errorf("storage timeout: %w", context.DeadlineExceeded)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"released", nil},
		{"expired", nil},
		{"storage failure", storageError},
		{"storage timeout before cutoff", storageTimeout},
	} {
		test.Run(tc.name, func(test *testing.T) {
			synctest.Test(test, func(test *testing.T) {
				fixture, service, request := newAppendFixture()
				fixture.seedTail(1, true)
				start := time.Now()
				reads := 0
				service.Scheduler = appendWaitScheduler{AppendScheduler: fixture, getTail: func(context.Context, AppendChain) (StoredJob, bool, error) {
					reads++
					live := liveAppendTail(fixture.tail, "held", start.Add(time.Millisecond))
					if reads == 1 || tc.name == "expired" {
						return live, true, nil
					}
					return fixture.tail, true, tc.err
				}, claim: func(ctx context.Context, req ClaimAppendRequest) (LeaseIdentity, error) {
					require.Equal(test, test.Context(), ctx)
					return fixture.ClaimAppend(ctx, req)
				}}
				handle, err := service.StartOrAppendJob(test.Context(), request)
				require.Equal(test, 2, reads)
				require.Equal(test, time.Millisecond, time.Since(start))
				require.Zero(test, fixture.creates)
				if tc.err != nil {
					require.ErrorIs(test, err, tc.err)
					require.NotErrorIs(test, err, jobdb.ErrAppendBusy)
					require.Len(test, fixture.logs[fixture.tail.JobKey], 1)
					if tc.err == storageError {
						var classified *jobdb.ExecutionInfrastructureError
						require.ErrorAs(test, err, &classified)
						require.Equal(test, "get_append_tail", classified.Operation)
					}
					return
				}
				require.NoError(test, err)
				require.Equal(test, fixture.tail.JobKey, handle.JobKey)
				require.Len(test, fixture.logs[handle.JobKey], 2)
			})
		})
	}
}

func TestAppendLiveClaimKeepsOrdinaryAttemptBudget(test *testing.T) {
	for _, tc := range []struct {
		name string
		held time.Duration
	}{
		{"ordinary conflicts", 0},
		{"live polls then ordinary conflicts", 2 * time.Second},
	} {
		test.Run(tc.name, func(test *testing.T) {
			synctest.Test(test, func(test *testing.T) {
				fixture, service, request := newAppendFixture()
				fixture.seedTail(1, true)
				start := time.Now()
				claims, ordinaryReads, polls := 0, 0, 0
				service.Scheduler = appendWaitScheduler{AppendScheduler: fixture, getTail: func(ctx context.Context, _ AppendChain) (StoredJob, bool, error) {
					if ctx == test.Context() {
						ordinaryReads++
					} else {
						polls++
					}
					if time.Since(start) < tc.held {
						return liveAppendTail(fixture.tail, "held", start.Add(time.Minute)), true, nil
					}
					return fixture.tail, true, nil
				}, claim: func(ctx context.Context, _ ClaimAppendRequest) (LeaseIdentity, error) {
					require.Equal(test, test.Context(), ctx)
					claims++
					return LeaseIdentity{}, jobdb.ErrConflict
				}}
				_, err := service.StartOrAppendJob(test.Context(), request)
				require.ErrorIs(test, err, jobdb.ErrAppendBusy)
				require.Equal(test, 200, claims)
				require.Equal(test, 200, ordinaryReads)
				if tc.held > 0 {
					require.Greater(test, polls, 200)
					require.GreaterOrEqual(test, time.Since(start), tc.held+1100*time.Millisecond)
				} else {
					require.Zero(test, polls)
					require.Equal(test, 1100*time.Millisecond, time.Since(start))
				}
				require.Zero(test, fixture.creates)
				require.Len(test, fixture.logs[fixture.tail.JobKey], 1)
			})
		})
	}
}

func TestAppendLiveClaimCrashConcernIsImmediate(test *testing.T) {
	synctest.Test(test, func(test *testing.T) {
		fixture, service, request := newAppendFixture()
		fixture.seedTail(1, true)
		start := time.Now()
		tail := liveAppendTail(fixture.tail, "held", start.Add(time.Minute))
		tail.Status = jobdb.JobStatusCrashConcern
		reads := 0
		service.Scheduler = appendWaitScheduler{AppendScheduler: fixture, getTail: func(context.Context, AppendChain) (StoredJob, bool, error) {
			reads++
			return tail, true, nil
		}}
		_, err := service.StartOrAppendJob(test.Context(), request)
		require.ErrorIs(test, err, jobdb.ErrAppendBusy)
		require.Equal(test, start, time.Now())
		require.Equal(test, 1, reads)
		require.Zero(test, fixture.creates)
		require.Len(test, fixture.logs[fixture.tail.JobKey], 1)
	})
}
