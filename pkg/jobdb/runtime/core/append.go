package runtimecore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
	"github.com/colony-2/jobdb/pkg/jobdb/jobid"
	"github.com/segmentio/ksuid"
)

const AppendWorkerPrefix = "jobdb-append-"

func IsAppendWorker(worker string) bool { return strings.HasPrefix(worker, AppendWorkerPrefix) }

type AppendChain struct{ TenantID, CellID, JobType, AppendKey string }
type ClaimAppendRequest struct {
	JobKey   jobdb.JobKey
	WorkerID string
	Now      time.Time
	Duration time.Duration
}

// AppendScheduler contains the scheduler operations used by the shared algorithm.
// Resolution and publication must validate any lease proof in ctx.
type AppendScheduler interface {
	ResolveAppendCell(context.Context, jobdb.SubmitJob) (jobdb.ResolvedCell, error)
	GetAppendTail(context.Context, AppendChain) (StoredJob, bool, error)
	ClaimAppend(context.Context, ClaimAppendRequest) (LeaseIdentity, error)
	ReleaseAppend(context.Context, LeaseIdentity) error
	PrepareJob(context.Context, JobPreparation) (PreparedJob, error)
	CreateJob(context.Context, CreateJobRequest) (StoredJob, error)
}
type AppendChapterLog interface {
	Create(context.Context, ChapterLogKey, EncodedChapter) error
	Append(context.Context, ChapterLogKey, EncodedChapter) error
	Get(context.Context, ChapterLogKey, int64) (EncodedChapter, error)
	Count(context.Context, ChapterLogKey) (int64, error)
}

// AppendService shares the algorithm with the embedded runtimes.
type AppendService struct {
	Projects jobdb.ProjectAdmission

	Scheduler    AppendScheduler
	Chapters     AppendChapterLog
	Schemas      jobdb.JobSchemaRegistry
	PayloadLimit int64
	Now          func() time.Time
}

func (r *Runtime) StartOrAppendJob(ctx context.Context, req jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	return (AppendService{Projects: r.projects, Scheduler: r.scheduler, Chapters: r.chapters, Schemas: r.chapterRegistry(), PayloadLimit: r.ChapterApplicationPayloadLimit(), Now: r.now}).StartOrAppendJob(ctx, req)
}
func (s AppendService) StartOrAppendJob(ctx context.Context, req jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	in := req.Job
	bad := func(message string) (jobdb.JobHandle, error) {
		return jobdb.JobHandle{}, fmt.Errorf("%w: %s", jobdb.ErrInvalidAppendRequest, message)
	}
	if strings.TrimSpace(in.AppendKey) == "" || !utf8.ValidString(in.AppendKey) || len(in.AppendKey) > 1024 || strings.ContainsRune(in.AppendKey, 0) {
		return bad("appendKey must be 1–1024 UTF-8 bytes")
	}
	if in.MaxInputs < 0 || in.MaxInputs > jobdb.MaxAppendInputs {
		return bad("maxInputs is outside the server limit")
	}
	if in.MaxInputs == 0 {
		in.MaxInputs = jobdb.DefaultMaxAppendInputs
	}
	if in.Job.JobID != "" || in.Job.GoalID != "" || len(in.Job.Dependencies) > 0 || len(in.Job.Prerequisites) > 0 {
		return bad("jobId, goalId, dependencies and prerequisites are server assigned")
	}
	if !json.Valid(in.Append) {
		return bad("append must be a JSON value")
	}
	if err := jobdb.ValidateIdentifier(in.Job.JobType); err != nil {
		return bad(err.Error())
	}
	if in.Job.TenantId == "" {
		return bad("tenantId is required")
	}
	names := map[string]bool{}
	for _, ref := range in.AppendArtifacts {
		if err := ref.Validate(); err != nil {
			return bad(err.Error())
		}
		if !validAppendArtifactName(ref.Name) || names[ref.Name] {
			return bad("append artifact names must be unique relative paths")
		}
		names[ref.Name] = true
	}
	if projectErr := jobdb.RequireProjectWrite(ctx, s.Projects, req.Job.Job.TenantId); projectErr != nil {
		var projectZero0 jobdb.JobHandle
		return projectZero0, projectErr
	}
	selector := in.Job
	selector.CellRepo = ""
	cell, err := s.Scheduler.ResolveAppendCell(ctx, selector)
	err = jobdb.ClassifyRuntimeIO(err, "resolve_append_cell", nil, nil)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	chain := AppendChain{in.Job.TenantId, cell.CellID, in.Job.JobType, in.AppendKey}
	now := nowFunc(s.Now)
	worker := AppendWorkerPrefix + ksuid.New().String()
	// A bounded, context-aware wait avoids turning an appender burst into a chain.
	for attempt := 0; attempt < 200; attempt++ {
		if err := ctx.Err(); err != nil {
			return jobdb.JobHandle{}, err
		}
		tail, found, err := s.Scheduler.GetAppendTail(ctx, chain)
		err = jobdb.ClassifyRuntimeIO(err, "get_append_tail", nil, nil)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		if found && tail.Status == jobdb.JobStatusCrashConcern {
			return jobdb.JobHandle{}, jobdb.ErrAppendBusy
		}
		if found && tail.ArchivedAt == nil && !tail.CancelRequested {
			live := tail.LeaseExpiresAt != nil && tail.LeaseExpiresAt.After(now())
			if live && IsAppendWorker(tail.LeaseWorkerID) {
				if err := appendBackoff(ctx, attempt); err != nil {
					return jobdb.JobHandle{}, err
				}
				continue
			}
			if !live {
				count, open, err := s.appendable(ctx, tail.JobKey, in.MaxInputs)
				if err != nil {
					return jobdb.JobHandle{}, err
				}
				if open {
					claim, err := s.Scheduler.ClaimAppend(ctx, ClaimAppendRequest{tail.JobKey, worker, now(), 10 * time.Second})
					err = jobdb.ClassifyRuntimeIO(err, "claim_append", &tail.JobKey, nil)
					if errors.Is(err, jobdb.ErrConflict) {
						if err := appendBackoff(ctx, attempt); err != nil {
							return jobdb.JobHandle{}, err
						}
						continue
					}
					if err != nil {
						return jobdb.JobHandle{}, err
					}
					// Recheck after the CAS: the precheck may have raced another appender or worker.
					count, open, err = s.appendable(ctx, tail.JobKey, in.MaxInputs)
					if err == nil && open {
						ch := jobdb.Chapter{Ordinal: count, TaskType: tail.JobType, CreatedAt: now(), Body: jobdb.AdditionalInputChapter{Input: jobdb.ApplicationInputBytes{Data: in.Append}, Artifacts: in.AppendArtifacts}}
						err = jobdb.ValidateChapterApplicationPayload(ctx, ch, s.PayloadLimit)
						if err == nil {
							err = ValidateOrdinaryChapter(ctx, s.Schemas, jobdb.JobSchemaKey{TenantId: in.Job.TenantId, SchemaHash: tail.SchemaHash}, ch)
						}
						if err == nil {
							var encoded EncodedChapter
							encoded, err = EncodeChapter(EncodeChapterRequest{Chapter: ch})
							if err == nil {
								err = s.Chapters.Append(ctx, ChapterLogKey{tail.JobKey}, encoded)
								err = jobdb.ClassifyRuntimeIO(err, "append_input", &tail.JobKey, &count)
							}
						}
					}
					releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
					releaseErr := s.Scheduler.ReleaseAppend(releaseCtx, claim)
					releaseErr = jobdb.ClassifyRuntimeIO(releaseErr, "release_append", &tail.JobKey, nil)
					cancel()
					if releaseErr != nil {
						return jobdb.JobHandle{}, errors.Join(err, releaseErr)
					}
					if err == nil && open {
						return tail.Cell.Handle(tail.JobKey, tail.GoalID), nil
					}
					if err != nil && !errors.Is(err, jobdb.ErrConflict) {
						return jobdb.JobHandle{}, err
					}
					continue
				}
			}
		}
		generation := int64(1)
		if found {
			if tail.AppendGeneration >= math.MaxUint32 {
				return bad("append generation exhausted (maximum 4294967295)")
			}
			generation = tail.AppendGeneration + 1
		}
		id, err := compactAppendJobID(chain, generation)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		key := jobdb.JobKey{TenantId: chain.TenantID, JobId: jobid.Prefix + id}
		savedKey, chapter, err := s.findAppendStart(ctx, chain.TenantID, legacyAppendJobID(chain, generation), id, key.JobId)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		if savedKey.JobId != "" {
			if _, _, err = s.publish(ctx, savedKey, chain, generation, chapter, false); err != nil {
				return jobdb.JobHandle{}, err
			}
			continue // Retry this caller's append against the recovered winner.
		}
		handle, created, err := s.create(ctx, req, in, cell, key, tail, found, generation, now())
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		if created {
			return handle, nil
		}
	}
	return jobdb.JobHandle{}, jobdb.ErrAppendBusy
}

func (service AppendService) findAppendStart(ctx context.Context, tenant string, ids ...string) (jobdb.JobKey, EncodedChapter, error) {
	var savedKey jobdb.JobKey
	var savedChapter EncodedChapter
	for _, id := range ids {
		key := jobdb.JobKey{TenantId: tenant, JobId: id}
		chapter, err := service.Chapters.Get(ctx, ChapterLogKey{key}, 0)
		if errors.Is(err, jobdb.ErrJobNotFound) || errors.Is(err, jobdb.ErrChapterNotFound) {
			continue
		}
		if err != nil {
			return jobdb.JobKey{}, EncodedChapter{}, jobdb.ClassifyRuntimeIO(err, "get_append_chapter", &key, nil)
		}
		if savedKey.JobId != "" {
			return jobdb.JobKey{}, EncodedChapter{}, fmt.Errorf("%w: competing append starts %q and %q", jobdb.ErrConflict, savedKey.JobId, key.JobId)
		}
		savedKey, savedChapter = key, chapter
	}
	return savedKey, savedChapter, nil
}

func appendIdentityHash(chain AppendChain) [32]byte {
	return sha256.Sum256([]byte(fmt.Sprintf("%q/%q/%q", chain.CellID, chain.JobType, chain.AppendKey)))
}

func legacyAppendJobID(chain AppendChain, generation int64) string {
	sum := appendIdentityHash(chain)
	return fmt.Sprintf("ak-%x-%019d", sum[:16], generation)
}

func compactAppendJobID(chain AppendChain, generation int64) (string, error) {
	if generation < 1 || generation > math.MaxUint32 {
		return "", fmt.Errorf("%w: append generation outside range 1–4294967295", jobdb.ErrInvalidAppendRequest)
	}
	sum := appendIdentityHash(chain)
	var raw [20]byte
	copy(raw[:16], sum[:16])
	binary.BigEndian.PutUint32(raw[16:], uint32(generation))
	id, err := ksuid.FromBytes(raw[:])
	return id.String(), err
}

func appendBackoff(ctx context.Context, attempt int) error {
	delay := time.Duration(1+attempt%10) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (s AppendService) appendable(ctx context.Context, key jobdb.JobKey, max int) (int64, bool, error) {
	n, err := s.Chapters.Count(ctx, ChapterLogKey{key})
	err = jobdb.ClassifyRuntimeIO(err, "count_append_chapters", &key, nil)
	if err != nil {
		return 0, false, err
	}
	if n < 1 || n >= int64(max) {
		return n, false, nil
	}
	encoded, err := s.Chapters.Get(ctx, ChapterLogKey{key}, n-1)
	ordinal := n - 1
	err = jobdb.ClassifyRuntimeIO(err, "get_append_chapter", &key, &ordinal)
	if err != nil {
		return 0, false, err
	}
	ch, err := DecodeChapter(encoded)
	err = jobdb.ClassifyExecutionInfrastructure(err, jobdb.ExecutionInfrastructureDurableDecode, "decode_append_chapter", &key, &encoded.Ordinal)
	if err != nil {
		return 0, false, err
	}
	switch b := ch.Body.(type) {
	case jobdb.JobStartChapter:
		return n, b.Append != nil, nil
	case jobdb.AdditionalInputChapter:
		return n, true, nil
	}
	return n, false, nil
}
func (s AppendService) create(ctx context.Context, req jobdb.StartOrAppendJobRequest, in jobdb.StartOrAppendJob, cell jobdb.ResolvedCell, key jobdb.JobKey, tail StoredJob, found bool, generation int64, now time.Time) (jobdb.JobHandle, bool, error) {
	chain := AppendChain{TenantID: key.TenantId, CellID: cell.CellID, JobType: in.Job.JobType, AppendKey: in.AppendKey}
	// Read before preparing the template. A loser must recover the winner's facts,
	// including its schema, availability, initial client payload and artifacts.
	existing, err := s.Chapters.Get(ctx, ChapterLogKey{key}, 0)
	err = jobdb.ClassifyRuntimeIO(err, "get_append_chapter", &key, nil)
	if err == nil {
		return s.publish(ctx, key, chain, generation, existing, false)
	}
	if !errors.Is(err, jobdb.ErrJobNotFound) && !errors.Is(err, jobdb.ErrChapterNotFound) {
		return jobdb.JobHandle{}, false, err
	}
	if in.Job.CellRepo != "" {
		repo, e := jobdb.NormalizeCellRepo(in.Job.CellRepo)
		if e != nil {
			return jobdb.JobHandle{}, false, e
		}
		if repo != cell.CellRepo {
			return jobdb.JobHandle{}, false, jobdb.ErrCellRepoMismatch
		}
	}
	if !req.RequestTime.IsZero() {
		now = req.RequestTime.UTC()
	}
	payload, artifacts, uploads, sources, hash, err := prepareInitialTaskData(ctx, in.Job.Data)
	if err != nil {
		return jobdb.JobHandle{}, false, fmt.Errorf("%w: %v", jobdb.ErrInvalidAppendRequest, err)
	}
	names := map[string]bool{}
	for _, artifact := range artifacts {
		if !validAppendArtifactName(artifact.Name) || names[artifact.Name] {
			return jobdb.JobHandle{}, false, fmt.Errorf("%w: start artifact names must be unique relative paths", jobdb.ErrInvalidAppendRequest)
		}
		names[artifact.Name] = true
	}
	if err = jobdb.ValidateApplicationMetadata(in.Job.Metadata); err != nil {
		return jobdb.JobHandle{}, false, fmt.Errorf("%w: %v", jobdb.ErrInvalidAppendRequest, err)
	}
	schema, err := ResolveActiveSchemaForNewJob(ctx, s.Schemas, key.TenantId, in.Job.Schema)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	initial, revision, err := clientpayload.Initial(in.Job.ClientPayloadUpdate)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	digest, err := clientpayload.Digest(initial)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	facts := CreateJobRequest{Cell: cell, JobKey: key, JobType: in.Job.JobType, RunPolicy: normalizeCoreRunPolicy(in.Job.RunPolicy), AppMetadata: in.Job.Metadata, SchemaHash: schema, AvailableAt: in.Job.AvailableAt, CreatedAt: now, WorkerID: req.WorkerID, ClientPayload: initial, ClientPayloadRevision: revision, InitialPayloadDigest: digest, AppendKey: in.AppendKey, AppendGeneration: generation}
	if len(facts.AppMetadata) == 0 {
		facts.AppMetadata = json.RawMessage(`{}`)
	}
	if facts.WorkerID == "" {
		facts.WorkerID = AppendWorkerPrefix + ksuid.New().String()
	}
	var prereqs []jobdb.JobPrerequisite
	if found {
		facts.WaitForJobIDs = []string{tail.JobKey.JobId}
		prereqs = []jobdb.JobPrerequisite{{JobID: tail.JobKey.JobId, Condition: jobdb.JobPrereqComplete}}
	}
	publication, err := json.Marshal(facts)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	meta := runtimecodec.ChapterMeta{Version: runtimecodec.EnvelopeVersion, Ordinal: 0, TaskType: in.Job.JobType, CreatedAt: now, InputHash: hash, RunPolicy: &facts.RunPolicy, Prerequisites: prereqs, AppendPublication: publication}
	raw, err := json.Marshal(meta)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	metadata, err := runtimecodec.ChapterMetadataFromJSON(raw)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	ch := jobdb.Chapter{Ordinal: 0, TaskType: in.Job.JobType, CreatedAt: now, InputHash: hash, Metadata: metadata, Artifacts: artifacts, Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: payload}, Append: &jobdb.ApplicationInputBytes{Data: in.Append}, AppendArtifacts: in.AppendArtifacts}}
	if err = jobdb.ValidateChapterApplicationPayload(ctx, ch, s.PayloadLimit); err != nil {
		return jobdb.JobHandle{}, false, err
	}
	if err = ValidateFirstChapter(ctx, s.Schemas, jobdb.JobSchemaKey{TenantId: key.TenantId, SchemaHash: schema}, ch); err != nil {
		return jobdb.JobHandle{}, false, err
	}
	encoded, err := EncodeChapter(EncodeChapterRequest{Chapter: ch, ArtifactUploads: uploads})
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	accept := func() (jobdb.JobHandle, bool, error) {
		for _, artifact := range sources {
			jobdb.AssignArtifactKey(artifact, jobdb.ArtifactKey{JobId: key.JobId, TaskOrdinal: 0, Name: artifact.Name(), SizeBytes: artifact.Size()})
			_ = artifact.Cleanup()
		}
		return s.publish(ctx, key, chain, generation, encoded, true)
	}
	if err = s.Chapters.Create(ctx, ChapterLogKey{key}, encoded); err != nil {
		if !errors.Is(err, jobdb.ErrConflict) {
			return jobdb.JobHandle{}, false, jobdb.ClassifyRuntimeIO(err, "create_append_job", &key, nil)
		}
		existing, err = s.Chapters.Get(ctx, ChapterLogKey{key}, 0)
		err = jobdb.ClassifyRuntimeIO(err, "get_append_chapter", &key, nil)
		if errors.Is(err, jobdb.ErrChapterNotFound) || errors.Is(err, jobdb.ErrJobNotFound) {
			// Some logs allocate their container before chapter zero. Recover an
			// empty container by competing on ordinal zero, the actual arbiter.
			err = s.Chapters.Append(ctx, ChapterLogKey{key}, encoded)
			err = jobdb.ClassifyRuntimeIO(err, "create_append_job", &key, nil)
			if err == nil {
				return accept()
			}
			if errors.Is(err, jobdb.ErrConflict) {
				existing, err = s.Chapters.Get(ctx, ChapterLogKey{key}, 0)
				err = jobdb.ClassifyRuntimeIO(err, "get_append_chapter", &key, nil)
			}
		}
		if err != nil {
			return jobdb.JobHandle{}, false, err
		}
		return s.publish(ctx, key, chain, generation, existing, false)
	}
	return accept()
}
func (s AppendService) publish(ctx context.Context, key jobdb.JobKey, chain AppendChain, generation int64, encoded EncodedChapter, created bool) (jobdb.JobHandle, bool, error) {
	ch, err := DecodeChapter(encoded)
	err = jobdb.ClassifyExecutionInfrastructure(err, jobdb.ExecutionInfrastructureDurableDecode, "decode_append_chapter", &key, &encoded.Ordinal)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	meta, err := initialChapterMetadata(ch.Metadata)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	var facts CreateJobRequest
	if err = json.Unmarshal(meta.AppendPublication, &facts); err != nil {
		return jobdb.JobHandle{}, false, fmt.Errorf("append publication: %w", err)
	}
	start, ok := ch.Body.(jobdb.JobStartChapter)
	if !ok || start.Append == nil || ch.Ordinal != 0 || ch.TaskType != chain.JobType ||
		facts.JobKey != key || facts.JobKey.TenantId != chain.TenantID || facts.Cell.CellID != chain.CellID ||
		facts.JobType != chain.JobType || facts.AppendKey != chain.AppendKey || facts.AppendGeneration != generation {
		return jobdb.JobHandle{}, false, fmt.Errorf("%w: append start does not match generation %d at %q", jobdb.ErrConflict, generation, key.JobId)
	}
	// A stable descriptor intentionally excludes the caller's template and parent.
	prepared, err := s.Scheduler.PrepareJob(ctx, JobPreparation{Key: key, Cell: facts.Cell.Cell, CellRepo: facts.Cell.CellRepo, Target: facts.Cell, Descriptor: fmt.Sprintf(`{"kind":"start-or-append","id":%q}`, key.JobId)})
	err = jobdb.ClassifyRuntimeIO(err, "prepare_append_job", &key, nil)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	facts.GoalID = prepared.GoalID
	stored, err := s.Scheduler.CreateJob(ctx, facts)
	err = jobdb.ClassifyRuntimeIO(err, "publish_append_job", &key, nil)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	return stored.Cell.Handle(key, stored.GoalID), created, nil
}

func (l *executionLease) StartOrAppendJob(ctx context.Context, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	if err := l.requireCurrent(ctx); err != nil {
		return jobdb.JobHandle{}, err
	}
	q.Job.Job.TenantId = l.snapshot.Identity.JobKey.TenantId
	return l.runtime.StartOrAppendJob(l.cellContext(ctx), q)
}
func (r *Runtime) StartOrAppendJobWithLeaseByID(ctx context.Context, key jobdb.JobKey, lease, worker string, q jobdb.StartOrAppendJobRequest) (jobdb.JobHandle, error) {
	l, err := r.leaseForTransport(ctx, key, lease, worker)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	return l.StartOrAppendJob(ctx, q)
}

func validAppendArtifactName(name string) bool {
	return name != "" && name != "." && name != ".." && utf8.ValidString(name) && !strings.ContainsRune(name, 0) && path.Clean(name) == name && !strings.HasPrefix(name, "/") && !strings.HasPrefix(name, "../") && !strings.Contains(name, "\\")
}
