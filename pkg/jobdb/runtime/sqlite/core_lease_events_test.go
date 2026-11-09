package sqlite

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadata"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtimetest"
	"github.com/stretchr/testify/require"
)

// This test-only composition runs the real core runtime over SQLite's atomic
// scheduler mutations and chapter/schema/configuration stores. In particular,
// the leases under test are core handles, not SQLite handles or mock snapshots.
func TestLeaseEventCoreConformance(t *testing.T) {
	for _, overHTTP := range []bool{false, true} {
		name := "core-sqlite"
		if overHTTP {
			name = "remote-" + name
		}
		runtimetest.RunLeaseEventConformance(t, runtimetest.Harness{Name: name, New: func(tb testing.TB) runtimetest.Fixture {
			ctx := celltest.Context(context.Background())
			storage, err := NewFromConfig(ctx, Config{DBPath: filepath.Join(tb.TempDir(), "core.db")})
			require.NoError(tb, err)
			tb.Cleanup(func() { require.NoError(tb, storage.Close(ctx)) })
			core, err := runtimecore.NewRuntime(runtimecore.Config{
				Scheduler: &coreEventScheduler{r: storage, CellRuntime: storage, GoalScheduler: storage}, Chapters: coreEventChapters{r: storage},
				Schemas: sqliteSchemaStore{runtime: storage}, Configurations: storage.Service.Store,
			})
			require.NoError(tb, err)
			var runtime jobdb.WorkflowRuntime = core
			if overHTTP {
				server := httptest.NewServer(celltest.Handler(remote.NewServer(core)))
				tb.Cleanup(server.Close)
				runtime, err = remote.New(server.URL, server.Client())
				require.NoError(tb, err)
			}
			fixture := runtimetest.Fixture{Runtime: runtime}
			if overHTTP {
				fixture.LeaseEventLeaseValidity = core.ValidateExecutionLeaseByID
			}
			return fixture
		}})
	}
}

// Unused ports deliberately have no implementation: an unexpected call fails
// the fixture instead of silently simulating successful persistence.
type unusedCoreEventScheduler struct{ runtimecore.Scheduler }
type coreEventScheduler struct {
	r *Runtime
	jobdb.CellRuntime
	runtimecore.GoalScheduler
	unusedCoreEventScheduler
}

func (s *coreEventScheduler) CreateJob(ctx context.Context, req runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	metadata, err := jobdb.BuildJobMetadataEnvelope(req.AppMetadata, jobdb.RuntimeJobMetadata{SchemaHash: req.SchemaHash, ParentJobID: req.ParentJobID})
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	// The conformance submissions have no client payload or dependencies. Reject
	// unsupported fixture inputs explicitly rather than losing their semantics.
	if req.ClientPayloadRevision != 0 || len(req.Dependencies) != 0 || req.Schedule != nil || req.ExpiresAt != nil {
		return runtimecore.StoredJob{}, fmt.Errorf("unsupported core event fixture submission")
	}
	err = s.r.ensureSubmittedJobRecord(ctx, req.JobKey, req.JobType, metadata, req.WaitForJobIDs, jobPayload{RunPolicy: req.RunPolicy}, req.WorkerID, req.AvailableAt, nil)
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	return s.GetJob(ctx, req.JobKey)
}
func (s *coreEventScheduler) GetJob(ctx context.Context, key jobdb.JobKey) (runtimecore.StoredJob, error) {
	row, err := s.r.loadJobRow(ctx, key)
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	payload, err := decodeJobPayload(row.payload)
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	out := runtimecore.StoredJob{JobKey: key, Cell: row.cell, GoalID: row.goalID, JobType: row.jobType,
		RouteJobType: row.nextRoute.JobType, WorkKind: runtimecore.WorkKindJob, RunPolicy: payload.RunPolicy,
		ClientPayload: row.clientPayload, ClientPayloadRevision: row.clientPayloadRevision,
		AppMetadata: jobdb.AppMetadataFromStoredMetadata(row.metadata), ParentJobID: row.parentJobID.String,
		InitialPayloadDigest: row.initialPayloadDigest, SchemaHash: jobmetadata.SchemaHashFromStoredMetadata(row.metadata),
		CancelRequested: row.cancelRequested, Store: jobdb.JobStoreActive, CreatedAt: timeFromNS(row.createdAtNS),
		UpdatedAt: timeFromNS(row.updatedAtNS), AvailableAt: timeFromNS(row.availableAtNS),
		LeaseExpiresAt: nullTimeFromNS(row.leaseExpiresAtNS), LeaseWorkerID: row.leaseWorkerID.String,
	}
	if row.nextRoute.TaskType != "" {
		out.WorkKind = runtimecore.WorkKindTask
		task := payload.TaskWait
		if task == nil {
			return out, fmt.Errorf("missing task wait")
		}
		out.TaskWork = &runtimecore.TaskWork{TaskType: row.nextRoute.TaskType, ResumeJobType: task.Next, InputOrdinal: task.InputStep, OutputOrdinal: task.OutputStep, InputHash: task.InputHash}
	}
	if row.leaseID.Valid {
		out.Lease = &runtimecore.LeaseIdentity{JobKey: key, LeaseID: row.leaseID.String, WorkerID: row.leaseWorkerID.String, ExpiresAt: timeFromNS(row.leaseExpiresAtNS.Int64)}
	}
	if row.archivedAtNS.Valid {
		out.Store = jobdb.JobStoreArchived
		out.ArchivedAt = nullTimeFromNS(row.archivedAtNS)
	}
	return out, nil
}
func (s *coreEventScheduler) snapshot(ctx context.Context, key jobdb.JobKey, duration time.Duration) (runtimecore.LeaseSnapshot, error) {
	row, err := s.GetJob(ctx, key)
	if err != nil {
		return runtimecore.LeaseSnapshot{}, err
	}
	if row.Lease == nil {
		return runtimecore.LeaseSnapshot{}, jobdb.ErrExecutionLeaseLost
	}
	return runtimecore.LeaseSnapshot{Identity: *row.Lease, Cell: row.Cell, GoalID: row.GoalID, JobType: row.JobType,
		RouteJobType: row.RouteJobType, WorkKind: row.WorkKind, TaskWork: row.TaskWork, RunPolicy: row.RunPolicy,
		ClientPayload: row.ClientPayload, ClientPayloadRevision: row.ClientPayloadRevision, SchemaHash: row.SchemaHash,
		InitialPayloadDigest: row.InitialPayloadDigest, Duration: leaseDurationOrDefault(duration)}, nil
}
func coreEventRoutes(selector runtimecore.WorkSelector) []jobdb.Route {
	routes := []jobdb.Route{}
	for _, kind := range selector.JobTypes {
		routes = append(routes, jobdb.Route{JobType: kind})
	}
	for _, task := range selector.Tasks {
		routes = append(routes, jobdb.Route{JobType: task.JobType, TaskType: task.TaskType})
	}
	return routes
}
func (s *coreEventScheduler) AcquireJobLease(ctx context.Context, req runtimecore.JobLeaseRequest) (*runtimecore.LeaseSnapshot, error) {
	lease, err := s.r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: req.JobKey, WorkerID: req.WorkerID, Routes: coreEventRoutes(req.Selector), LeaseDuration: req.LeaseDuration})
	if err != nil || lease == nil {
		return nil, err
	}
	snapshot, err := s.snapshot(ctx, req.JobKey, req.LeaseDuration)
	return &snapshot, err
}
func (s *coreEventScheduler) AcquireWork(ctx context.Context, req runtimecore.WorkRequest) ([]runtimecore.LeaseSnapshot, error) {
	leases, err := s.r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: req.TenantId, WorkerID: req.WorkerID, Routes: coreEventRoutes(req.Selector), LeaseDuration: req.LeaseDuration, Limit: req.Limit, CellNames: req.CellNames})
	if err != nil {
		return nil, err
	}
	snapshots := make([]runtimecore.LeaseSnapshot, 0, len(leases))
	for _, lease := range leases {
		snapshot, err := s.snapshot(ctx, lease.Job().JobKey, req.LeaseDuration)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}
func (s *coreEventScheduler) ValidateLease(ctx context.Context, identity runtimecore.LeaseIdentity) (runtimecore.LeaseSnapshot, error) {
	if _, err := s.r.validateLease(ctx, identity.JobKey, identity.LeaseID, identity.WorkerID); err != nil {
		return runtimecore.LeaseSnapshot{}, err
	}
	return s.snapshot(ctx, identity.JobKey, 0)
}
func (s *coreEventScheduler) KeepAliveLease(ctx context.Context, req runtimecore.LeaseMutation) (runtimecore.LeaseSnapshot, error) {
	_, err := s.r.RenewExecutionLeaseByID(ctx, req.Identity.JobKey, req.Identity.LeaseID, req.Identity.WorkerID, req.Duration)
	if err != nil {
		return runtimecore.LeaseSnapshot{}, err
	}
	return s.snapshot(ctx, req.Identity.JobKey, req.Duration)
}
func (s *coreEventScheduler) CancelJob(ctx context.Context, req runtimecore.CancelJobMutation) (runtimecore.StoredJob, error) {
	err := s.r.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: req.JobKey, WorkerID: req.WorkerID, Reason: req.Reason})
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	return s.GetJob(ctx, req.JobKey)
}
func (s *coreEventScheduler) RescheduleLease(ctx context.Context, req runtimecore.RescheduleMutation) (runtimecore.StoredJob, error) {
	next := jobdb.RescheduleExecutionRequest{NextRoute: jobdb.Route{JobType: req.RouteJobType}, WaitUntil: req.WaitUntil, WaitForJobIDs: req.WaitForJobIDs, ClientPayloadUpdate: req.ClientPayloadUpdate, DependencyWait: req.DependencyWait}
	if req.TaskWork != nil {
		task := req.TaskWork
		next.NextRoute.TaskType = task.TaskType
		next.TaskWait = &jobdb.TaskWait{ResumeJobType: task.ResumeJobType, InputOrdinal: task.InputOrdinal, OutputOrdinal: task.OutputOrdinal, InputHash: task.InputHash}
	}
	if req.AlternateRoute != nil {
		route := req.AlternateRoute
		next.AlternateRoute = &jobdb.Route{JobType: route.JobType, TaskType: route.TaskType}
		next.AlternateAfter = &route.After
	}
	err := s.r.RescheduleJobWithLeaseByID(ctx, req.Identity.JobKey, req.Identity.LeaseID, req.Identity.WorkerID, next)
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	return s.GetJob(ctx, req.Identity.JobKey)
}
func (s *coreEventScheduler) GetWaitingTask(ctx context.Context, key jobdb.JobKey) (runtimecore.WaitingTaskSnapshot, error) {
	row, err := s.GetJob(ctx, key)
	if err != nil {
		return runtimecore.WaitingTaskSnapshot{}, err
	}
	if row.TaskWork == nil {
		return runtimecore.WaitingTaskSnapshot{}, jobdb.ErrConflict
	}
	return runtimecore.WaitingTaskSnapshot{JobType: row.RouteJobType, Task: *row.TaskWork}, nil
}
func (s *coreEventScheduler) ClaimTask(ctx context.Context, req runtimecore.ClaimTaskRequest) (runtimecore.LeaseIdentity, error) {
	row, err := s.r.loadJobRow(ctx, req.JobKey)
	if err != nil {
		return runtimecore.LeaseIdentity{}, err
	}
	payload, err := decodeJobPayload(row.payload)
	if err != nil {
		return runtimecore.LeaseIdentity{}, err
	}
	// claimWaitingTask uses the runtime's fallback worker ID. Give the claim a
	// request-local storage facade so concurrent fixtures never mutate shared state.
	local := Runtime{db: s.r.db, workerID: req.WorkerID}
	lease, err := local.claimWaitingTask(ctx, row, req.Task.Task.ResumeJobType, payload.RunPolicy)
	if err != nil {
		return runtimecore.LeaseIdentity{}, err
	}
	return runtimecore.LeaseIdentity{JobKey: req.JobKey, LeaseID: lease.LeaseID(), WorkerID: req.WorkerID, ExpiresAt: lease.LeaseExpiry()}, nil
}
func (s *coreEventScheduler) CompleteLease(ctx context.Context, req runtimecore.CompletionMutation) (runtimecore.StoredJob, error) {
	count, err := s.r.storyChapterCount(ctx, req.Identity.JobKey)
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	chapter, err := s.r.GetChapter(ctx, jobdb.ChapterRef{JobKey: req.Identity.JobKey, Ordinal: count - 1})
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	err = s.r.CompleteJobWithLeaseByID(ctx, req.Identity.JobKey, req.Identity.LeaseID, req.Identity.WorkerID, jobdb.CompleteExecutionRequest{Status: req.Status, Detail: req.Detail, Chapter: &chapter, ClientPayloadUpdate: req.ClientPayloadUpdate})
	if err != nil {
		return runtimecore.StoredJob{}, err
	}
	return s.GetJob(ctx, req.Identity.JobKey)
}

// Chapter bytes use the same runtime codec; the real SQLite chapter store
// enforces ordinal insertion and keeps core completion/task transitions durable.
type coreEventChapters struct {
	runtimecore.ChapterLog
	r *Runtime
}

func (c coreEventChapters) Create(ctx context.Context, key runtimecore.ChapterLogKey, ch runtimecore.EncodedChapter) error {
	if len(ch.ArtifactUploads) != 0 {
		return fmt.Errorf("artifact uploads outside core event fixture scope")
	}
	_, err := c.r.chapterStore.CreateStory(chapterContext(ctx), storyKeyForJob(key.JobKey), story.CreateOptions{InitialChapter: story.NewChapter().WithOrdinal(ch.Ordinal).WithBytes(ch.Payload)})
	return err
}
func (c coreEventChapters) Append(ctx context.Context, key runtimecore.ChapterLogKey, ch runtimecore.EncodedChapter) error {
	if len(ch.ArtifactUploads) != 0 {
		return fmt.Errorf("artifact uploads outside core event fixture scope")
	}
	return c.r.chapterStore.SaveChapter(chapterContext(ctx), storyKeyForJob(key.JobKey), story.NewChapter().WithOrdinal(ch.Ordinal).WithBytes(ch.Payload))
}
func (c coreEventChapters) Count(ctx context.Context, key runtimecore.ChapterLogKey) (int64, error) {
	return c.r.storyChapterCount(ctx, key.JobKey)
}
func (c coreEventChapters) Get(ctx context.Context, key runtimecore.ChapterLogKey, ordinal int64) (runtimecore.EncodedChapter, error) {
	chapter, err := c.r.chapterStore.Chapter(chapterContext(ctx), storyKeyForJob(key.JobKey), ordinal)
	if err != nil {
		return runtimecore.EncodedChapter{}, err
	}
	return runtimecore.EncodedChapter{Ordinal: ordinal, Payload: chapter.Body()}, nil
}
func (c coreEventChapters) List(ctx context.Context, key runtimecore.ChapterLogKey, rng runtimecore.ChapterRange) ([]runtimecore.EncodedChapter, error) {
	count, err := c.Count(ctx, key)
	if err != nil {
		return nil, err
	}
	if rng.EndOrdinal != nil && *rng.EndOrdinal < count-1 {
		count = *rng.EndOrdinal + 1
	}
	out := []runtimecore.EncodedChapter{}
	for ordinal := rng.StartOrdinal; ordinal < count; ordinal++ {
		ch, err := c.Get(ctx, key, ordinal)
		if err != nil {
			return nil, err
		}
		out = append(out, ch)
	}
	return out, nil
}

func (s *coreEventScheduler) ListLeaseEvents(ctx context.Context, req jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	return s.r.ListLeaseEvents(ctx, req)
}
