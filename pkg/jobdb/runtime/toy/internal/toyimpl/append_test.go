package toyimpl

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

type appendPublicationFault struct {
	appendScheduler
	fail bool
}

func (s *appendPublicationFault) CreateJob(ctx context.Context, q runtimecore.CreateJobRequest) (runtimecore.StoredJob, error) {
	if s.fail {
		s.fail = false
		return runtimecore.StoredJob{}, errors.New("publication interrupted")
	}
	return s.appendScheduler.CreateJob(ctx, q)
}
func appendFixture(t *testing.T) (*Runtime, context.Context, jobdb.StartOrAppendJobRequest) {
	t.Helper()
	r := New()
	ctx := celltest.Context(context.Background())
	_, err := r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "test", Repo: "example.test/test"})
	require.NoError(t, err)
	return r, ctx, jobdb.StartOrAppendJobRequest{Job: jobdb.StartOrAppendJob{AppendKey: "key", Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "test", JobType: "batch", Data: jobdb.NewTaskDataOrPanic("creator")}, Append: json.RawMessage(`1`)}}
}
func TestAppendRecoversCreatorPublication(t *testing.T) {
	r, ctx, q := appendFixture(t)
	scheduler := &appendPublicationFault{appendScheduler{r}, true}
	svc := runtimecore.AppendService{Scheduler: scheduler, Chapters: appendChapters{r}, Schemas: r}
	at := time.Now().Add(time.Hour)
	q.Job.Job.AvailableAt = &at
	_, err := svc.StartOrAppendJob(ctx, q)
	require.EqualError(t, err, "publication interrupted")
	q.Job.Job.Data = jobdb.NewTaskDataOrPanic("loser")
	q.Job.Job.AvailableAt = nil
	q.Job.Append = json.RawMessage(`2`)
	h, err := svc.StartOrAppendJob(ctx, q)
	require.NoError(t, err)
	chapters, err := r.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: h.JobKey})
	require.NoError(t, err)
	require.Len(t, chapters, 2)
	require.Equal(t, `"creator"`, string(chapters[0].Body.(jobdb.JobStartChapter).Input.Data))
	info, err := r.GetJob(ctx, h.JobKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobStatusAwaitingFuture, info.Status)
}
func TestAppendExpiredClaimsPreserveStateAndCrashCounter(t *testing.T) {
	r, ctx, q := appendFixture(t)
	at := time.Now().Add(time.Hour)
	q.Job.Job.AvailableAt = &at
	h, err := r.StartOrAppendJob(ctx, q)
	require.NoError(t, err)
	scheduler := appendScheduler{r}
	for i := 0; i < 8; i++ {
		claim, err := scheduler.ClaimAppend(ctx, runtimecore.ClaimAppendRequest{JobKey: h.JobKey, WorkerID: runtimecore.AppendWorkerPrefix + "test", Now: time.Now().Add(-time.Minute + time.Duration(i*2)*time.Second), Duration: time.Second})
		require.NoError(t, err)
		if i%2 == 0 {
			require.NoError(t, scheduler.ReleaseAppend(ctx, claim))
		}
	}
	q.Job.Append = json.RawMessage(`2`)
	got, err := r.StartOrAppendJob(ctx, q)
	require.NoError(t, err)
	require.Equal(t, h, got)
	r.engine.mu.Lock()
	rec := r.engine.jobRecords[h.JobKey]
	require.Zero(t, rec.consecutiveExpirations)
	require.True(t, at.Equal(rec.availableAt))
	require.Equal(t, jobdb.Route{JobType: "batch"}, rec.route)
	r.engine.mu.Unlock()
	events, err := r.ListLeaseEvents(ctx, jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{h.JobKey.JobId}})
	require.NoError(t, err)
	require.NotEmpty(t, events.Events)
	for _, event := range events.Events {
		require.Equal(t, jobdb.LeaseAcquisitionAppend, event.AcquisitionSource)
	}
}

func TestAppendClaimPollingPreservesPredecessorWait(t *testing.T) {
	r, ctx, q := appendFixture(t)
	q.Job.MaxInputs = 1
	first, err := r.StartOrAppendJob(ctx, q)
	require.NoError(t, err)
	second, err := r.StartOrAppendJob(ctx, q)
	require.NoError(t, err)
	require.NotEqual(t, first.JobKey, second.JobKey)
	scheduler := appendScheduler{r}
	claim, err := scheduler.ClaimAppend(ctx, runtimecore.ClaimAppendRequest{JobKey: second.JobKey, WorkerID: runtimecore.AppendWorkerPrefix + "test", Now: time.Now(), Duration: time.Minute})
	require.NoError(t, err)
	// Polling another route still visits the claimed row and projects its state.
	leases, err := r.PollWork(ctx, jobdb.PollWorkRequest{TenantId: "tenant", Routes: []jobdb.Route{{JobType: "unrelated"}}, WorkerID: "poller"})
	require.NoError(t, err)
	require.Empty(t, leases)
	require.NoError(t, scheduler.ReleaseAppend(ctx, claim))
	info, err := r.GetJob(ctx, second.JobKey)
	require.NoError(t, err)
	require.Equal(t, jobdb.JobStatusPendingJobs, info.Status)
	r.engine.mu.Lock()
	record := r.engine.jobRecords[second.JobKey]
	require.Equal(t, []string{first.JobKey.JobId}, record.waitFor)
	r.engine.mu.Unlock()
}
