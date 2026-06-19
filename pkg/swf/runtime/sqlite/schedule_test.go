package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

func TestScheduleMetadataIsHiddenFromPublicJobViews(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	now := time.Now().UTC()
	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-hidden", now, time.Hour)
	if info.NextJobKey == nil {
		t.Fatal("expected first scheduled job")
	}
	row, err := embedded.Runtime.loadJobRow(ctx, *info.NextJobKey)
	if err != nil {
		t.Fatalf("load scheduled job row: %v", err)
	}
	assertInternalScheduleMetadata(t, row.metadata, "schedule-hidden")

	resp, err := embedded.Runtime.ListJobs(ctx, swf.ListJobsRequest{
		TenantIds: []string{"tenant"},
		JobKeys:   []swf.JobKey{*info.NextJobKey},
	})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("listed jobs = %d, want 1", len(resp.Jobs))
	}
	assertPublicScheduleMetadata(t, resp.Jobs[0].Metadata)

	filter, err := swf.Metadata().EqualFilter("customer", "acme")
	if err != nil {
		t.Fatalf("metadata filter: %v", err)
	}
	resp, err = embedded.Runtime.ListJobs(ctx, swf.ListJobsRequest{
		TenantIds:      []string{"tenant"},
		MetadataFilter: filter,
	})
	if err != nil {
		t.Fatalf("list jobs by app metadata: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("app metadata jobs = %d, want 1", len(resp.Jobs))
	}
	assertPublicScheduleMetadata(t, resp.Jobs[0].Metadata)

	prefixFilter, err := swf.Metadata().EqualFilter("swf_customer", "visible")
	if err != nil {
		t.Fatalf("swf-prefixed app metadata filter: %v", err)
	}
	resp, err = embedded.Runtime.ListJobs(ctx, swf.ListJobsRequest{
		TenantIds:      []string{"tenant"},
		MetadataFilter: prefixFilter,
	})
	if err != nil {
		t.Fatalf("list jobs by swf-prefixed app metadata: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("swf-prefixed app metadata jobs = %d, want 1", len(resp.Jobs))
	}
	assertPublicScheduleMetadata(t, resp.Jobs[0].Metadata)

	internalNameFilter, err := swf.Metadata().EqualFilter("_swf", "app-visible")
	if err != nil {
		t.Fatalf("_swf app metadata filter should be allowed: %v", err)
	}
	resp, err = embedded.Runtime.ListJobs(ctx, swf.ListJobsRequest{
		TenantIds:      []string{"tenant"},
		MetadataFilter: internalNameFilter,
	})
	if err != nil {
		t.Fatalf("list jobs by _swf app metadata: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("_swf app metadata jobs = %d, want 1", len(resp.Jobs))
	}
	assertPublicScheduleMetadata(t, resp.Jobs[0].Metadata)

	scheduleFieldFilter, err := swf.Metadata().EqualFilter("swf_schedule_id", "schedule-hidden")
	if err != nil {
		t.Fatalf("swf-prefixed schedule-looking app metadata filter should be allowed: %v", err)
	}
	resp, err = embedded.Runtime.ListJobs(ctx, swf.ListJobsRequest{
		TenantIds:      []string{"tenant"},
		MetadataFilter: scheduleFieldFilter,
	})
	if err != nil {
		t.Fatalf("list jobs by schedule-looking app metadata: %v", err)
	}
	if len(resp.Jobs) != 0 {
		t.Fatalf("schedule-looking app metadata jobs = %d, want 0", len(resp.Jobs))
	}

	runs, err := embedded.Runtime.ListScheduleRuns(ctx, swf.ListScheduleRunsRequest{ScheduleKey: info.ScheduleKey})
	if err != nil {
		t.Fatalf("list schedule runs: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("schedule runs = %d, want 1", len(runs.Runs))
	}
	assertPublicScheduleMetadata(t, runs.Runs[0].JobSummary.Metadata)
}

func TestScheduleLeasePreflightSubmitsSerialSuccessorBeforeReturningLease(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	now := time.Now().UTC()
	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-successor", now, time.Hour)
	if info.NextJobKey == nil {
		t.Fatal("expected first scheduled job")
	}

	leases, err := embedded.Runtime.PollWork(ctx, swf.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-a",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll work: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %d, want 1", len(leases))
	}
	firstJobID := leases[0].Job().JobKey.JobId
	if firstJobID != info.NextJobKey.JobId {
		t.Fatalf("leased job = %s, want %s", firstJobID, info.NextJobKey.JobId)
	}

	runs, err := embedded.Runtime.ListScheduleRuns(ctx, swf.ListScheduleRunsRequest{ScheduleKey: info.ScheduleKey})
	if err != nil {
		t.Fatalf("list schedule runs: %v", err)
	}
	if len(runs.Runs) != 2 {
		t.Fatalf("schedule runs = %d, want 2", len(runs.Runs))
	}
	var sawFirst, sawSuccessor bool
	for _, run := range runs.Runs {
		assertPublicScheduleMetadata(t, run.JobSummary.Metadata)
		switch run.JobSummary.JobKey.JobId {
		case firstJobID:
			sawFirst = true
			if run.JobSummary.Status != swf.JobStatusActive {
				t.Fatalf("first status = %s, want ACTIVE", run.JobSummary.Status)
			}
		default:
			sawSuccessor = true
			if len(run.JobSummary.WaitFor) != 1 || run.JobSummary.WaitFor[0] != firstJobID {
				t.Fatalf("successor waitFor = %+v, want [%s]", run.JobSummary.WaitFor, firstJobID)
			}
			if run.JobSummary.Status != swf.JobStatusPendingJobs {
				t.Fatalf("successor status = %s, want PENDING_JOBS", run.JobSummary.Status)
			}
		}
	}
	if !sawFirst || !sawSuccessor {
		t.Fatalf("saw first=%v successor=%v in runs %+v", sawFirst, sawSuccessor, runs.Runs)
	}
}

func TestPausedScheduleCancelsUnstartedOccurrenceWithReason(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-paused", time.Now().UTC(), time.Hour)
	if _, err := embedded.Runtime.PauseSchedule(ctx, swf.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey}); err != nil {
		t.Fatalf("pause schedule: %v", err)
	}

	leases, err := embedded.Runtime.PollWork(ctx, swf.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-a",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll work: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases = %d, want 0", len(leases))
	}

	runs, err := embedded.Runtime.ListScheduleRuns(ctx, swf.ListScheduleRunsRequest{
		ScheduleKey: info.ScheduleKey,
		Statuses:    []swf.JobStatus{swf.JobStatusCancelled},
	})
	if err != nil {
		t.Fatalf("list schedule runs: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("cancelled schedule runs = %d, want 1", len(runs.Runs))
	}
	if runs.Runs[0].ReasonCode != "schedule_paused" {
		t.Fatalf("reason = %q, want schedule_paused", runs.Runs[0].ReasonCode)
	}
	assertPublicScheduleMetadata(t, runs.Runs[0].JobSummary.Metadata)
}

func TestScheduleRejectsInvalidTargetMetadataAndStateConflicts(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	_, err = embedded.Runtime.UpsertSchedule(ctx, swf.UpsertScheduleRequest{
		TenantId:   "tenant",
		ScheduleId: "invalid-metadata",
		Trigger: swf.ScheduleTrigger{
			Kind:     swf.ScheduleTriggerInterval,
			Interval: time.Hour,
		},
		Target: swf.ScheduleTarget{
			JobType:  "scheduled-job",
			Data:     swf.JobData(swf.NewTaskDataOrPanic(map[string]any{"n": 1})),
			Metadata: json.RawMessage(`["not","object"]`),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "metadata must be a JSON object") {
		t.Fatalf("expected invalid metadata error, got %v", err)
	}

	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-conflict", time.Now().UTC(), time.Hour)
	badGeneration := info.Generation + 1
	_, err = embedded.Runtime.PauseSchedule(ctx, swf.ScheduleMutationRequest{
		ScheduleKey:        info.ScheduleKey,
		ExpectedGeneration: &badGeneration,
	})
	if !errors.Is(err, swf.ErrConflict) {
		t.Fatalf("expected generation conflict, got %v", err)
	}

	if _, err := embedded.Runtime.ArchiveSchedule(ctx, swf.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey}); err != nil {
		t.Fatalf("archive schedule: %v", err)
	}
	_, err = embedded.Runtime.ResumeSchedule(ctx, swf.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey})
	if !errors.Is(err, swf.ErrConflict) {
		t.Fatalf("expected archived schedule conflict, got %v", err)
	}
}

func TestScheduleFailurePolicyCancelsSuccessorBeforeAppLease(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	info, err := embedded.Runtime.UpsertSchedule(ctx, swf.UpsertScheduleRequest{
		TenantId:      "tenant",
		ScheduleId:    "schedule-failure-policy",
		RequestTime:   time.Now().UTC(),
		OverlapPolicy: swf.ScheduleOverlapSerial,
		Trigger: swf.ScheduleTrigger{
			Kind:     swf.ScheduleTriggerInterval,
			Interval: time.Millisecond,
		},
		FailurePolicy: swf.ScheduleFailurePolicy{
			WindowSize:            2,
			MaxSequentialFailures: 1,
		},
		Target: swf.ScheduleTarget{
			JobType:  "scheduled-job",
			Data:     swf.JobData(swf.NewTaskDataOrPanic(map[string]any{"n": 1})),
			Metadata: json.RawMessage(`{"customer":"acme","swf_customer":"visible","_swf":"app-visible"}`),
		},
	})
	if err != nil {
		t.Fatalf("upsert schedule: %v", err)
	}

	leases, err := embedded.Runtime.PollWork(ctx, swf.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-a",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll first run: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("first leases = %d, want 1", len(leases))
	}
	if err := leases[0].Complete(ctx, swf.CompleteExecutionRequest{Status: "failed_app", Detail: "boom"}); err != nil {
		t.Fatalf("complete first failed: %v", err)
	}
	time.Sleep(3 * time.Millisecond)

	leases, err = embedded.Runtime.PollWork(ctx, swf.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-a",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll successor: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("successor leases = %d, want 0", len(leases))
	}

	runs, err := embedded.Runtime.ListScheduleRuns(ctx, swf.ListScheduleRunsRequest{
		ScheduleKey: info.ScheduleKey,
		Statuses:    []swf.JobStatus{swf.JobStatusCancelled},
	})
	if err != nil {
		t.Fatalf("list cancelled runs: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("cancelled runs = %d, want 1", len(runs.Runs))
	}
	if runs.Runs[0].ReasonCode != "failure_policy" {
		t.Fatalf("reason = %q, want failure_policy", runs.Runs[0].ReasonCode)
	}
}

func TestStartedScheduleRunIsReleasableAfterPauseAndLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-started-recovery", time.Now().UTC(), time.Hour)
	leases, err := embedded.Runtime.PollWork(ctx, swf.PollWorkRequest{
		TenantId:      "tenant",
		WorkerID:      "worker-a",
		Capabilities:  []string{"scheduled-job"},
		Limit:         1,
		LeaseDuration: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("poll initial lease: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("initial leases = %d, want 1", len(leases))
	}
	jobKey := leases[0].Job().JobKey

	if err := embedded.Runtime.PutChapter(ctx, swf.PutChapterRequest{
		LeaseID: leases[0].LeaseID(),
		Ref:     swf.ChapterRef{JobKey: jobKey, Ordinal: 1},
		Chapter: swf.Chapter{
			Ordinal:   1,
			TaskType:  "scheduled-job",
			CreatedAt: time.Now().UTC(),
			Body: swf.TaskAttemptOutcomeChapter{
				Outcome: swf.ApplicationOutputOutcome{Output: swf.ApplicationOutputBytes{Data: []byte(`{"started":true}`)}},
			},
		},
	}); err != nil {
		t.Fatalf("put started chapter: %v", err)
	}
	if _, err := embedded.Runtime.PauseSchedule(ctx, swf.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey}); err != nil {
		t.Fatalf("pause schedule: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	leases, err = embedded.Runtime.PollWork(ctx, swf.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-b",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll recovery lease: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("recovery leases = %d, want 1", len(leases))
	}
	if leases[0].Job().JobKey != jobKey {
		t.Fatalf("recovery leased job = %+v, want %+v", leases[0].Job().JobKey, jobKey)
	}
}

func upsertScheduleForTest(t *testing.T, ctx context.Context, runtime *Runtime, scheduleID string, now time.Time, interval time.Duration) swf.ScheduleInfo {
	t.Helper()
	info, err := runtime.UpsertSchedule(ctx, swf.UpsertScheduleRequest{
		TenantId:      "tenant",
		ScheduleId:    scheduleID,
		RequestTime:   now,
		OverlapPolicy: swf.ScheduleOverlapSerial,
		Trigger: swf.ScheduleTrigger{
			Kind:     swf.ScheduleTriggerInterval,
			Interval: interval,
		},
		Target: swf.ScheduleTarget{
			JobType:  "scheduled-job",
			Data:     swf.JobData(swf.NewTaskDataOrPanic(map[string]any{"n": 1})),
			Metadata: json.RawMessage(`{"customer":"acme","swf_customer":"visible","_swf":"app-visible"}`),
		},
	})
	if err != nil {
		t.Fatalf("upsert schedule: %v", err)
	}
	return info
}

func assertPublicScheduleMetadata(t *testing.T, raw json.RawMessage) {
	t.Helper()
	text := string(raw)
	if strings.Contains(text, "schedule_tick") {
		t.Fatalf("runtime schedule metadata leaked in public metadata: %s", text)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("metadata JSON: %v", err)
	}
	var customer string
	if err := json.Unmarshal(fields["customer"], &customer); err != nil {
		t.Fatalf("customer metadata missing: %v in %s", err, text)
	}
	if customer != "acme" {
		t.Fatalf("customer = %q, want acme", customer)
	}
	var swfCustomer string
	if err := json.Unmarshal(fields["swf_customer"], &swfCustomer); err != nil {
		t.Fatalf("swf_customer metadata missing: %v in %s", err, text)
	}
	if swfCustomer != "visible" {
		t.Fatalf("swf_customer = %q, want visible", swfCustomer)
	}
	var swfValue string
	if err := json.Unmarshal(fields["_swf"], &swfValue); err != nil {
		t.Fatalf("_swf app metadata missing: %v in %s", err, text)
	}
	if swfValue != "app-visible" {
		t.Fatalf("_swf app metadata = %q, want app-visible", swfValue)
	}
}

func assertInternalScheduleMetadata(t *testing.T, raw json.RawMessage, scheduleID string) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("stored metadata JSON: %v", err)
	}
	if _, ok := fields["app"]; !ok {
		t.Fatalf("stored metadata missing app namespace: %s", string(raw))
	}
	if _, ok := fields["internal"]; !ok {
		t.Fatalf("stored metadata missing internal namespace: %s", string(raw))
	}
	legacyFields := []string{
		"swf_kind",
		"swf_schedule_id",
		"swf_schedule_generation",
		"swf_scheduled_at",
		"swf_schedule_run_id",
		"swf_schedule_manual",
		"swf_schedule_backfill_id",
	}
	for _, key := range legacyFields {
		if _, ok := fields[key]; ok {
			t.Fatalf("stored metadata has legacy top-level schedule field %q: %s", key, string(raw))
		}
	}
	var envelope swf.JobMetadataEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("metadata envelope JSON: %v", err)
	}
	if len(envelope.App) == 0 {
		t.Fatalf("stored metadata missing app payload: %s", string(raw))
	}
	internal := envelope.Internal
	if internal == nil {
		t.Fatalf("stored metadata missing internal payload: %s", string(raw))
	}
	if internal.Schedule == nil {
		t.Fatalf("internal metadata missing schedule: %s", string(raw))
	}
	if internal.Schedule.Kind != swf.ScheduleMetadataKind {
		t.Fatalf("schedule metadata kind = %q, want %q", internal.Schedule.Kind, swf.ScheduleMetadataKind)
	}
	if internal.Schedule.ScheduleId != scheduleID {
		t.Fatalf("scheduleId = %q, want %q", internal.Schedule.ScheduleId, scheduleID)
	}
	if internal.Schedule.RunId == "" {
		t.Fatal("schedule metadata runId is required")
	}
}
