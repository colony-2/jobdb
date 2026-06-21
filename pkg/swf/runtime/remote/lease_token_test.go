package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/leaseauth"
	"github.com/colony-2/swf-go/pkg/swf/internal/runtimeapi"
)

func TestLeaseTokenMintForLeaseUsesExpiryAndSchemaHash(t *testing.T) {
	signer := &leaseTokenSigner{key: bytes.Repeat([]byte{7}, 32)}
	jobKey := swf.JobKey{TenantId: "tenant-token", JobId: "job-token"}
	leaseExpiresAt := time.Now().UTC().Add(5 * time.Second)

	token, err := signer.mintForLease(&testExecutionLease{
		jobKey:      jobKey,
		leaseID:     "lease-token",
		workerID:    "worker-token",
		expiresAt:   leaseExpiresAt,
		schemaHash:  "sha256:schema",
		capability:  "cap",
		payloadJSON: json.RawMessage(`{"ok":true}`),
	}, 30*time.Second)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	claims, err := signer.validateAndParse(token, jobKey, "lease-token", time.Now().UTC())
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	if claims.SchemaHash != "sha256:schema" {
		t.Fatalf("schema hash = %q, want sha256:schema", claims.SchemaHash)
	}
	if !claims.expiresAt().Before(leaseExpiresAt) {
		t.Fatalf("token expiry %s should be before lease expiry %s", claims.expiresAt(), leaseExpiresAt)
	}
	if diff := leaseExpiresAt.Sub(claims.expiresAt()); diff <= 0 || diff > time.Second {
		t.Fatalf("token expiry skew = %s, want >0 and <=1s", diff)
	}
	if got := claims.leaseDuration(); got < 4*time.Second || got > 6*time.Second {
		t.Fatalf("lease duration = %s, want roughly 5s", got)
	}
}

func TestAddChapterWithLeasePassesValidatedClaimsToRuntime(t *testing.T) {
	signer := &leaseTokenSigner{key: bytes.Repeat([]byte{9}, 32)}
	jobKey := swf.JobKey{TenantId: "tenant-server", JobId: "job-server"}
	leaseID := "lease-server"
	schemaHash := "sha256:server-schema"
	token, err := signer.mintForLeaseExpiry(jobKey, leaseID, "worker-server", schemaHash, time.Now().UTC().Add(5*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	runtime := &claimsCapturingRuntime{}
	server := &proxyServer{runtime: runtime, tokens: signer}

	chapter := swf.Chapter{
		Ordinal:   1,
		TaskType:  "server-task",
		CreatedAt: time.Now().UTC(),
		Body: swf.TaskAttemptOutcomeChapter{Outcome: swf.ApplicationOutputOutcome{
			Output: swf.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
		}},
	}
	body, err := runtimeChapterToAddRequest(context.Background(), chapter, nil)
	if err != nil {
		t.Fatalf("build add chapter body: %v", err)
	}
	resp, err := server.AddChapterWithLease(context.Background(), runtimeapi.AddChapterWithLeaseRequestObject{
		TenantId: jobKey.TenantId,
		JobId:    jobKey.JobId,
		LeaseId:  leaseID,
		Params:   runtimeapi.AddChapterWithLeaseParams{XSWFLeaseToken: token},
		Body:     &body,
	})
	if err != nil {
		t.Fatalf("add chapter with lease: %v", err)
	}
	if _, ok := resp.(runtimeapi.AddChapterWithLease204Response); !ok {
		t.Fatalf("response = %T, want AddChapterWithLease204Response", resp)
	}
	if runtime.req.LeaseID != leaseID || runtime.req.Ref.JobKey != jobKey {
		t.Fatalf("unexpected put chapter request %+v", runtime.req)
	}
	if !runtime.sawClaims {
		t.Fatal("runtime did not receive lease claims")
	}
	if runtime.claims.SchemaHash != schemaHash {
		t.Fatalf("schema hash = %q, want %q", runtime.claims.SchemaHash, schemaHash)
	}
	if !leaseauth.Matches(runtime.claims, jobKey, leaseID) {
		t.Fatalf("claims do not match job/lease: %+v", runtime.claims)
	}
}

type testExecutionLease struct {
	jobKey      swf.JobKey
	leaseID     string
	workerID    string
	expiresAt   time.Time
	schemaHash  string
	capability  string
	payloadJSON json.RawMessage
}

func (l *testExecutionLease) LeaseID() string    { return l.leaseID }
func (l *testExecutionLease) Job() swf.JobHandle { return swf.JobHandle{JobKey: l.jobKey} }
func (l *testExecutionLease) Capability() string { return l.capability }
func (l *testExecutionLease) Payload() json.RawMessage {
	return append(json.RawMessage(nil), l.payloadJSON...)
}
func (l *testExecutionLease) KeepAlive(context.Context) error { return nil }
func (l *testExecutionLease) StopKeepAlive()                  {}
func (l *testExecutionLease) Complete(context.Context, swf.CompleteExecutionRequest) error {
	return nil
}
func (l *testExecutionLease) Reschedule(context.Context, swf.RescheduleExecutionRequest) error {
	return nil
}
func (l *testExecutionLease) LeaseWorkerID() string   { return l.workerID }
func (l *testExecutionLease) LeaseExpiry() time.Time  { return l.expiresAt }
func (l *testExecutionLease) LeaseSchemaHash() string { return l.schemaHash }

type claimsCapturingRuntime struct {
	req       swf.PutChapterRequest
	claims    leaseauth.Claims
	sawClaims bool
}

func (r *claimsCapturingRuntime) SubmitJob(context.Context, swf.SubmitJobRequest) (swf.JobHandle, error) {
	return swf.JobHandle{}, errors.New("unexpected SubmitJob")
}

func (r *claimsCapturingRuntime) SubmitRestartJob(context.Context, swf.SubmitRestartJobRequest) (swf.JobHandle, error) {
	return swf.JobHandle{}, errors.New("unexpected SubmitRestartJob")
}

func (r *claimsCapturingRuntime) PollWork(context.Context, swf.PollWorkRequest) ([]swf.ExecutionLease, error) {
	return nil, errors.New("unexpected PollWork")
}

func (r *claimsCapturingRuntime) GetJobLease(context.Context, swf.GetJobLeaseRequest) (swf.ExecutionLease, error) {
	return nil, errors.New("unexpected GetJobLease")
}

func (r *claimsCapturingRuntime) CancelJob(context.Context, swf.CancelJobRequest) error {
	return errors.New("unexpected CancelJob")
}

func (r *claimsCapturingRuntime) CompleteTaskIfWaiting(context.Context, swf.CompleteTaskIfWaitingRequest) error {
	return errors.New("unexpected CompleteTaskIfWaiting")
}

func (r *claimsCapturingRuntime) UpsertSchedule(context.Context, swf.UpsertScheduleRequest) (swf.ScheduleInfo, error) {
	return swf.ScheduleInfo{}, errors.New("unexpected UpsertSchedule")
}

func (r *claimsCapturingRuntime) GetSchedule(context.Context, swf.ScheduleKey) (swf.ScheduleInfo, error) {
	return swf.ScheduleInfo{}, errors.New("unexpected GetSchedule")
}

func (r *claimsCapturingRuntime) ListSchedules(context.Context, swf.ListSchedulesRequest) (swf.ListSchedulesResponse, error) {
	return swf.ListSchedulesResponse{}, errors.New("unexpected ListSchedules")
}

func (r *claimsCapturingRuntime) PauseSchedule(context.Context, swf.ScheduleMutationRequest) (swf.ScheduleInfo, error) {
	return swf.ScheduleInfo{}, errors.New("unexpected PauseSchedule")
}

func (r *claimsCapturingRuntime) ResumeSchedule(context.Context, swf.ScheduleMutationRequest) (swf.ScheduleInfo, error) {
	return swf.ScheduleInfo{}, errors.New("unexpected ResumeSchedule")
}

func (r *claimsCapturingRuntime) ArchiveSchedule(context.Context, swf.ScheduleMutationRequest) (swf.ScheduleInfo, error) {
	return swf.ScheduleInfo{}, errors.New("unexpected ArchiveSchedule")
}

func (r *claimsCapturingRuntime) TriggerSchedule(context.Context, swf.TriggerScheduleRequest) (swf.JobHandle, error) {
	return swf.JobHandle{}, errors.New("unexpected TriggerSchedule")
}

func (r *claimsCapturingRuntime) ListScheduleRuns(context.Context, swf.ListScheduleRunsRequest) (swf.ListScheduleRunsResponse, error) {
	return swf.ListScheduleRunsResponse{}, errors.New("unexpected ListScheduleRuns")
}

func (r *claimsCapturingRuntime) GetJob(context.Context, swf.JobKey) (swf.JobInfo, error) {
	return swf.JobInfo{}, errors.New("unexpected GetJob")
}

func (r *claimsCapturingRuntime) ListJobs(context.Context, swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	return swf.ListJobsResponse{}, errors.New("unexpected ListJobs")
}

func (r *claimsCapturingRuntime) GetChapter(context.Context, swf.ChapterRef) (swf.Chapter, error) {
	return swf.Chapter{}, errors.New("unexpected GetChapter")
}

func (r *claimsCapturingRuntime) ListChapters(context.Context, swf.ListChaptersRequest) ([]swf.Chapter, error) {
	return nil, errors.New("unexpected ListChapters")
}

func (r *claimsCapturingRuntime) PutChapter(ctx context.Context, req swf.PutChapterRequest) error {
	r.req = req
	var ok bool
	r.claims, ok = leaseauth.ClaimsFromContext(ctx)
	r.sawClaims = ok
	if !ok {
		return errors.New("missing lease claims")
	}
	if !leaseauth.Matches(r.claims, req.Ref.JobKey, req.LeaseID) {
		return swf.ErrExecutionLeaseLost
	}
	return nil
}

func (r *claimsCapturingRuntime) OpenArtifact(context.Context, swf.ArtifactRef) (swf.ArtifactReader, error) {
	return nil, errors.New("unexpected OpenArtifact")
}

var _ swf.ExecutionLease = (*testExecutionLease)(nil)
var _ swf.WorkflowRuntime = (*claimsCapturingRuntime)(nil)
