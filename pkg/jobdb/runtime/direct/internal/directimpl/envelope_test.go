package directimpl

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	chapterartifact "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/artifact"
)

func TestTaskAppErrorEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	input := jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 1})
	inputHash, err := computeInputHash(ctx, input)
	if err != nil {
		t.Fatalf("hash input: %v", err)
	}

	appErr := jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "user boom", Level: "error"}}
	payload, kind, err := errorPayloadFromError(appErr, nil)
	if err != nil {
		t.Fatalf("taskDataFromError: %v", err)
	}
	if kind != payloadKindAppError {
		t.Fatalf("expected payload kind %s, got %s", payloadKindAppError, kind)
	}

	taskType := "taskErr"
	chap, err := payloadToChapter(payload, nil, 1, taskType, "worker1", chapterTypeTaskAttemptOutcome, kind, inputHash, time.Now(), chapterMetadata{})
	if err != nil {
		t.Fatalf("taskDataToChapter: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.PayloadKind != payloadKindAppError {
		t.Fatalf("unexpected payload kind %s", env.PayloadKind)
	}
	if env.Meta.TaskType != taskType {
		t.Fatalf("expected task type %s, got %s", taskType, env.Meta.TaskType)
	}
	var payloadBody jobdb.AppErrorPayload
	if err := json.Unmarshal(env.Payload, &payloadBody); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payloadBody.Message != appErr.Payload.Message {
		t.Fatalf("payload message mismatch: %s", payloadBody.Message)
	}

	artifacts := convertChapterArtifactsToJobDB(chap.Artifacts())
	td, payloadErr := envelopeToTaskData(env, artifacts)
	if td == nil {
		t.Fatalf("expected task data envelope on error payload")
	}
	if envTD, ok := td.(*jobdb.EnvelopedTaskData); !ok || envTD.Kind != payloadKindAppError {
		t.Fatalf("expected enveloped task data with kind %s, got %T %+v", payloadKindAppError, td, td)
	}
	var gotAppErr jobdb.AppError
	if !errors.As(payloadErr, &gotAppErr) {
		t.Fatalf("expected AppError, got %v", payloadErr)
	}
	if gotAppErr.Payload.Message != appErr.Payload.Message {
		t.Fatalf("app error message mismatch: %s", gotAppErr.Payload.Message)
	}
}

func TestTaskSystemErrorEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	input := jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 2})
	inputHash, err := computeInputHash(ctx, input)
	if err != nil {
		t.Fatalf("hash input: %v", err)
	}

	sysErr := jobdb.SystemError{Payload: jobdb.SystemErrorPayload{Message: "infra fail", Component: "chapterstore"}}
	payload, kind, err := errorPayloadFromError(sysErr, nil)
	if err != nil {
		t.Fatalf("taskDataFromError: %v", err)
	}
	if kind != payloadKindSystemError {
		t.Fatalf("expected payload kind %s, got %s", payloadKindSystemError, kind)
	}

	taskType := "taskSysErr"
	chap, err := payloadToChapter(payload, nil, 1, taskType, "worker1", chapterTypeTaskAttemptOutcome, kind, inputHash, time.Now(), chapterMetadata{})
	if err != nil {
		t.Fatalf("taskDataToChapter: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.PayloadKind != payloadKindSystemError {
		t.Fatalf("unexpected payload kind %s", env.PayloadKind)
	}
	if env.Meta.TaskType != taskType {
		t.Fatalf("expected task type %s, got %s", taskType, env.Meta.TaskType)
	}
	var payloadBody jobdb.SystemErrorPayload
	if err := json.Unmarshal(env.Payload, &payloadBody); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payloadBody.Message != sysErr.Payload.Message {
		t.Fatalf("payload message mismatch: %s", payloadBody.Message)
	}

	artifacts := convertChapterArtifactsToJobDB(chap.Artifacts())
	td, payloadErr := envelopeToTaskData(env, artifacts)
	if td == nil {
		t.Fatalf("expected task data envelope on system error payload")
	}
	if envTD, ok := td.(*jobdb.EnvelopedTaskData); !ok || envTD.Kind != payloadKindSystemError {
		t.Fatalf("expected enveloped task data with kind %s, got %T %+v", payloadKindSystemError, td, td)
	}
	var gotSysErr jobdb.SystemError
	if !errors.As(payloadErr, &gotSysErr) {
		t.Fatalf("expected systemError, got %v", payloadErr)
	}
	if gotSysErr.Payload.Message != sysErr.Payload.Message {
		t.Fatalf("system error message mismatch: %s", gotSysErr.Payload.Message)
	}
}

func TestJobAppErrorEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	input := jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 3})
	inputHash, err := computeInputHash(ctx, input)
	if err != nil {
		t.Fatalf("hash input: %v", err)
	}

	appErr := jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "job failed"}}
	payload, kind, err := errorPayloadFromError(appErr, nil)
	if err != nil {
		t.Fatalf("taskDataFromError: %v", err)
	}
	if kind != payloadKindAppError {
		t.Fatalf("expected payload kind %s, got %s", payloadKindAppError, kind)
	}

	taskType := "jobWorker"
	chap, err := payloadToChapter(payload, nil, 1, taskType, "worker-job", chapterTypeJobAttemptOutcome, kind, inputHash, time.Now(), chapterMetadata{})
	if err != nil {
		t.Fatalf("taskDataToChapter: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.PayloadKind != payloadKindAppError {
		t.Fatalf("unexpected payload kind %s", env.PayloadKind)
	}
	if env.Meta.TaskType != taskType {
		t.Fatalf("expected task type %s, got %s", taskType, env.Meta.TaskType)
	}

	artifacts := convertChapterArtifactsToJobDB(chap.Artifacts())
	td, payloadErr := envelopeToTaskData(env, artifacts)
	if td == nil {
		t.Fatalf("expected task data envelope on job app error payload")
	}
	if envTD, ok := td.(*jobdb.EnvelopedTaskData); !ok || envTD.Kind != payloadKindAppError {
		t.Fatalf("expected enveloped task data with kind %s, got %T %+v", payloadKindAppError, td, td)
	}
	var gotAppErr jobdb.AppError
	if !errors.As(payloadErr, &gotAppErr) {
		t.Fatalf("expected AppError, got %v", payloadErr)
	}
}

func TestJobSystemErrorEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	input := jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 4})
	inputHash, err := computeInputHash(ctx, input)
	if err != nil {
		t.Fatalf("hash input: %v", err)
	}

	sysErr := jobdb.SystemError{Payload: jobdb.SystemErrorPayload{Message: "job infra fail", Component: "pgwf"}}
	payload, kind, err := errorPayloadFromError(sysErr, nil)
	if err != nil {
		t.Fatalf("taskDataFromError: %v", err)
	}
	if kind != payloadKindSystemError {
		t.Fatalf("expected payload kind %s, got %s", payloadKindSystemError, kind)
	}

	taskType := "jobWorker"
	chap, err := payloadToChapter(payload, nil, 1, taskType, "worker-job", chapterTypeJobAttemptOutcome, kind, inputHash, time.Now(), chapterMetadata{})
	if err != nil {
		t.Fatalf("taskDataToChapter: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.PayloadKind != payloadKindSystemError {
		t.Fatalf("unexpected payload kind %s", env.PayloadKind)
	}
	if env.Meta.TaskType != taskType {
		t.Fatalf("expected task type %s, got %s", taskType, env.Meta.TaskType)
	}

	artifacts := convertChapterArtifactsToJobDB(chap.Artifacts())
	td, payloadErr := envelopeToTaskData(env, artifacts)
	if td == nil {
		t.Fatalf("expected task data envelope on job system error payload")
	}
	if envTD, ok := td.(*jobdb.EnvelopedTaskData); !ok || envTD.Kind != payloadKindSystemError {
		t.Fatalf("expected enveloped task data with kind %s, got %T %+v", payloadKindSystemError, td, td)
	}
	var gotSysErr jobdb.SystemError
	if !errors.As(payloadErr, &gotSysErr) {
		t.Fatalf("expected systemError, got %v", payloadErr)
	}
}

// convertChapterArtifactsToJobDB converts chapter artifacts to jobdb artifacts
func convertChapterArtifactsToJobDB(chapterArts []chapterartifact.Artifact) []jobdb.Artifact {
	artifacts := make([]jobdb.Artifact, 0, len(chapterArts))
	for _, a := range chapterArts {
		artifacts = append(artifacts, fromChapterArtifact(a))
	}
	return artifacts
}
