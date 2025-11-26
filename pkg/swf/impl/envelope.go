package impl

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

const (
	envelopeVersion        = 1
	payloadKindApp         = "App"
	payloadKindAppError    = "AppError"
	payloadKindSystemError = "SystemError"
)

type chapterMeta struct {
	Version   int       `json:"version"`
	Ordinal   int64     `json:"ordinal"`
	TaskType  string    `json:"task_type"`
	WorkerID  string    `json:"worker_id"`
	CreatedAt time.Time `json:"created_at"`
	InputHash string    `json:"input_hash"`
}

type chapterEnvelope struct {
	Meta        chapterMeta      `json:"meta"`
	PayloadKind string           `json:"payload_kind"`
	Payload     json.RawMessage  `json:"payload"`
}

func buildChapterEnvelope(meta chapterMeta, payloadKind string, payload json.RawMessage) ([]byte, error) {
	if payloadKind == "" {
		return nil, fmt.Errorf("payload kind is required")
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("payload must be valid JSON")
	}

	env := chapterEnvelope{
		Meta:        meta,
		PayloadKind: payloadKind,
		Payload:     payload,
	}

	return json.Marshal(env)
}

func decodeChapterEnvelope(body []byte) (chapterEnvelope, error) {
	var env chapterEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return chapterEnvelope{}, err
	}
	return env, nil
}

func computeInputHash(ctx context.Context, taskData swf.TaskData) (string, error) {
	if taskData == nil {
		return "", fmt.Errorf("task data is required for hashing")
	}

	data, err := taskData.GetData()
	if err != nil {
		return "", err
	}
	dataBytes, err := data.ToBytes()
	if err != nil {
		return "", err
	}

	artifacts, err := taskData.GetArtifacts()
	if err != nil {
		return "", err
	}

	artifactParts := make([]string, 0, len(artifacts))
	for _, art := range artifacts {
		hash, err := artifactHash(ctx, art)
		if err != nil {
			return "", err
		}
		uri := art.ID()
		if uri == "" {
			uri = art.Name()
		}
		artifactParts = append(artifactParts, fmt.Sprintf("%s|%s|%s", uri, hash, art.Name()))
	}
	sort.Strings(artifactParts)

	h := sha256.New()
	_, _ = h.Write(dataBytes)
	for _, part := range artifactParts {
		_, _ = h.Write([]byte(part))
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func artifactHash(ctx context.Context, art swf.Artifact) (string, error) {
	return art.Sha256(ctx)
}

func envelopeToTaskData(env chapterEnvelope, artifacts []swf.Artifact) (swf.TaskData, error) {
	if env.PayloadKind != payloadKindApp {
		return nil, fmt.Errorf("unsupported payload kind %q", env.PayloadKind)
	}

	copiedArtifacts := make([]swf.Artifact, 0, len(artifacts))
	for _, a := range artifacts {
		copiedArtifacts = append(copiedArtifacts, a)
	}

	// Keep payload as-is (already validated).
	payload := make([]byte, len(env.Payload))
	copy(payload, env.Payload)
	task := swf.SimpleTaskData{
		Data:      swf.NewBytesData(payload),
		Artifacts: copiedArtifacts,
	}
	return &task, nil
}
