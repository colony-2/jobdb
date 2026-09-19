package runtimecore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
)

// scheduleTargetSnapshot owns target bytes so a later application mutation
// cannot change a schedule's submitted input.
type scheduleTargetSnapshot struct {
	ClientPayload []byte                     `json:"clientPayloadBytes,omitempty"`
	JobType       string                     `json:"jobType"`
	Data          json.RawMessage            `json:"data,omitempty"`
	Artifacts     []scheduleArtifactSnapshot `json:"artifacts,omitempty"`
	RunPolicy     jobdb.RunPolicy            `json:"runPolicy,omitempty"`
	Metadata      json.RawMessage            `json:"metadata,omitempty"`
}

type scheduleArtifactSnapshot struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Digest string `json:"sha256,omitempty"`
	Data   []byte `json:"data,omitempty"`
}

func snapshotScheduleTarget(ctx context.Context, target jobdb.ScheduleTarget) (scheduleTargetSnapshot, error) {
	raw, err := target.Data.GetData()
	if err != nil {
		return scheduleTargetSnapshot{}, err
	}
	if !json.Valid(raw) {
		return scheduleTargetSnapshot{}, fmt.Errorf("schedule target data must be valid JSON")
	}
	artifacts, err := target.Data.GetArtifacts()
	if err != nil {
		return scheduleTargetSnapshot{}, err
	}
	stored := make([]scheduleArtifactSnapshot, 0, len(artifacts))
	for _, artifact := range artifacts {
		if artifact == nil {
			return scheduleTargetSnapshot{}, fmt.Errorf("schedule target artifact is nil")
		}
		body, err := artifact.Bytes(ctx)
		if err != nil {
			return scheduleTargetSnapshot{}, err
		}
		sum := sha256.Sum256(body)
		stored = append(stored, scheduleArtifactSnapshot{
			Name: artifact.Name(), Size: int64(len(body)),
			Digest: hex.EncodeToString(sum[:]), Data: append([]byte(nil), body...),
		})
	}
	initial, _, err := clientpayload.Initial(target.ClientPayloadUpdate)
	if err != nil {
		return scheduleTargetSnapshot{}, err
	}
	return scheduleTargetSnapshot{ClientPayload: initial,
		JobType: target.JobType, Data: append(json.RawMessage(nil), raw...),
		Artifacts: stored, RunPolicy: target.RunPolicy,
		Metadata: jobdb.NormalizeJSON(target.Metadata),
	}, nil
}

func (s scheduleTargetSnapshot) toTarget() jobdb.ScheduleTarget {
	artifacts := make([]jobdb.Artifact, 0, len(s.Artifacts))
	for _, artifact := range s.Artifacts {
		artifacts = append(artifacts, jobdb.NewArtifactFromBytes(artifact.Name,
			append([]byte(nil), artifact.Data...)))
	}
	var update *jobdb.ClientPayloadUpdate
	if s.ClientPayload != nil {
		update = &jobdb.ClientPayloadUpdate{Mode: "reset", Value: append([]byte(nil), s.ClientPayload...)}
	}
	return jobdb.ScheduleTarget{ClientPayloadUpdate: update,
		JobType:   s.JobType,
		Data:      &jobdb.SimpleTaskData{Data: append(json.RawMessage(nil), s.Data...), Artifacts: artifacts},
		RunPolicy: s.RunPolicy,
		Metadata:  append(json.RawMessage(nil), s.Metadata...),
	}
}

func decodeSchedule(stored StoredSchedule) (jobdb.ScheduleInfo, error) {
	var trigger jobdb.ScheduleTrigger
	if err := json.Unmarshal(stored.TriggerSnapshot, &trigger); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	var target scheduleTargetSnapshot
	if err := json.Unmarshal(stored.TargetSnapshot, &target); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	var failure jobdb.ScheduleFailurePolicy
	if err := json.Unmarshal(stored.FailurePolicySnapshot, &failure); err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	return jobdb.ScheduleInfo{
		TenantId: stored.ScheduleKey.TenantId, ScheduleId: stored.ScheduleKey.ScheduleId,
		ScheduleKey: stored.ScheduleKey, State: stored.State, EffectiveState: stored.State,
		Generation: stored.Generation, SpecHash: stored.SpecHash,
		Trigger: trigger, Target: target.toTarget(), OverlapPolicy: stored.OverlapPolicy,
		FailurePolicy: failure, NextFireAt: stored.NextFireAt,
		NextJobKey: stored.NextJobKey, CreatedAt: stored.CreatedAt.UTC(), UpdatedAt: stored.UpdatedAt.UTC(),
	}, nil
}
