package jobdb

import (
	"encoding/json"
	"fmt"
	"strings"
)

type RuntimeJobMetadata struct {
	SchemaHash  string `json:"schemaHash,omitempty"`
	ParentJobID string `json:"parentJobId,omitempty"`
}

type JobMetadataEnvelope struct {
	App      json.RawMessage     `json:"app,omitempty"`
	Internal *RuntimeJobMetadata `json:"internal,omitempty"`
}

func ValidateApplicationMetadata(raw json.RawMessage) error {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return fmt.Errorf("metadata must be a JSON object: %w", err)
	}
	return nil
}

func StripRuntimeMetadata(raw json.RawMessage) json.RawMessage {
	return AppMetadataFromStoredMetadata(raw)
}

func AppMetadataFromStoredMetadata(raw json.RawMessage) json.RawMessage {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return append(json.RawMessage(nil), raw...)
	}
	if _, hasApp := obj["app"]; hasApp {
		var envelope JobMetadataEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return append(json.RawMessage(nil), raw...)
		}
		if len(strings.TrimSpace(string(envelope.App))) == 0 || strings.TrimSpace(string(envelope.App)) == "null" {
			return nil
		}
		return append(json.RawMessage(nil), envelope.App...)
	}
	if internalRaw, hasInternal := obj["internal"]; hasInternal && storedInternalMetadataKnown(internalRaw) {
		var envelope JobMetadataEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return append(json.RawMessage(nil), raw...)
		}
		if len(strings.TrimSpace(string(envelope.App))) == 0 || strings.TrimSpace(string(envelope.App)) == "null" {
			return nil
		}
		return append(json.RawMessage(nil), envelope.App...)
	}
	return append(json.RawMessage(nil), raw...)
}

func storedInternalMetadataKnown(raw json.RawMessage) bool {
	var internal map[string]json.RawMessage
	if err := json.Unmarshal(raw, &internal); err != nil {
		return false
	}
	_, hasSchemaHash := internal["schemaHash"]
	_, hasParentJobID := internal["parentJobId"]
	return hasSchemaHash || hasParentJobID
}

func BuildJobMetadataEnvelope(appMetadata json.RawMessage, internal RuntimeJobMetadata) (json.RawMessage, error) {
	if err := ValidateApplicationMetadata(appMetadata); err != nil {
		return nil, err
	}
	app := NormalizeJSON(appMetadata)
	var internalPtr *RuntimeJobMetadata
	if !runtimeJobMetadataEmpty(internal) {
		internalCopy := internal
		internalPtr = &internalCopy
	}
	if len(app) == 0 && internalPtr == nil {
		return nil, nil
	}
	return json.Marshal(JobMetadataEnvelope{
		App:      app,
		Internal: internalPtr,
	})
}

func runtimeJobMetadataEmpty(meta RuntimeJobMetadata) bool {
	return strings.TrimSpace(meta.SchemaHash) == "" && strings.TrimSpace(meta.ParentJobID) == ""
}

func ExtractParentJobID(raw json.RawMessage) (string, bool, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return "", false, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return "", false, fmt.Errorf("metadata must be a JSON object: %w", err)
	}
	internalRaw, ok := root["internal"]
	if !ok {
		return "", false, nil
	}
	var runtimeMeta RuntimeJobMetadata
	if err := json.Unmarshal(internalRaw, &runtimeMeta); err != nil {
		return "", true, fmt.Errorf("internal metadata must be a JSON object: %w", err)
	}
	parentJobID := strings.TrimSpace(runtimeMeta.ParentJobID)
	return parentJobID, parentJobID != "", nil
}

func NormalizeJSON(raw json.RawMessage) json.RawMessage {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return append(json.RawMessage(nil), raw...)
	}
	out, err := json.Marshal(v)
	if err != nil {
		return append(json.RawMessage(nil), raw...)
	}
	return out
}
