// Package executionmeta defines the private reconstruction metadata carried by
// execution chapters. It does not change the public chapter or execution APIs.
package executionmeta

import (
	"encoding/json"
	"fmt"
)

const Key = "__execution_prefix_v3"

// Decode keeps the representation independent of the public record type so the
// chapter reader and workflow engine can share it without an import cycle.
func Decode(raw string) ([]json.RawMessage, error) {
	if raw == "" {
		return nil, nil
	}
	var records []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &records); err != nil {
		return nil, fmt.Errorf("invalid execution prefix: %w", err)
	}
	return records, nil
}
