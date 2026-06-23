package jobmetadata

import (
	"encoding/json"
	"strings"
)

const SchemaHashKey = "jobdb_schema_hash"

// SchemaHashFromStoredMetadata extracts the schema hash from the stored job
// metadata envelope. The hash is optional; malformed metadata is treated as
// absent so it does not change legacy job behavior.
func SchemaHashFromStoredMetadata(raw json.RawMessage) string {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return ""
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return ""
	}
	if hash := schemaHashFromObject(root); hash != "" {
		return hash
	}
	if appRaw, ok := root["app"]; ok {
		var app map[string]json.RawMessage
		if err := json.Unmarshal(appRaw, &app); err == nil {
			if hash := schemaHashFromObject(app); hash != "" {
				return hash
			}
		}
	}
	if internalRaw, ok := root["internal"]; ok {
		var internal map[string]json.RawMessage
		if err := json.Unmarshal(internalRaw, &internal); err == nil {
			if hash := schemaHashFromObject(internal); hash != "" {
				return hash
			}
		}
	}
	return ""
}

func schemaHashFromObject(obj map[string]json.RawMessage) string {
	for _, key := range []string{SchemaHashKey, "schema_hash", "schemaHash"} {
		raw, ok := obj[key]
		if !ok {
			continue
		}
		var hash string
		if err := json.Unmarshal(raw, &hash); err != nil {
			continue
		}
		hash = strings.TrimSpace(hash)
		if hash != "" {
			return hash
		}
	}
	return ""
}
