package jobmetadata

import (
	"encoding/json"
	"testing"
)

func TestSchemaHashFromStoredMetadata(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
		want string
	}{
		{
			name: "top level reserved key",
			raw:  json.RawMessage(`{"swf_schema_hash":"sha256:top"}`),
			want: "sha256:top",
		},
		{
			name: "stored app envelope",
			raw:  json.RawMessage(`{"app":{"swf_schema_hash":"sha256:app"}}`),
			want: "sha256:app",
		},
		{
			name: "stored internal envelope",
			raw:  json.RawMessage(`{"internal":{"schema_hash":"sha256:internal"}}`),
			want: "sha256:internal",
		},
		{
			name: "absent",
			raw:  json.RawMessage(`{"app":{"queue":"blue"}}`),
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := SchemaHashFromStoredMetadata(tc.raw); got != tc.want {
				t.Fatalf("schema hash = %q, want %q", got, tc.want)
			}
		})
	}
}
