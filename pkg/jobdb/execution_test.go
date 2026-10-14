package jobdb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExecutionRecordValidation(t *testing.T) {
	good := ExecutionRecord{Version: ExecutionPatternVersion, Kind: "result", ID: "task/1", At: time.Now().UTC(), Output: &ExecutionSnapshot{Data: json.RawMessage(`{"value":1}`)}}
	raw, err := json.Marshal(good)
	require.NoError(t, err)
	decoded, err := DecodeExecutionRecord(raw)
	require.NoError(t, err)
	require.Equal(t, good, decoded)
	for _, test := range []struct {
		name   string
		change func(*ExecutionRecord)
	}{
		{"missing version", func(r *ExecutionRecord) { r.Version = 0 }},
		{"future version", func(r *ExecutionRecord) { r.Version = 99 }},
		{"unknown kind", func(r *ExecutionRecord) { r.Kind = "future" }},
		{"missing id", func(r *ExecutionRecord) { r.ID = "" }},
		{"missing outcome", func(r *ExecutionRecord) { r.Output = nil }},
		{"ambiguous v1 outcome", func(r *ExecutionRecord) {
			r.Version = 1
			r.ErrorKind = "AppError"
			r.Error = json.RawMessage(`{"message":"bad"}`)
		}},
		{"unknown error", func(r *ExecutionRecord) { r.Output = nil; r.ErrorKind = "FutureError"; r.Error = json.RawMessage(`{}`) }},
		{"missing input", func(r *ExecutionRecord) { r.Kind = "wait" }},
		{"missing descriptor", func(r *ExecutionRecord) { r.Kind = "scope" }},
		{"invalid attempt", func(r *ExecutionRecord) { r.Kind = "attempt"; r.Attempt = -1 }},
		{"invalid disposition", func(r *ExecutionRecord) { r.Disposition = "unknown" }},
		{"missing versions", func(r *ExecutionRecord) { r.Kind = "header" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := good
			test.change(&r)
			raw, err := json.Marshal(r)
			require.NoError(t, err)
			_, err = DecodeExecutionRecord(raw)
			require.Error(t, err)
		})
	}
	_, err = DecodeExecutionRecord(append(raw, []byte(` {}`)...))
	require.Error(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(raw, &fields))
	fields["unknown_field"] = true
	raw, err = json.Marshal(fields)
	require.NoError(t, err)
	_, err = DecodeExecutionRecord(raw)
	require.Error(t, err)
}

func TestExecutionDiagnosticOutcomeVersion(t *testing.T) {
	for _, version := range []uint32{1, 2} {
		r := ExecutionRecord{Version: version, Kind: "result", ID: "task/attempt:1", ErrorKind: "AppError", Error: json.RawMessage(`{"message":"failed"}`), Output: &ExecutionSnapshot{Data: json.RawMessage(`{"diagnostic":"sanitized"}`)}}
		raw, err := json.Marshal(r)
		require.NoError(t, err)
		_, err = DecodeExecutionRecord(raw)
		if version < 2 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		r.Output = nil
		raw, err = json.Marshal(r)
		require.NoError(t, err)
		_, err = DecodeExecutionRecord(raw)
		require.NoError(t, err)
	}
}

func TestExecutionDispatchVersion(t *testing.T) {
	for _, version := range []uint32{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			r := ExecutionRecord{Version: version, Kind: "dispatch", ID: "task/attempt:1", ExecutionTaskType: "fallback"}
			raw, err := json.Marshal(r)
			require.NoError(t, err)
			_, err = DecodeExecutionRecord(raw)
			if version == 1 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			r.ExecutionTaskType = ""
			raw, err = json.Marshal(r)
			require.NoError(t, err)
			_, err = DecodeExecutionRecord(raw)
			require.Error(t, err)
		})
	}
	for _, field := range []string{"alternate_at", "execution_task_type"} {
		r := ExecutionRecord{Version: 1, Kind: "wait", ID: "task/attempt:1", Descriptor: json.RawMessage(`"primary"`), Input: &ExecutionSnapshot{Data: json.RawMessage(`null`)}}
		if field == "alternate_at" {
			at := time.Now().UTC()
			r.AlternateAt = &at
		} else {
			r.ExecutionTaskType = "fallback"
		}
		raw, err := json.Marshal(r)
		require.NoError(t, err)
		_, err = DecodeExecutionRecord(raw)
		require.Error(t, err)
	}
}
