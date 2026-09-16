package runtimecore_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
)

func TestProjectLeasePayloadPreservesVisibleView(t *testing.T) {
	base := runtimecore.LeasePayloadProjection{
		RunPolicy: jobdb.RunPolicy{
			Retry: jobdb.RetryPolicy{MaximumAttempts: 5},
		},
		TaskWork: &runtimecore.TaskWork{
			TaskType: "download", ResumeJobType: "collect",
			InputOrdinal: 2, OutputOrdinal: 3, InputHash: "sha256:input",
		},
	}
	generated, err := runtimecore.ProjectLeasePayload(base)
	if err != nil {
		t.Fatalf("project generated view: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(generated, &fields); err != nil {
		t.Fatalf("decode generated view: %v", err)
	}
	if _, ok := fields["run_policy"]; !ok {
		t.Fatalf("generated view lost run policy: %s", generated)
	}
	var task struct {
		Input  int64  `json:"in"`
		Output int64  `json:"out"`
		Next   string `json:"next"`
		Hash   string `json:"input_hash"`
	}
	if err := json.Unmarshal(fields["task_wait"], &task); err != nil ||
		task.Input != 2 || task.Output != 3 || task.Next != "collect" ||
		task.Hash != "sha256:input" {
		t.Fatalf("generated task view = %+v, %v", task, err)
	}

	for _, opaque := range []json.RawMessage{
		json.RawMessage(`{"kind":"rescheduled","n":2}`),
		json.RawMessage(`{}`),
	} {
		projected, err := runtimecore.ProjectLeasePayload(runtimecore.LeasePayloadProjection{
			RunPolicy: base.RunPolicy, TaskWork: base.TaskWork,
			Opaque: opaque, OpaquePresent: true,
		})
		if err != nil {
			t.Fatalf("project opaque view: %v", err)
		}
		var got, want any
		if err := json.Unmarshal(projected, &got); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(opaque, &want); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("opaque view = %s, want %s", projected, opaque)
		}
	}
	if _, err := runtimecore.ProjectLeasePayload(runtimecore.LeasePayloadProjection{
		Opaque: json.RawMessage(`null`), OpaquePresent: true,
	}); err == nil {
		t.Fatal("expected non-object opaque payload to fail")
	}
}
