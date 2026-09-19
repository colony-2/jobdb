package remote

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

func TestRescheduleRejectsRemovedAndImmutableFields(t *testing.T) {
	typ := reflect.TypeOf(runtimeapi.RescheduleExecutionRequest{})
	for _, raw := range []string{`{"nextNeed":"job"}`, `{"alternateNeed":"job"}`, `{"payload":{}}`, `{"runPolicy":{}}`, `{"clientPayloadUpdate":null}`, `{"taskWait":{"run_policy":{}}}`} {
		if err := validateRequestFields([]byte(raw), typ, ""); err == nil {
			t.Fatalf("accepted removed field: %s", raw)
		}
	}
	raw := []byte(`{"nextRoute":{"jobType":"job"},"clientPayloadUpdate":{"mode":"reset","expectedRevision":"1","value":{"run_policy":1,"task_wait":2}}}`)
	if err := validateRequestFields(raw, typ, ""); err != nil {
		t.Fatal(err)
	}
	var v runtimeapi.RescheduleExecutionRequest
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if string(v.ClientPayloadUpdate.Value) != `{"run_policy":1,"task_wait":2}` {
		t.Fatal("client data changed")
	}
}
