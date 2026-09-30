package remote

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
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

// Embedding the interface lets this test panic if any runtime method is reached;
// removed request fields must be rejected by HTTP validation before dispatch.
type rejectCompletionPayloadRuntime struct{ jobdb.WorkflowRuntime }

func TestCommitIfWaitingRejectsClientPayloadUpdate(t *testing.T) {
	handler := NewServer(rejectCompletionPayloadRuntime{})
	for _, update := range []string{`null`, `{}`, `{"mode":"reset","expectedRevision":"0","value":{"cursor":"next"}}`} {
		body := `{"route":{"jobType":"job","taskType":"external"},"data":{"data":2,"artifacts":[]},"clientPayloadUpdate":` + update + `}`
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant/jobs/job/chapters/1/commit-if-waiting", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "clientPayloadUpdate") {
			t.Fatalf("removed update %s: status=%d body=%s", update, response.Code, response.Body.String())
		}
	}
}
