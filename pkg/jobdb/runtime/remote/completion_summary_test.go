package remote

import (
	"encoding/json"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
	"github.com/stretchr/testify/require"
)

func TestCompletionSummaryWireCompatibility(t *testing.T) {
	for _, status := range []string{"", "success", "failed_app", "failed_system", "failed_timeout", "cancelled", "future_category"} {
		t.Run(status, func(t *testing.T) {
			want := jobdb.JobSummary{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, JobType: "job", Status: jobdb.JobStatusCompleted, CompletionStatus: status}
			if status != "" && status != "success" {
				want.CompletionDetail = "exact detail\nwith unicode: \u2603"
			}
			wire, err := jobSummaryToAPI(want)
			require.NoError(t, err)
			raw, err := json.Marshal(wire)
			require.NoError(t, err)
			var decoded runtimeapi.JobSummary
			require.NoError(t, json.Unmarshal(raw, &decoded))
			got, err := jobSummaryFromAPI(decoded)
			require.NoError(t, err)
			require.Equal(t, want.CompletionStatus, got.CompletionStatus)
			require.Equal(t, want.CompletionDetail, got.CompletionDetail)
			// Older servers omit both fields. Do not infer success from status.
			var old map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &old))
			delete(old, "completionStatus")
			delete(old, "completionDetail")
			raw, err = json.Marshal(old)
			require.NoError(t, err)
			decoded = runtimeapi.JobSummary{}
			require.NoError(t, json.Unmarshal(raw, &decoded))
			got, err = jobSummaryFromAPI(decoded)
			require.NoError(t, err)
			require.Empty(t, got.CompletionStatus)
			require.Empty(t, got.CompletionDetail)
		})
	}
}
