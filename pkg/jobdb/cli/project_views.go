package cli

import (
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/projectwire"
	"io"
)

// These are public CLI presentation envelopes. Internal JobSummary deliberately
// has no JSON tags; do not change that shared type or its historical encoding.
func encodeProjectCLI(w io.Writer, value any) error {
	if page, ok := value.(jobdb.ListJobsResponse); ok {
		rows := make([]map[string]any, 0, len(page.Jobs))
		for _, j := range page.Jobs {
			rows = append(rows, map[string]any{
				"jobKey":    map[string]string{"projectId": j.JobKey.TenantId, "jobId": j.JobKey.JobId},
				"appendKey": j.AppendKey, "cell": j.Cell, "cellId": j.CellID, "cellRepo": j.CellRepo, "goalId": j.GoalID,
				"recordKind": j.RecordKind, "terminalFailure": j.TerminalFailure,
				"status": j.Status, "completionStatus": j.CompletionStatus, "completionDetail": j.CompletionDetail, "jobType": j.JobType,
				"nextRoute": j.NextRoute, "waitFor": j.WaitFor, "availableAt": j.AvailableAt, "expiresAt": j.ExpiresAt, "leaseExpiresAt": j.LeaseExpiresAt,
				"cancelRequested": j.CancelRequested, "createdAt": j.CreatedAt, "archivedAt": j.ArchivedAt, "clientPayload": j.ClientPayload,
				"clientPayloadRevision": j.ClientPayloadRevision, "executionState": j.ExecutionState, "metadata": j.Metadata, "schemaHash": j.SchemaHash, "parentJobId": j.ParentJobID, "blockingWaitSummary": j.BlockingWaitSummary,
			})
		}
		return json.NewEncoder(w).Encode(struct {
			Jobs          []map[string]any `json:"jobs"`
			NextPageToken string           `json:"nextPageToken,omitempty"`
		}{rows, page.NextPageToken})
	}
	return projectwire.Encode(w, shapeForCLI(value), value)
}
