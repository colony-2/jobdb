package toyimpl

import (
	"fmt"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// Caller holds the record mutex. Protection is stored explicitly; absence of
// legacy execution chapters cannot turn an ordinary failed job into this kind.
func validateProtectedToyRecord(record *jobRecord) error {
	if !record.recordKind.IsTerminalRecord() || record.goalOrigin || record.key.Validate() != nil || record.goalID == "" ||
		record.terminalFailure == nil || record.terminalFailure.GoalID != record.goalID ||
		record.status != jobdb.JobStatusCompleted || record.completionStatus != "failed" ||
		record.archived == nil || record.jobType != jobdb.GoalFailureJobType ||
		record.route.JobType != "" || record.route.TaskType != "" || record.leased || record.alternateRoute != nil ||
		len(record.waitFor) != 0 || record.dependencyWaitID != "" {
		return fmt.Errorf("%w: protected toy record must be archived without executable route, lease or wait", jobdb.ErrInvalidJobRecord)
	}
	if record.goalClosure != nil && (record.goalClosure.state != jobdb.GoalFailed ||
		!record.goalClosure.at.Equal(record.terminalFailure.FailedAt) || record.goalClosure.winner != "") {
		return fmt.Errorf("%w: protected closure differs from retained failure", jobdb.ErrInvalidJobRecord)
	}
	return nil
}
