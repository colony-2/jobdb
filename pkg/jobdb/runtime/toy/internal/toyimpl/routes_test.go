package toyimpl

import (
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestExpiredLeaseActivatesAlternateRouteInSamePoll(t *testing.T) {
	now := time.Now().UTC()
	fallback := jobdb.Route{JobType: "fallback:job", TaskType: "fallback:task"}
	record := &jobRecord{
		route:          jobdb.Route{JobType: "primary:job", TaskType: "primary:task"},
		alternateRoute: &fallback, alternateAt: now.Add(-time.Second),
		leased: true, leaseID: "expired", leaseExpiresAt: now.Add(-time.Second),
		status: jobdb.JobStatusActive,
	}
	New().advanceRecordStateLocked("tenant", now, record)
	if record.leased || record.route != fallback || record.status != jobdb.JobStatusReady {
		t.Fatalf("expired lease did not release to fallback: leased=%v route=%+v status=%s", record.leased, record.route, record.status)
	}
}
