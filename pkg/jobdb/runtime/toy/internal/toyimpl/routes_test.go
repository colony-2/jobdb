package toyimpl

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestPrimaryClaimPreservesFutureAlternate(t *testing.T) {
	for _, method := range []string{"poll", "get"} {
		t.Run(method, func(t *testing.T) {
			rt := New()
			key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
			primary := jobdb.Route{JobType: "job", TaskType: "primary"}
			fallback := jobdb.Route{JobType: "job"}
			record := &jobRecord{
				route: primary, alternateRoute: &fallback,
				alternateAt: time.Now().Add(time.Hour), status: jobdb.JobStatusReady,
			}
			rt.engine.jobRecords[key] = record
			claim := func(route jobdb.Route) jobdb.ExecutionLease {
				t.Helper()
				if method == "poll" {
					leases, err := rt.PollWork(context.Background(), jobdb.PollWorkRequest{
						TenantId: key.TenantId, Routes: []jobdb.Route{route}, WorkerID: "worker",
					})
					require.NoError(t, err)
					require.Len(t, leases, 1)
					return leases[0]
				}
				lease, err := rt.GetJobLease(context.Background(), jobdb.GetJobLeaseRequest{
					JobKey: key, Routes: []jobdb.Route{route}, WorkerID: "worker",
				})
				require.NoError(t, err)
				require.NotNil(t, lease)
				return lease
			}
			require.Equal(t, primary, claim(primary).Route())
			require.NotNil(t, record.alternateRoute)
			require.Equal(t, fallback, *record.alternateRoute)

			// Simulate a crashed primary and a now-due alternate without sleeping.
			record.leaseExpiresAt = time.Now().Add(-time.Second)
			record.alternateAt = time.Now().Add(-time.Second)
			require.Equal(t, fallback, claim(fallback).Route())
			require.Equal(t, fallback, record.route)
			require.Nil(t, record.alternateRoute)
		})
	}
}

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
	if record.leased || effectiveRoute(record, now) != fallback || record.status != jobdb.JobStatusReady {
		t.Fatalf("expired lease did not release to fallback: leased=%v route=%+v status=%s", record.leased, record.route, record.status)
	}
}
