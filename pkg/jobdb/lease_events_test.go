package jobdb

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestLeaseEventNormalization(t *testing.T) {
	now := time.Now()
	later := now.Add(time.Second)
	for _, q := range []ListLeaseEventsRequest{
		{}, {TenantID: "t", JobIDs: []string{}}, {TenantID: "t", JobIDs: []string{" "}}, {TenantID: "t", CellNames: []string{"test"}, CellIDs: []string{"id"}}, {TenantID: "t", PageSize: -1}, {TenantID: "t", PageSize: 201}, {TenantID: "t", EventTypes: []LeaseEventType{"other"}}, {TenantID: "t", Routes: []Route{{}}}, {TenantID: "t", OccurredAfter: &later, OccurredBefore: &now}, {TenantID: "t", OccurredAfter: &now, OccurredBefore: &now},
	} {
		_, e := NormalizeLeaseEventRequest(q)
		require.ErrorIs(t, e, ErrInvalidLeaseEventRequest)
	}
	for _, field := range []string{"cellNames", "cellIds", "jobIds", "goalIds", "workerIds", "leaseIds", "eventTypes", "jobTypes", "routes"} {
		for _, value := range []string{"null", "[]", "[ ]"} {
			var q ListLeaseEventsRequest
			e := json.Unmarshal([]byte(`{"`+field+`":`+value+`}`), &q)
			if e == nil {
				q.TenantID = "t"
				_, e = NormalizeLeaseEventRequest(q)
			}
			require.Error(t, e, field+value)
		}
	}
	for _, raw := range []string{`null`, `{"unknown":1}`, `{"tenantId":"other"}`, `{"occurredAfter":"bad"}`, `{"routes":[{"jobType":"j","other":1}]}`, `{"pageSize":0}`, `{"pageSize":null}`, `{"pageSize":201}`} {
		var q ListLeaseEventsRequest
		require.Error(t, json.Unmarshal([]byte(raw), &q), raw)
	}
	q := ListLeaseEventsRequest{TenantID: "t", JobIDs: []string{"b", "a", "b"}, Routes: []Route{{JobType: "x"}, {JobType: "x"}}}
	n, e := NormalizeLeaseEventRequest(q)
	require.NoError(t, e)
	require.Equal(t, []string{"a", "b"}, n.JobIDs)
	require.Equal(t, []string{"b", "a", "b"}, q.JobIDs)
	require.Equal(t, 100, n.PageSize)
	require.Len(t, n.Routes, 1)
	_, e = NormalizeLeaseEventRequest(ListLeaseEventsRequest{TenantID: "t", JobIDs: strings.Fields(strings.Repeat("a ", 101))})
	require.ErrorIs(t, e, ErrInvalidLeaseEventRequest)
}
