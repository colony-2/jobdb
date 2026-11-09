package jobdb

import (
	"context"
	"encoding/json"
	"errors"
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

type historyProbe struct {
	WorkflowRuntime
	req   ListLeaseEventsRequest
	calls int
	err   error
}

func (r *historyProbe) ListLeaseEvents(_ context.Context, req ListLeaseEventsRequest) (ListLeaseEventsResponse, error) {
	r.calls++
	r.req = req
	return ListLeaseEventsResponse{Events: []LeaseEvent{{EventID: "actual"}}, NextPageToken: "next"}, r.err
}
func TestLeaseEventHelperMandatoryForwarding(t *testing.T) {
	r := &historyProbe{}
	var port WorkflowRuntime = r
	_, err := ListLeaseEvents(context.Background(), port, ListLeaseEventsRequest{})
	require.ErrorIs(t, err, ErrInvalidLeaseEventRequest)
	require.Zero(t, r.calls)
	req := ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"b", "a", "b"}, PageToken: "page", PageSize: 7}
	out, err := ListLeaseEvents(context.Background(), port, req)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, r.req.JobIDs)
	require.Equal(t, "page", r.req.PageToken)
	require.Equal(t, 7, r.req.PageSize)
	require.Equal(t, "actual", out.Events[0].EventID)
	require.Equal(t, "next", out.NextPageToken)
	r.err = errors.New("backend failed")
	out, err = ListLeaseEvents(context.Background(), port, req)
	require.ErrorIs(t, err, r.err)
	require.Equal(t, "actual", out.Events[0].EventID)
}
