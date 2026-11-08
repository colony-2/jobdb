package runtimecore_test

import (
	"context"
	"errors"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
	"testing"
)

type eventTestScheduler struct {
	runtimecore.Scheduler
	calls int
	req   jobdb.ListLeaseEventsRequest
	err   error
}

func (s *eventTestScheduler) ListLeaseEvents(_ context.Context, q jobdb.ListLeaseEventsRequest) (jobdb.ListLeaseEventsResponse, error) {
	s.calls++
	s.req = q
	return jobdb.ListLeaseEventsResponse{Events: []jobdb.LeaseEvent{{EventID: "5"}}, NextPageToken: "opaque"}, s.err
}
func TestLeaseEventCoreForwardsOnlyScheduler(t *testing.T) {
	s := &eventTestScheduler{}
	rt, e := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: s, Chapters: noSummaryHistory{}, Schemas: readTestSchemas{}})
	require.NoError(t, e)
	page, e := rt.ListLeaseEvents(celltest.Context(context.Background()), jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"b", "a", "b"}})
	require.NoError(t, e)
	require.Equal(t, 1, s.calls)
	require.Equal(t, []string{"a", "b"}, s.req.JobIDs)
	require.Equal(t, 100, s.req.PageSize)
	require.Equal(t, "opaque", page.NextPageToken)
	_, e = rt.ListLeaseEvents(context.Background(), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.ErrorIs(t, e, jobdb.ErrCellForbidden)
	require.Equal(t, 1, s.calls)
	_, e = rt.ListLeaseEvents(celltest.Context(context.Background()), jobdb.ListLeaseEventsRequest{TenantID: "tenant", PageSize: 201})
	require.ErrorIs(t, e, jobdb.ErrInvalidLeaseEventRequest)
	require.Equal(t, 1, s.calls)
	s.err = errors.New("backend failure")
	_, e = rt.ListLeaseEvents(celltest.Context(context.Background()), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.ErrorIs(t, e, s.err)
}

func TestLeaseEventCoreLegacyScheduler(t *testing.T) {
	// readTestScheduler has no optional event reader.
	rt, e := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: readTestScheduler{}, Chapters: noSummaryHistory{}, Schemas: readTestSchemas{}})
	require.NoError(t, e)
	_, e = rt.ListLeaseEvents(celltest.Context(context.Background()), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.ErrorIs(t, e, jobdb.ErrLeaseEventsUnsupported)
	_, e = rt.ListLeaseEvents(context.Background(), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.ErrorIs(t, e, jobdb.ErrCellForbidden)
	_, e = rt.ListLeaseEvents(celltest.Context(context.Background()), jobdb.ListLeaseEventsRequest{})
	require.ErrorIs(t, e, jobdb.ErrInvalidLeaseEventRequest)
}
