package remote

import (
	"context"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLegacyRuntimeOptionalEvents(t *testing.T) {
	var legacy jobdb.WorkflowRuntime = legacyRuntime{}
	_, e := jobdb.ListLeaseEvents(context.Background(), legacy, jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.ErrorIs(t, e, jobdb.ErrLeaseEventsUnsupported)
	_, e = jobdb.ListLeaseEvents(context.Background(), legacy, jobdb.ListLeaseEventsRequest{})
	require.ErrorIs(t, e, jobdb.ErrInvalidLeaseEventRequest)
	for _, backend := range []jobdb.WorkflowRuntime{legacy, toy.New()} {
		handler := NewServer(backend)
		for _, authorized := range []bool{true, false} {
			wrapped := handler
			if authorized {
				wrapped = celltest.Handler(handler)
			}
			s := httptest.NewServer(wrapped)
			client, e := New(s.URL, s.Client())
			require.NoError(t, e)
			_, e = client.ListLeaseEvents(context.Background(), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
			if authorized && backend == legacy {
				require.ErrorIs(t, e, jobdb.ErrLeaseEventsUnsupported)
			} else if !authorized {
				require.ErrorIs(t, e, jobdb.ErrCellForbidden)
			} else {
				require.NoError(t, e)
			}
			s.Close()
		}
	}
}

func TestLeaseEventsOldServerAndErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		want   error
	}{
		{404, "", jobdb.ErrLeaseEventsUnsupported}, {405, "", jobdb.ErrLeaseEventsUnsupported},
		{501, "lease_events_unsupported", jobdb.ErrLeaseEventsUnsupported},
		{404, "not_found", jobdb.ErrCellNotFound}, {403, "forbidden", jobdb.ErrCellForbidden},
		{400, "invalid_request", jobdb.ErrInvalidLeaseEventRequest},
		{400, "invalid_lease_event_page_token", jobdb.ErrInvalidLeaseEventPageToken},
		{403, "lease_permission_denied", jobdb.ErrLeasePermissionDenied},
		{401, "", nil}, {500, "", nil}, {404, "other_domain_error", nil},
	} {
		t.Run(fmt.Sprintf("%d/%s", tc.status, tc.code), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				if tc.code != "" {
					fmt.Fprintf(w, `{"code":%q}`, tc.code)
				} else {
					fmt.Fprint(w, "old server")
				}
			}))
			defer s.Close()
			client, e := New(s.URL, s.Client())
			require.NoError(t, e)
			_, e = client.ListLeaseEvents(context.Background(), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
			require.Error(t, e)
			if tc.want != nil {
				require.ErrorIs(t, e, tc.want)
			} else {
				require.NotErrorIs(t, e, jobdb.ErrLeaseEventsUnsupported)
			}
		})
	}
	s := httptest.NewServer(http.NotFoundHandler())
	client, e := New(s.URL, s.Client())
	require.NoError(t, e)
	s.Close()
	_, e = client.ListLeaseEvents(context.Background(), jobdb.ListLeaseEventsRequest{TenantID: "tenant"})
	require.Error(t, e)
	require.NotErrorIs(t, e, jobdb.ErrLeaseEventsUnsupported)
}
