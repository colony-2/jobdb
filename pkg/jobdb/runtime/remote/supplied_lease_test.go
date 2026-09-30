package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

func TestLeaseCapabilityBindingAndRedaction(t *testing.T) {
	ctx := context.Background()
	backend := toy.New()
	server := httptest.NewServer(NewServer(backend))
	defer server.Close()
	client, err := New(server.URL, server.Client())
	require.NoError(t, err)
	handle, err := client.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
	require.NoError(t, err)
	lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "owner", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
	require.NoError(t, err)
	cap, err := ExportLease(lease)
	require.NoError(t, err)
	secret := cap.wire.Token
	for _, value := range []any{cap, &cap, lease} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
			require.NotContains(t, fmt.Sprintf(format, value), secret)
		}
		raw, err := json.Marshal(value)
		require.NoError(t, err)
		require.NotContains(t, string(raw), secret)
	}
	for _, field := range []string{"token", "tenant", "job", "lease", "version"} {
		t.Run(field, func(t *testing.T) {
			edited := cap
			switch field {
			case "token":
				edited.wire.Token += "tampered"
			case "tenant":
				edited.wire.TenantID = "other"
			case "job":
				edited.wire.JobID = "other"
			case "lease":
				edited.wire.LeaseID = "other"
			case "version":
				edited.wire.Version = 2
			}
			_, err := client.ImportLease(ctx, edited)
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			require.NotContains(t, fmt.Sprint(err), secret)
		})
	}
	raw, err := cap.Encode()
	require.NoError(t, err)
	for _, bad := range [][]byte{[]byte(secret), append(raw, []byte(` {}`)...), []byte(strings.TrimSuffix(string(raw), "}") + `,"route":{"jobType":"edited"}}`)} {
		_, err := DecodeLeaseCapability(bad)
		require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
		require.NotContains(t, fmt.Sprint(err), secret)
	}
	require.NoError(t, backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: handle.JobKey}))
	_, err = client.ImportLease(ctx, cap)
	require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
}

func TestImportFailsClosedOnOldOrFailingServer(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			cap := LeaseCapability{wire: leaseCapabilityWire{Version: 1, TenantID: "tenant", JobID: "job", LeaseID: "lease", Token: "sensitive-bearer"}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.True(t, strings.HasSuffix(r.URL.Path, "/keepalive"))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				if status == http.StatusOK {
					fmt.Fprint(w, `{"leaseToken":"replacement"}`)
				} else {
					fmt.Fprint(w, cap.wire.Token)
				}
			}))
			defer server.Close()
			client, err := New(server.URL, server.Client())
			require.NoError(t, err)
			_, err = client.ImportLease(context.Background(), cap)
			require.Error(t, err)
			require.NotContains(t, fmt.Sprint(err), cap.wire.Token)
			if status == http.StatusOK {
				require.ErrorIs(t, err, jobdb.ErrLeaseRenewalUnsupported)
			} else {
				var transport *LeaseTransportError
				require.ErrorAs(t, err, &transport)
				require.False(t, errors.Is(err, jobdb.ErrExecutionLeaseLost))
			}
		})
	}
}

func TestUnexpiredCapabilityCannotAppendAfterCancellation(t *testing.T) {
	for _, kind := range []string{"toy", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			var backend jobdb.WorkflowRuntime = toy.New()
			if kind == "sqlite" {
				embedded, err := sqliteruntime.StartEmbeddedRuntime(ctx)
				require.NoError(t, err)
				t.Cleanup(embedded.Shutdown)
				backend = embedded.Runtime
			}
			server := httptest.NewServer(NewServer(backend))
			defer server.Close()
			client, err := New(server.URL, server.Client())
			require.NoError(t, err)
			handle, err := client.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobType: "job", Data: jobdb.NewTaskDataOrPanic(1)}})
			require.NoError(t, err)
			lease, err := client.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, WorkerID: "owner", Routes: []jobdb.Route{{JobType: "job"}}, LeaseDuration: time.Minute})
			require.NoError(t, err)
			require.NoError(t, backend.CancelJob(ctx, jobdb.CancelJobRequest{JobKey: handle.JobKey}))
			err = client.PutChapter(ctx, jobdb.PutChapterRequest{LeaseID: lease.LeaseID(), LeaseToken: leaseTokenForTest(lease), Ref: jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 1}, Chapter: jobdb.Chapter{Ordinal: 1, TaskType: "job", CreatedAt: time.Now(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}}}}})
			require.ErrorIs(t, err, jobdb.ErrExecutionLeaseLost)
			chapters, err := backend.ListChapters(ctx, jobdb.ListChaptersRequest{JobKey: handle.JobKey})
			require.NoError(t, err)
			require.Len(t, chapters, 1)
		})
	}
}
