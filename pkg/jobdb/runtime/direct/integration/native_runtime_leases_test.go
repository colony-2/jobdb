package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/internal/runtimeadapter"
	"github.com/colony-2/pgjobdb/pkg/pgjobdb"
)

func TestNativeSchedulerAdapterLeasesTypedWork(t *testing.T) {
	runDatabaseTest(t, func(ctx context.Context, db *sql.DB) {
		_, err := pgjobdb.SubmitJob(ctx, db, pgjobdb.SubmitJobRequest{
			TenantID: "tenant", JobID: "job", WorkerID: "submitter", JobType: "collect",
			LeasePayload: json.RawMessage(`{}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		adapter := runtimeadapter.Scheduler{DB: db}
		selector := runtimecore.WorkSelector{JobTypes: []string{"collect"}}
		leased, err := adapter.AcquireWork(ctx, runtimecore.WorkRequest{
			TenantId: "tenant", WorkerID: "worker", Selector: selector,
			Limit: 2, LeaseDuration: time.Minute,
		})
		if err != nil || len(leased) != 1 || leased[0].WorkKind != runtimecore.WorkKindJob ||
			!leased[0].LeasePayloadVisible || string(leased[0].LeasePayload) != `{}` {
			t.Fatalf("typed work = %+v, %v", leased, err)
		}
		identity := leased[0].Identity
		validated, err := adapter.ValidateLease(ctx, identity)
		if err != nil || validated.Identity.LeaseID != identity.LeaseID {
			t.Fatalf("validated lease = %+v, %v", validated, err)
		}
		wrong := identity
		wrong.WorkerID = "other"
		if _, err := adapter.ValidateLease(ctx, wrong); !errors.Is(err, jobdb.ErrExecutionLeaseLost) {
			t.Fatalf("wrong owner validation = %v", err)
		}
		renewed, err := adapter.KeepAliveLease(ctx, runtimecore.LeaseMutation{
			Identity: identity, Duration: time.Minute,
		})
		if err != nil || !renewed.Identity.ExpiresAt.After(identity.ExpiresAt) {
			t.Fatalf("renewed lease = %+v, %v", renewed, err)
		}
		if _, err := adapter.CompleteLease(ctx, runtimecore.CompletionMutation{
			Identity: identity, Status: "success",
		}); err != nil {
			t.Fatalf("complete leased job: %v", err)
		}
		if _, err := adapter.ValidateLease(ctx, identity); !errors.Is(err, jobdb.ErrExecutionLeaseLost) {
			t.Fatalf("completed lease validation = %v", err)
		}
	})
}
