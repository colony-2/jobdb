package runtimetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

type expiryValidatorRuntime struct {
	jobdb.WorkflowRuntime
	validate func(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error)
}

func (r expiryValidatorRuntime) ValidateExecutionLeaseByID(ctx context.Context, key jobdb.JobKey, lease, worker string) (jobdb.LeaseValidity, error) {
	return r.validate(ctx, key, lease, worker)
}

func TestLeaseEventExpiryOracle(t *testing.T) {
	backend := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	key := jobdb.JobKey{TenantId: "tenant", JobId: "job"}
	sentinel := errors.New("backend unavailable")
	for _, tc := range []struct {
		name                 string
		native, missing      bool
		safe, event, backend time.Time
		validatorErr         error
		want                 string
	}{
		{name: "native", native: true, safe: backend, event: backend, backend: backend},
		{name: "remote earlier deadline", safe: backend.Add(-time.Second), event: backend, backend: backend},
		{name: "remote equal deadline", safe: backend, event: backend, backend: backend},
		{name: "wrong event despite earlier deadline", safe: backend.Add(-time.Second), event: backend.Add(time.Second), backend: backend, want: "differs from backend"},
		{name: "native earlier deadline", native: true, safe: backend.Add(-time.Second), event: backend, backend: backend, want: "native deadline differs"},
		{name: "late safe deadline", safe: backend.Add(time.Second), event: backend, backend: backend, want: "safe deadline"},
		{name: "zero safe deadline", event: backend, backend: backend, want: "safe deadline"},
		{name: "zero backend expiry", want: "backend expiry is zero"},
		{name: "missing validator", missing: true, want: "backend validator required"},
		{name: "hook error", validatorErr: sentinel, want: "backend validation"},
		{name: "native error", native: true, validatorErr: sentinel, want: "backend validation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), struct{}{}, "authority")
			calls := 0
			validate := func(gotCtx context.Context, gotKey jobdb.JobKey, lease, worker string) (jobdb.LeaseValidity, error) {
				calls++
				require.Equal(t, ctx, gotCtx)
				require.Equal(t, key, gotKey)
				require.Equal(t, "lease", lease)
				require.Equal(t, "worker", worker)
				return jobdb.LeaseValidity{ExpiresAt: tc.backend}, tc.validatorErr
			}
			f := Fixture{}
			if tc.native {
				f.Runtime = expiryValidatorRuntime{validate: validate}
			} else if !tc.missing {
				// A supplied hook takes precedence over direct runtime validation.
				f.Runtime = expiryValidatorRuntime{validate: func(context.Context, jobdb.JobKey, string, string) (jobdb.LeaseValidity, error) {
					t.Fatal("unexpected fallback")
					return jobdb.LeaseValidity{}, nil
				}}
				f.LeaseEventLeaseValidity = validate
			}
			err := leaseEventExpiry(ctx, f, key, "lease", "worker", tc.safe, tc.event)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
			if tc.validatorErr != nil {
				require.ErrorIs(t, err, sentinel)
			}
			if tc.missing {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
}
