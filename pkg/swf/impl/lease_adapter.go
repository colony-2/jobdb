package impl

import (
	"context"
	"database/sql"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
)

type pgwfLeaseAdapter struct {
	lease *pgwf.Lease
	udb   *sql.DB
}

func newPgwfLeaseAdapter(lease *pgwf.Lease, udb *sql.DB) *pgwfLeaseAdapter {
	return &pgwfLeaseAdapter{lease: lease, udb: udb}
}

func (l *pgwfLeaseAdapter) KeepAlive(ctx context.Context) error {
	if l == nil || l.lease == nil || l.udb == nil {
		return nil
	}
	_ = l.lease.WithKeepAlive(l.udb)
	return nil
}

func (l *pgwfLeaseAdapter) StopKeepAlive() {
	if l == nil || l.lease == nil {
		return
	}
	stopLeaseKeepAlive(l.lease)
}

func (l *pgwfLeaseAdapter) CompleteWithStatus(ctx context.Context, status pgwf.CompletionStatus, completionDetail string) error {
	if l == nil || l.lease == nil || l.udb == nil {
		return nil
	}
	return l.lease.CompleteWithStatus(ctx, l.udb, status, completionDetail)
}

func (l *pgwfLeaseAdapter) Reschedule(ctx context.Context, deps pgwf.JobDependencies, payload any) error {
	if l == nil || l.lease == nil || l.udb == nil {
		return nil
	}
	return l.lease.Reschedule(ctx, l.udb, deps, payload)
}

func (l *pgwfLeaseAdapter) NextNeed() pgwf.Capability {
	if l == nil || l.lease == nil {
		return ""
	}
	return l.lease.NextNeed()
}

func (l *pgwfLeaseAdapter) Payload() []byte {
	if l == nil || l.lease == nil {
		return nil
	}
	return l.lease.Payload()
}

// PgwfLease exposes the underlying pgwf lease for internal tests.
func (l *pgwfLeaseAdapter) PgwfLease() *pgwf.Lease {
	if l == nil {
		return nil
	}
	return l.lease
}
