// Package configurationbroker enforces live lease authority around configuration reads.
package configurationbroker

import (
	"context"
	"errors"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
)

type Access struct {
	JobKey            jobdb.JobKey
	LeaseID, WorkerID string
	// TokenExpiry also bounds embedded requests to the issued handle's expiry.
	TokenExpiry time.Time
}

func FromRequest(ctx context.Context, req jobdb.GetConfigurationRequest) (Access, error) {
	authorized, err := leaseauth.Authorize(ctx, req.JobKey, req.LeaseID)
	claims, _ := leaseauth.ClaimsFromContext(ctx)
	if !authorized || err != nil || claims.WorkerID == "" {
		return Access{}, jobdb.ErrExecutionLeaseLost
	}
	return Access{req.JobKey, req.LeaseID, claims.WorkerID, claims.ExpiresAt}, nil
}
func Read(ctx context.Context, service *configuration.Service, access Access, validate func(context.Context) error) (jobdb.ConfigurationSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if access.JobKey.Validate() != nil || access.LeaseID == "" || access.WorkerID == "" {
		return jobdb.ConfigurationSnapshot{}, jobdb.ErrExecutionLeaseLost
	}
	ctx, cancel := context.WithTimeout(ctx, service.ConfigurationLimits().ReadTimeout)
	defer cancel()
	if !access.TokenExpiry.IsZero() {
		var c context.CancelFunc
		ctx, c = context.WithDeadline(ctx, access.TokenExpiry)
		defer c()
	}
	check := func() error {
		if !access.TokenExpiry.IsZero() && !time.Now().Before(access.TokenExpiry) {
			return jobdb.ErrExecutionLeaseLost
		}
		if err := ctx.Err(); err != nil {
			return configuration.SafeError(err)
		}
		err := validate(ctx)
		if errors.Is(err, jobdb.ErrJobNotFound) {
			return jobdb.ErrExecutionLeaseLost
		}
		return configuration.SafeError(err)
	}
	if err := check(); err != nil {
		return jobdb.ConfigurationSnapshot{}, err
	}
	result, err := service.GetTenantConfiguration(ctx, access.JobKey.TenantId)
	if e := check(); e != nil {
		return jobdb.ConfigurationSnapshot{}, e
	}
	if ctx.Err() != nil {
		return jobdb.ConfigurationSnapshot{}, configuration.SafeError(ctx.Err())
	}
	if err != nil {
		return jobdb.ConfigurationSnapshot{}, configuration.SafeError(err)
	}
	return result, nil
}
