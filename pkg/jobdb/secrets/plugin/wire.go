package secretplugin

import (
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsutil"
	pb "github.com/colony-2/jobdb/pkg/jobdb/secrets/plugin/proto/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func timestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
func readTime(t *timestamppb.Timestamp) (time.Time, error) {
	if t == nil {
		return time.Time{}, nil
	}
	if t.CheckValid() != nil {
		return time.Time{}, jobdb.ErrSecretResolutionFailed
	}
	return t.AsTime(), nil
}
func encodeRequest(r jobdb.SecretResolveRequest) *pb.ResolveRequest {
	return &pb.ResolveRequest{TenantId: r.TenantID, JobId: r.JobID, Principal: r.Principal, AppMetadataJson: r.AppMetadata, Names: r.Names,
		Lease: &pb.LeaseContext{LeaseId: r.Lease.LeaseID, WorkerId: r.Lease.WorkerID, LeaseExpiresAt: timestamp(r.Lease.LeaseExpiresAt), AuthorizedNotAfter: timestamp(r.Lease.AuthorizedNotAfter)}}
}
func decodeRequest(r *pb.ResolveRequest) (jobdb.SecretResolveRequest, error) {
	if r == nil {
		return jobdb.SecretResolveRequest{}, jobdb.ErrInvalidSecretRequest
	}
	v := jobdb.SecretResolveRequest{TenantID: r.TenantId, JobID: r.JobId, Principal: r.Principal, AppMetadata: r.AppMetadataJson, Names: r.Names}
	if l := r.Lease; l != nil {
		v.Lease.LeaseID = l.LeaseId
		v.Lease.WorkerID = l.WorkerId
		var err error
		v.Lease.LeaseExpiresAt, err = readTime(l.LeaseExpiresAt)
		if err != nil {
			return jobdb.SecretResolveRequest{}, jobdb.ErrInvalidSecretRequest
		}
		v.Lease.AuthorizedNotAfter, err = readTime(l.AuthorizedNotAfter)
		if err != nil {
			return jobdb.SecretResolveRequest{}, jobdb.ErrInvalidSecretRequest
		}
	}
	return v, nil
}
func encodeResult(r jobdb.SecretResolveResult) *pb.ResolveReply {
	out := &pb.ResolveResult{Secrets: map[string]*pb.SecretValue{}}
	for k, v := range r.Secrets {
		value := v.Value
		out.Secrets[k] = &pb.SecretValue{Value: &value, Version: v.Version, ExpiresAt: optionalTimestamp(v.ExpiresAt), LeaseBound: v.LeaseBound}
	}
	return &pb.ResolveReply{Outcome: &pb.ResolveReply_Result{Result: out}}
}
func optionalTimestamp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}
func decodeResult(r *pb.ResolveReply) (jobdb.SecretResolveResult, error) {
	if r == nil {
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
	}
	switch o := r.Outcome.(type) {
	case *pb.ResolveReply_Failure:
		return jobdb.SecretResolveResult{}, decodeError(o.Failure)
	case *pb.ResolveReply_Result:
		if o.Result == nil {
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
		}
		out := jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{}}
		for k, v := range o.Result.Secrets {
			if v == nil || v.Value == nil {
				return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
			}
			value := jobdb.SecretValue{Value: *v.Value, Version: v.Version, LeaseBound: v.LeaseBound}
			if v.ExpiresAt != nil {
				t, err := readTime(v.ExpiresAt)
				if err != nil {
					return jobdb.SecretResolveResult{}, err
				}
				value.ExpiresAt = &t
			}
			out.Secrets[k] = value
		}
		return out, nil
	default:
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
	}
}
func encodeError(err error) *pb.ResolveReply {
	code := pb.Failure_FAILURE_RESOLUTION_FAILED
	switch secretsutil.SafeError(err) {
	case jobdb.ErrSecretsUnavailable:
		code = pb.Failure_FAILURE_UNAVAILABLE
	case jobdb.ErrInvalidSecretRequest:
		code = pb.Failure_FAILURE_INVALID_REQUEST
	case jobdb.ErrSecretRequestTooLarge:
		code = pb.Failure_FAILURE_REQUEST_TOO_LARGE
	case jobdb.ErrNoSecretProvider:
		code = pb.Failure_FAILURE_NO_PROVIDER
	case jobdb.ErrExecutionLeaseLost:
		code = pb.Failure_FAILURE_LEASE_LOST
	case jobdb.ErrDevSecretsNotConfigured:
		code = pb.Failure_FAILURE_DEV_NOT_CONFIGURED
	}
	return &pb.ResolveReply{Outcome: &pb.ResolveReply_Failure{Failure: code}}
}
func decodeError(code pb.Failure) error {
	switch code {
	case pb.Failure_FAILURE_UNAVAILABLE:
		return jobdb.ErrSecretsUnavailable
	case pb.Failure_FAILURE_INVALID_REQUEST:
		return jobdb.ErrInvalidSecretRequest
	case pb.Failure_FAILURE_REQUEST_TOO_LARGE:
		return jobdb.ErrSecretRequestTooLarge
	case pb.Failure_FAILURE_NO_PROVIDER:
		return jobdb.ErrNoSecretProvider
	case pb.Failure_FAILURE_LEASE_LOST:
		return jobdb.ErrExecutionLeaseLost
	case pb.Failure_FAILURE_DEV_NOT_CONFIGURED:
		return jobdb.ErrDevSecretsNotConfigured
	default:
		return jobdb.ErrSecretResolutionFailed
	}
}
