// Package secretplugin serves and consumes local HashiCorp go-plugin secret providers.
package secretplugin

import (
	"context"
	"encoding/json"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/secretsutil"
	pb "github.com/colony-2/jobdb/pkg/jobdb/secrets/plugin/proto/v1"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const ProtocolVersion = 1
const DefaultMaxMessageBytes = 8 << 20
const pluginName = "secret_provider"

func handshake() plugin.HandshakeConfig {
	return plugin.HandshakeConfig{MagicCookieKey: "JOBDB_SECRET_PLUGIN", MagicCookieValue: "jobdb-secret-provider"}
}

// ServeOptions bounds the plugin independently of limits enforced by its host.
type ServeOptions struct {
	Limits                         jobdb.SecretLimits
	MaxMessageBytes, MaxConcurrent int
}

func (o ServeOptions) defaults() ServeOptions {
	o.Limits = o.Limits.WithDefaults()
	if o.MaxMessageBytes <= 0 {
		o.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if o.MaxConcurrent <= 0 {
		o.MaxConcurrent = 32
	}
	return o
}

// Serve takes over the plugin process. Initialize its backend before calling it.
func Serve(provider jobdb.SecretProvider, options ...ServeOptions) {
	o := ServeOptions{}
	if len(options) > 0 {
		o = options[0]
	}
	o = o.defaults()
	plugin.Serve(&plugin.ServeConfig{HandshakeConfig: handshake(), Logger: hclog.NewNullLogger(),
		VersionedPlugins: map[int]plugin.PluginSet{ProtocolVersion: {pluginName: &adapter{provider: provider, options: o}}},
		GRPCServer: func(opts []grpc.ServerOption) *grpc.Server {
			return grpc.NewServer(append(opts, grpc.MaxRecvMsgSize(o.MaxMessageBytes), grpc.MaxSendMsgSize(o.MaxMessageBytes))...)
		}})
}

type adapter struct {
	plugin.NetRPCUnsupportedPlugin
	provider jobdb.SecretProvider
	options  ServeOptions
}

func (a *adapter) GRPCServer(_ *plugin.GRPCBroker, s *grpc.Server) error {
	o := a.options.defaults()
	pb.RegisterSecretProviderServer(s, &rpcServer{provider: a.provider, options: o, slots: make(chan struct{}, o.MaxConcurrent)})
	return nil
}
func (a *adapter) GRPCClient(ctx context.Context, _ *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	return &rpcProvider{client: pb.NewSecretProviderClient(c), lifetime: ctx, options: a.options.defaults()}, nil
}

type rpcServer struct {
	pb.UnimplementedSecretProviderServer
	provider jobdb.SecretProvider
	options  ServeOptions
	slots    chan struct{}
}

func (s *rpcServer) ResolveSecrets(ctx context.Context, wire *pb.ResolveRequest) (reply *pb.ResolveReply, err error) {
	// A provider panic must not serialize credential-bearing panic text.
	defer func() {
		if recover() != nil {
			reply = encodeError(jobdb.ErrSecretResolutionFailed)
			err = nil
		}
	}()
	req, e := decodeRequest(wire)
	if e != nil {
		return encodeError(e), nil
	}
	if e = validateRequest(req, s.options.Limits); e != nil {
		return encodeError(e), nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.options.Limits.LookupTimeout)
	defer cancel()
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return encodeError(ctx.Err()), nil
	}
	defer func() { <-s.slots }()
	if ctx.Err() != nil {
		return encodeError(ctx.Err()), nil
	}
	if s.provider == nil {
		return encodeError(jobdb.ErrNoSecretProvider), nil
	}
	r, e := s.provider.ResolveSecrets(ctx, req)
	if ctx.Err() != nil {
		return encodeError(ctx.Err()), nil
	}
	if e != nil {
		return encodeError(e), nil
	}
	r, e = secretsutil.Result(r, req.Names, s.options.Limits.MaxResponseBytes)
	if e == nil {
		e = secretsutil.LeaseBounds(r, req.Lease)
	}
	if e != nil {
		return encodeError(e), nil
	}
	return encodeResult(r), nil
}

func validateRequest(r jobdb.SecretResolveRequest, limits jobdb.SecretLimits) error {
	if err := secretsutil.Names(r.Names, limits.MaxRequestBytes); err != nil {
		return err
	}
	if len(r.AppMetadata) > 0 && !json.Valid(r.AppMetadata) {
		return jobdb.ErrInvalidSecretRequest
	}
	w := encodeRequest(r)
	if _, err := decodeRequest(w); err != nil {
		return err
	}
	return nil
}

type rpcProvider struct {
	client   pb.SecretProviderClient
	lifetime context.Context
	options  ServeOptions
}

func (p *rpcProvider) ResolveSecrets(ctx context.Context, req jobdb.SecretResolveRequest) (jobdb.SecretResolveResult, error) {
	if err := validateRequest(req, p.options.Limits); err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	wire := encodeRequest(req)
	if proto.Size(wire) > p.options.MaxMessageBytes {
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretRequestTooLarge
	}
	ctx, cancel := context.WithTimeout(ctx, p.options.Limits.LookupTimeout)
	defer cancel()
	stop := context.AfterFunc(p.lifetime, cancel)
	defer stop()
	r, err := p.client.ResolveSecrets(ctx, wire)
	if err != nil {
		switch status.Code(err) {
		case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretsUnavailable
		default:
			return jobdb.SecretResolveResult{}, jobdb.ErrSecretResolutionFailed
		}
	}
	result, err := decodeResult(r)
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	result, err = secretsutil.Result(result, req.Names, p.options.Limits.MaxResponseBytes)
	if err == nil {
		err = secretsutil.LeaseBounds(result, req.Lease)
	}
	if err != nil {
		return jobdb.SecretResolveResult{}, err
	}
	if ctx.Err() != nil {
		return jobdb.SecretResolveResult{}, jobdb.ErrSecretsUnavailable
	}
	return result, nil
}
