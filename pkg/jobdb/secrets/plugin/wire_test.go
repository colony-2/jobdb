package secretplugin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	pb "github.com/colony-2/jobdb/pkg/jobdb/secrets/plugin/proto/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWireRejectsMissingValuesAndInvalidTimestamps(t *testing.T) {
	empty := ""
	for _, reply := range []*pb.ResolveReply{
		nil, {},
		{Outcome: &pb.ResolveReply_Failure{Failure: pb.Failure(999)}},
		{Outcome: &pb.ResolveReply_Result{}},
		{Outcome: &pb.ResolveReply_Result{Result: &pb.ResolveResult{Secrets: map[string]*pb.SecretValue{"token": nil}}}},
		{Outcome: &pb.ResolveReply_Result{Result: &pb.ResolveResult{Secrets: map[string]*pb.SecretValue{"token": {}}}}},
		{Outcome: &pb.ResolveReply_Result{Result: &pb.ResolveResult{Secrets: map[string]*pb.SecretValue{"token": {Value: &empty, ExpiresAt: &timestamppb.Timestamp{Seconds: 999999999999999}}}}}},
	} {
		r, err := decodeResult(reply)
		require.ErrorIs(t, err, jobdb.ErrSecretResolutionFailed)
		require.Empty(t, r.Secrets)
	}
	r, err := decodeResult(encodeResult(jobdb.SecretResolveResult{Secrets: map[string]jobdb.SecretValue{"token": {Value: ""}}}))
	require.NoError(t, err)
	require.Contains(t, r.Secrets, "token")
	require.Empty(t, r.Secrets["token"].Value)
	_, err = decodeRequest(&pb.ResolveRequest{Lease: &pb.LeaseContext{AuthorizedNotAfter: &timestamppb.Timestamp{Nanos: -1}}})
	require.ErrorIs(t, err, jobdb.ErrInvalidSecretRequest)
}

func TestPluginBoundsFullRequestEnvelope(t *testing.T) {
	p := fixture(t)
	req := jobdb.SecretResolveRequest{Names: []string{"echo"}, AppMetadata: json.RawMessage(`"` + strings.Repeat("x", DefaultMaxMessageBytes) + `"`)}
	r, err := p.ResolveSecrets(context.Background(), req)
	require.ErrorIs(t, err, jobdb.ErrSecretRequestTooLarge)
	require.Empty(t, r.Secrets)
}

func TestOptionalProvenanceAcceptsOlderHTTPRepresentations(t *testing.T) {
	for _, body := range []string{`{"secrets":{"token":{"value":""}}}`, `{"secrets":{"token":{"value":"","providerId":"local"}}}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}))
		client, err := remote.New(server.URL, server.Client())
		require.NoError(t, err)
		r, err := client.ResolveSecrets(context.Background(), jobdb.ResolveSecretsRequest{
			JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, LeaseID: "lease", LeaseToken: "test-capability", Names: []string{"token"},
		})
		server.Close()
		require.NoError(t, err)
		require.Contains(t, r.Secrets, "token")
		if strings.Contains(body, "providerId") {
			require.Equal(t, "local", r.Secrets["token"].ProviderID)
		} else {
			require.Empty(t, r.Secrets["token"].ProviderID)
		}
	}
}
