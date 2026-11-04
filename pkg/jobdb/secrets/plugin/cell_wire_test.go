package secretplugin

import (
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	pb "github.com/colony-2/jobdb/pkg/jobdb/secrets/plugin/proto/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestCellWireCompatibility(t *testing.T) {
	// Model the previous protocol-v1 reader, which has no cell message or field.
	// Discard unknown fields to verify it can consume a new host's request and
	// that all previously supported fields survive unchanged.
	file := protodesc.ToFileDescriptorProto(pb.File_jobdb_secrets_plugin_v1_provider_proto)
	for i, message := range file.MessageType {
		if message.GetName() == "LeaseContext" {
			for j, field := range message.Field {
				if field.GetName() == "cell" {
					message.Field = append(message.Field[:j], message.Field[j+1:]...)
					break
				}
			}
		}
		if message.GetName() == "CellContext" {
			file.MessageType = append(file.MessageType[:i], file.MessageType[i+1:]...)
			break
		}
	}
	legacyFile, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	require.NoError(t, err)
	legacy := dynamicpb.NewMessage(legacyFile.Messages().ByName("ResolveRequest"))
	principal := "subject"
	expiry := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	req := jobdb.SecretResolveRequest{
		TenantID: "tenant", JobID: "job", Principal: &principal,
		AppMetadata: []byte(`{"team":"example"}`), Names: []string{"token"},
		Lease: jobdb.SecretLeaseContext{
			Cell:    jobdb.ResolvedCell{CellID: "cell-id", Cell: "example", CellRepo: "github.com/acme/example"},
			LeaseID: "lease", WorkerID: "worker", LeaseExpiresAt: expiry, AuthorizedNotAfter: expiry,
		},
	}
	raw, err := proto.Marshal(encodeRequest(req))
	require.NoError(t, err)
	require.NoError(t, (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, legacy))

	// A new reader also accepts an older host's request with no cell context.
	legacyRaw, err := proto.Marshal(legacy)
	require.NoError(t, err)
	var wire pb.ResolveRequest
	require.NoError(t, proto.Unmarshal(legacyRaw, &wire))
	require.Nil(t, wire.Lease.Cell)
	got, err := decodeRequest(&wire)
	require.NoError(t, err)
	req.Lease.Cell = jobdb.ResolvedCell{}
	require.Equal(t, req, got)
	require.Nil(t, encodeRequest(got).Lease.Cell)
}

func TestCellWireMissingLease(t *testing.T) {
	got, err := decodeRequest(&pb.ResolveRequest{TenantId: "tenant", JobId: "job"})
	require.NoError(t, err)
	require.Equal(t, jobdb.SecretLeaseContext{}, got.Lease)
}
