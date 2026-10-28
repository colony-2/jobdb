package runtimecodec

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/executionmeta"
	pb "github.com/colony-2/jobdb/pkg/internal/storagepb/v1"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestTypedExecutionStorageAndReconstruction(t *testing.T) {
	prefix := jobdb.ExecutionRecord{Version: 4, Kind: "scope", ID: "scope", Descriptor: json.RawMessage(`{"retry":{"maximum_attempts":3},"total_timeout":9007199254740993}`)}
	prefixJSON, err := json.Marshal([]jobdb.ExecutionRecord{prefix})
	require.NoError(t, err)
	metadata, err := json.Marshal(map[string]any{executionmeta.Key: string(prefixJSON), "ordinary": "metadata"})
	require.NoError(t, err)
	r := jobdb.ExecutionRecord{Version: 4, Kind: "result", ID: "scope/task", At: time.Unix(1, 2).UTC(), Output: &jobdb.ExecutionSnapshot{Data: json.RawMessage(`{"n":9007199254740993,"data":"application"}`), Artifacts: []jobdb.ExecutionArtifact{{Name: "logical", Key: &jobdb.ArtifactKey{JobId: "ancestor", TaskOrdinal: 5, Name: "stored", SizeBytes: 0}}}}}
	raw, err := json.Marshal(r)
	require.NoError(t, err)
	encoded, err := EncodeChapter(ChapterMeta{Ordinal: 6, Metadata: metadata}, "Execution", PayloadKindApp, raw)
	require.NoError(t, err)
	var p pb.ChapterRecord
	require.NoError(t, proto.Unmarshal(encoded, &p))
	require.EqualValues(t, ChapterFormatVersion, p.GetFormatVersion())
	require.Len(t, p.GetReconstruction(), 1)
	require.NotContains(t, p.GetMetadata().GetFields(), executionmeta.Key)
	require.Equal(t, "scope/task", p.GetExecution().GetRecord().GetId())
	env, err := DecodeChapter(encoded)
	require.NoError(t, err)
	got, err := jobdb.DecodeExecutionRecord(env.Payload)
	require.NoError(t, err)
	require.JSONEq(t, string(raw), string(env.Payload))
	require.Equal(t, r.Output.Data, got.Output.Data)
	var meta map[string]any
	require.NoError(t, json.Unmarshal(env.Meta.Metadata, &meta))
	require.JSONEq(t, string(prefixJSON), meta[executionmeta.Key].(string))
	wrapped, _ := json.Marshal(map[string]string{"protobuf": base64.StdEncoding.EncodeToString(encoded)})
	_, err = DecodeChapter(wrapped)
	require.Error(t, err)
	p.GetExecution().GetRecord().ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	bad, err := proto.Marshal(&p)
	require.NoError(t, err)
	_, err = DecodeChapter(bad)
	require.ErrorContains(t, err, "unsupported protobuf fields")
}
func TestExecutionSnapshotOpaqueBytesAndIdentity(t *testing.T) {
	r := jobdb.ExecutionRecord{Version: 4, Kind: "wait", ID: "task", At: time.Unix(1, 2).UTC(), Descriptor: json.RawMessage(`"task"`), Input: &jobdb.ExecutionSnapshot{Data: json.RawMessage(` { "a" : 1 } `)}}
	p, err := ExecutionToProto(r)
	require.NoError(t, err)
	got, err := executionFromProto(p)
	require.NoError(t, err)
	// Public JSON marshaling compacts its view; the storage field itself is opaque.
	require.Equal(t, []byte(r.Input.Data), p.GetInput().GetData())
	require.JSONEq(t, string(r.Input.Data), string(got.Input.Data))
	r.Input.Data = json.RawMessage(`{"a":1}`)
	hash, err := ExecutionIdentity(r)
	require.NoError(t, err)
	require.Equal(t, "ed2b67281bba3a851aadcafd41b4b64130279084d8c25e9b3606bf8e5ebaa834", hash)
	r.Input.Data = json.RawMessage(` { "a" : 1 } `)
	spaced, err := ExecutionIdentity(r)
	require.NoError(t, err)
	require.Equal(t, hash, spaced)
}
func TestNoInlineExecutionArtifactField(t *testing.T) {
	for _, data := range []string{`null`, `""`, `"YWJj"`} {
		raw := []byte(`{"version":4,"kind":"result","id":"task","output":{"data":null,"artifacts":[{"name":"a","key":{"jobId":"j","taskOrdinal":1,"name":"a","sizeBytes":0},"data":` + data + `}]}}`)
		_, err := jobdb.DecodeExecutionRecord(raw)
		require.ErrorContains(t, err, `unknown field "data"`)
	}
}
