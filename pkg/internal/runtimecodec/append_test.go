package runtimecodec

import (
	"encoding/json"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
)

func TestAppendChapterRoundTrip(t *testing.T) {
	for _, body := range []jobdb.ChapterBody{
		jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`"start"`)}, Append: &jobdb.ApplicationInputBytes{Data: []byte(`null`)}, AppendArtifacts: []jobdb.ArtifactKey{{JobId: "source", TaskOrdinal: 3, Name: "f", SizeBytes: 4}}},
		jobdb.AdditionalInputChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`{"n":2}`)}},
	} {
		typ, kind, payload, err := ChapterBodyToWire(body)
		require.NoError(t, err)
		meta := ChapterMeta{Version: 1, AppendPublication: json.RawMessage(`{"key":"value"}`)}
		raw, err := EncodeChapter(meta, typ, kind, payload)
		require.NoError(t, err)
		env, err := DecodeChapter(raw)
		require.NoError(t, err)
		got, err := ChapterBodyFromWire(env.ChapterType, env.PayloadKind, env.Payload)
		require.NoError(t, err)
		require.Equal(t, body, got)
		require.JSONEq(t, string(meta.AppendPublication), string(env.Meta.AppendPublication))
	}
}
