package workflow

import (
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGroupedChapterConversionsAndIsolation(t *testing.T) {
	ref := ArtifactKey{JobId: "source", TaskOrdinal: 7, Name: "same.txt", SizeBytes: 4}
	start := JobStartChapter{Input: ApplicationInputBytes{Data: []byte(`"creator"`)}, Append: &ApplicationInputBytes{Data: []byte(`null`)}, AppendArtifacts: []ArtifactKey{ref}}
	extra := jobdb.AdditionalInputChapter{Input: ApplicationInputBytes{Data: []byte(`{"n":2}`)}, Artifacts: []ArtifactKey{ref}}
	for _, tc := range []struct {
		body           ChapterBody
		want           ChapterBody
		typ, kind, raw string
	}{
		{start, start, "JobStart", "StartAppend", `{"input":"creator","append":null,"artifacts":[{"jobId":"source","taskOrdinal":7,"name":"same.txt","sizeBytes":4}]}`},
		{&start, start, "JobStart", "StartAppend", `{"input":"creator","append":null,"artifacts":[{"jobId":"source","taskOrdinal":7,"name":"same.txt","sizeBytes":4}]}`},
		{extra, extra, "AdditionalInput", "App", `{"input":{"n":2},"artifacts":[{"jobId":"source","taskOrdinal":7,"name":"same.txt","sizeBytes":4}]}`},
		{&extra, extra, "AdditionalInput", "App", `{"input":{"n":2},"artifacts":[{"jobId":"source","taskOrdinal":7,"name":"same.txt","sizeBytes":4}]}`},
	} {
		typ, kind, raw, err := chapterBodyToWire(tc.body)
		require.NoError(t, err)
		require.Equal(t, tc.typ, typ)
		require.Equal(t, tc.kind, kind)
		require.JSONEq(t, tc.raw, string(raw))
		got, err := chapterBodyFromWire(typ, kind, raw)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
		if typ == "JobStart" {
			payloadKind, payload, err := chapterPayload(Chapter{Body: tc.body})
			require.NoError(t, err)
			require.Equal(t, "App", payloadKind)
			require.Equal(t, `"creator"`, string(payload))
			payload[1] = 'x'
			require.Equal(t, `"creator"`, string(start.Input.Data))
		}
		cloned := cloneChapterBody(tc.body)
		switch b := cloned.(type) {
		case JobStartChapter:
			b.Input.Data[1] = 'x'
			b.Append.Data[0] = 'x'
			b.AppendArtifacts[0].Name = "changed"
		case *JobStartChapter:
			b.Input.Data[1] = 'x'
			b.Append.Data[0] = 'x'
			b.AppendArtifacts[0].Name = "changed"
		case jobdb.AdditionalInputChapter:
			b.Input.Data[1] = 'x'
			b.Artifacts[0].Name = "changed"
		case *jobdb.AdditionalInputChapter:
			b.Input.Data[1] = 'x'
			b.Artifacts[0].Name = "changed"
		}
		_, _, after, err := chapterBodyToWire(tc.body)
		require.NoError(t, err)
		require.JSONEq(t, tc.raw, string(after))
	}
	for _, body := range []ChapterBody{nil, (*JobStartChapter)(nil), (*jobdb.AdditionalInputChapter)(nil)} {
		_, _, _, err := chapterBodyToWire(body)
		require.Error(t, err)
		_, _, err = chapterPayload(Chapter{Body: body})
		require.Error(t, err)
	}
	for _, typ := range []string{"JobStart", "AdditionalInput"} {
		_, err := chapterBodyFromWire(typ, "StartAppend", []byte(`{`))
		require.Error(t, err)
	}
	ordinary, err := chapterBodyFromWire("JobStart", "App", []byte(`"ordinary"`))
	require.NoError(t, err)
	require.Equal(t, JobStartChapter{Input: ApplicationInputBytes{Data: []byte(`"ordinary"`)}}, ordinary)
	// Mutation in the original must also leave a previous clone unchanged.
	copyStart := cloneChapterBody(&start).(*JobStartChapter)
	copyExtra := cloneChapterBody(&extra).(*jobdb.AdditionalInputChapter)
	start.Input.Data[1] = 'x'
	start.Append.Data[0] = 'x'
	start.AppendArtifacts[0].Name = "changed"
	extra.Input.Data[1] = 'x'
	extra.Artifacts[0].Name = "changed"
	require.Equal(t, `"creator"`, string(copyStart.Input.Data))
	require.Equal(t, `null`, string(copyStart.Append.Data))
	require.Equal(t, ref, copyStart.AppendArtifacts[0])
	require.Equal(t, `{"n":2}`, string(copyExtra.Input.Data))
	require.Equal(t, ref, copyExtra.Artifacts[0])
}
