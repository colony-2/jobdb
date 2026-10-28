package jobdb

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChapterApplicationPayloadBudget(t *testing.T) {
	ctx := context.Background()
	ch := Chapter{Ordinal: 7, Body: JobStartChapter{Input: ApplicationInputBytes{Data: []byte(strings.Repeat("x", 256*1024))}}, Metadata: ChapterMetadata{Fields: map[string]ChapterMetadataValue{"metadata": {Kind: ChapterMetadataString, String: strings.Repeat("m", 512*1024)}}}}
	require.NoError(t, ValidateChapterApplicationPayload(ctx, ch, 0))
	require.Error(t, ValidateChapterApplicationPayload(ctx, ch, -1))
	ch.Body = JobStartChapter{Input: ApplicationInputBytes{Data: []byte(strings.Repeat("x", 256*1024+1))}}
	var size *ChapterApplicationPayloadError
	require.ErrorAs(t, ValidateChapterApplicationPayload(ctx, ch, 0), &size)
	require.EqualValues(t, 1, size.ExcessBytes)
	require.Equal(t, "non_json", size.Contributors[0].JSON.Status)
	require.NoError(t, ValidateChapterApplicationPayload(ctx, ch, 512*1024))
	r := ExecutionRecord{Version: ExecutionPatternVersion, Kind: "result", ID: "task", Input: &ExecutionSnapshot{Data: json.RawMessage(`"abc"`)}, Output: &ExecutionSnapshot{Data: json.RawMessage(`"def"`)}}
	raw, err := json.Marshal(r)
	require.NoError(t, err)
	ch.Body = ExecutionChapter{Record: raw}
	require.NoError(t, ValidateChapterApplicationPayload(ctx, ch, 10))
	require.ErrorAs(t, ValidateChapterApplicationPayload(ctx, ch, 9), &size)
	require.EqualValues(t, 10, size.ActualBytes)
	require.Len(t, size.Contributors, 2)
	ch = PayloadFailureChapter(ch, size)
	require.NoError(t, ValidateChapterApplicationPayload(ctx, ch, 1))
	require.True(t, IsChapterPayloadFailure(size.AppError()))
}
func TestJSONPayloadStatisticsAndBounds(t *testing.T) {
	raw := []byte(` {"small":1,"history":["private-value","another-secret"],"escaped":"\u263a","same":1,"same":22} `)
	e := ValidateChapterApplicationPayload(context.Background(), Chapter{Body: JobStartChapter{Input: ApplicationInputBytes{Data: raw}}}, 1)
	var size *ChapterApplicationPayloadError
	require.ErrorAs(t, e, &size)
	stats := size.Contributors[0].JSON
	require.Equal(t, "complete", stats.Status)
	require.Equal(t, "object", stats.RootKind)
	require.Equal(t, 5, stats.Members)
	require.Equal(t, 2, stats.Elements)
	require.Equal(t, "/history", stats.Largest[0].Path)
	require.Equal(t, len(`["private-value","another-secret"]`), stats.Largest[0].Bytes)
	encoded, err := json.Marshal(size)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private-value")
	require.NotContains(t, e.Error(), "another-secret")
	require.Contains(t, string(encoded), "same#occurrence:2")
	for _, raw := range [][]byte{[]byte(strings.Repeat("[", 80) + "0" + strings.Repeat("]", 80)), []byte(`"` + strings.Repeat("z", 9*1024*1024) + `"`)} {
		require.ErrorAs(t, ValidateChapterApplicationPayload(context.Background(), Chapter{Body: JobStartChapter{Input: ApplicationInputBytes{Data: raw}}}, 1), &size)
		require.Equal(t, "partial", size.Contributors[0].JSON.Status)
		b, _ := json.Marshal(size)
		require.Less(t, len(b)+len(size.Error()), 8192)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorAs(t, ValidateChapterApplicationPayload(ctx, Chapter{Body: JobStartChapter{Input: ApplicationInputBytes{Data: raw}}}, 1), &size)
	require.EqualValues(t, len(raw), size.ActualBytes)
}

func TestReservedChapterInputCountsAndJSONPartitions(t *testing.T) {
	ch := Chapter{Body: TaskAttemptOutcomeChapter{Outcome: ApplicationOutputOutcome{Output: ApplicationOutputBytes{Data: []byte(`42`)}}}, Metadata: ChapterMetadata{Fields: map[string]ChapterMetadataValue{"input": {Kind: ChapterMetadataString, String: "abc"}}}}
	require.NoError(t, ValidateChapterApplicationPayload(context.Background(), ch, 7))
	var size *ChapterApplicationPayloadError
	require.ErrorAs(t, ValidateChapterApplicationPayload(context.Background(), ch, 6), &size)
	require.EqualValues(t, 7, size.ActualBytes)
	require.Equal(t, "chapter.input", size.Contributors[0].Location)
	for _, raw := range []string{` {"a":[1,2],"b":"abc","c":42} `, `[1,2,3]`} {
		ch.Body = JobStartChapter{Input: ApplicationInputBytes{Data: []byte(raw)}}
		ch.Metadata = ChapterMetadata{}
		require.ErrorAs(t, ValidateChapterApplicationPayload(context.Background(), ch, 1), &size)
		stats := size.Contributors[0].JSON
		require.Equal(t, "complete", stats.Status)
		require.NotNil(t, stats.RootRemainderBytes)
		total := *stats.RootRemainderBytes
		for _, f := range stats.RootMembers {
			total += f.Bytes
		}
		require.Equal(t, len(raw), total)
		if raw[0] == '[' {
			require.Empty(t, stats.RootMembers)
			require.Equal(t, 3, stats.RootElements)
		}
	}
}
