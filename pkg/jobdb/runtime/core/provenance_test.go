package runtimecore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	j "github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func wireBytes(n protowire.Number, b []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, n, protowire.BytesType), b)
}
func wireText(n protowire.Number, v string) []byte { return wireBytes(n, []byte(v)) }
func wireNumber(n protowire.Number, v uint64) []byte {
	return protowire.AppendVarint(protowire.AppendTag(nil, n, protowire.VarintType), v)
}
func versionRecord(consumer, commit, version string, hasCommit bool) []byte {
	versions := wireText(2, version)
	versions = append(versions, wireBytes(3, append(wireText(1, "c2j"), wireText(2, consumer)...))...)
	if hasCommit {
		versions = append(versions, wireBytes(3, append(wireText(1, "c2j.commit"), wireText(2, commit)...))...)
	}
	return append(wireText(2, "activation"), wireBytes(5, versions)...)
}
func physical(records ...[]byte) []byte {
	out := wireNumber(20, 2)
	for _, r := range records {
		out = append(out, wireBytes(19, r)...)
	}
	return append(out, wireBytes(30, nil)...)
}

type provenanceFixture struct {
	entries    map[int64][]byte
	ceiling    *int64
	calls      int
	after      func(*provenanceFixture)
	storageErr error
}

func (f *provenanceFixture) ExecutionProvenanceCeiling(context.Context, ChapterLogKey) (*int64, error) {
	if f.ceiling == nil {
		return nil, f.storageErr
	}
	v := *f.ceiling
	return &v, f.storageErr
}
func (f *provenanceFixture) ReadProvenanceChapterPage(ctx context.Context, q ProvenanceChapterPageRequest) (ProvenanceChapterPage, error) {
	f.calls++
	if f.after != nil {
		f.after(f)
		f.after = nil
	}
	out := ProvenanceChapterPage{NextOrdinal: q.StartOrdinal}
	remaining := q.Bytes
	for n := q.StartOrdinal; n <= q.ThroughOrdinal && len(out.Entries) < q.Records; n++ {
		b, ok := f.entries[n]
		e := ProvenanceChapterEntry{Ordinal: n, Body: b}
		if !ok {
			e.Issue = j.ExecutionProvenanceMissingRecord
		} else if int64(len(b)) > remaining {
			if len(out.Entries) > 0 {
				break
			}
			e.Body = nil
			e.Issue = j.ExecutionProvenanceOversizedRecord
		} else {
			remaining -= int64(len(b))
		}
		out.Entries = append(out.Entries, e)
		out.NextOrdinal = n + 1
	}
	return out, f.storageErr
}
func scanFixture(t *testing.T, f *provenanceFixture, limits j.ExecutionProvenanceLimits) j.ExecutionProvenance {
	t.Helper()
	out, err := readExecutionProvenance(context.Background(), j.ReadExecutionProvenanceRequest{JobKey: j.JobKey{TenantId: "tenant", JobId: "job"}, Consumer: "c2j"}, func(context.Context) error { return nil }, f, limits)
	require.NoError(t, err)
	raw, err := json.Marshal(out)
	require.NoError(t, err)
	require.Equal(t, len(raw), out.Usage.ResponseBytes)
	return out
}
func hasIssue(out j.ExecutionProvenance, reason j.ExecutionProvenanceIssueReason) bool {
	for _, issue := range out.Issues {
		if issue.Reason == reason {
			return true
		}
	}
	return false
}
func TestProvenancePairsRetainedReconstructionAndOpaqueData(t *testing.T) {
	a := versionRecord("v2", "b", "j2", true)
	b := versionRecord("v1", "a", "j1", true)
	absent := versionRecord("v1", "", "j1", false)
	restart := append(wireText(2, "restart"), wireBytes(26, b)...)
	restart = append(restart, wireBytes(26, absent)...)
	ceiling := int64(2)
	f := &provenanceFixture{ceiling: &ceiling, entries: map[int64][]byte{0: physical(a, b), 1: physical(restart), 2: physical(a)}}
	limits := j.DefaultExecutionProvenanceLimits()
	limits.PageRecords = 1
	out := scanFixture(t, f, limits)
	require.Equal(t, j.ExecutionProvenanceComplete, out.Status)
	require.Len(t, out.Pairs, 3)
	require.Equal(t, int64(2), *out.ThroughOrdinal)
	require.Equal(t, int64(2), *out.ScannedThroughOrdinal)
	require.Equal(t, 3, out.Usage.Pages)
	require.Equal(t, "v1", out.Pairs[0].ConsumerVersion)
	require.Nil(t, out.Pairs[1].ConsumerCommit)
	require.Equal(t, "a", *out.Pairs[0].ConsumerCommit)
	// Arbitrary application bytes are never searched for versions. Unknown
	// unrelated protobuf fields leave recognized metadata usable.
	raw := append(physical(a), wireBytes(100, []byte(`versions jobdb_version c2j secret`))...)
	raw = append(raw, wireBytes(31, []byte{0xff})...)
	f.entries = map[int64][]byte{0: raw}
	ceiling = 0
	out = scanFixture(t, f, limits)
	require.Len(t, out.Pairs, 1)
	require.Equal(t, j.ExecutionProvenanceComplete, out.Status)
	_, err := DecodeChapter(EncodedChapter{Payload: raw})
	require.Error(t, err, "strict decoding remains strict")
}
func TestProvenanceGapsPreserveIndependentPairsAndCeiling(t *testing.T) {
	ceiling := int64(3)
	f := &provenanceFixture{ceiling: &ceiling, entries: map[int64][]byte{0: physical(versionRecord("v", "", "j", false)), 1: physical([]byte{0xff}), 3: physical(versionRecord("", "", "j", false))}}
	f.after = func(f *provenanceFixture) { f.entries[4] = physical(versionRecord("future", "", "future", false)) }
	out := scanFixture(t, f, j.DefaultExecutionProvenanceLimits())
	require.Equal(t, j.ExecutionProvenancePartial, out.Status)
	require.Len(t, out.Pairs, 1)
	require.True(t, hasIssue(out, j.ExecutionProvenanceUnreadableRecord))
	require.True(t, hasIssue(out, j.ExecutionProvenanceMissingRecord))
	require.True(t, hasIssue(out, j.ExecutionProvenanceIncompleteIdentity))
	require.Equal(t, int64(1), *out.ScannedThroughOrdinal)
	require.Equal(t, int64(3), *out.ThroughOrdinal)
	require.Nil(t, out.UnexaminedRecordCount, "gaps may conceal an unknown number of retained records")
	f.ceiling = nil
	out = scanFixture(t, f, j.DefaultExecutionProvenanceLimits())
	require.Equal(t, j.ExecutionProvenanceUnavailable, out.Status)
	require.True(t, hasIssue(out, j.ExecutionProvenanceUnknownCeiling))
	require.Empty(t, out.Pairs)
	ceiling = 0
	f.ceiling = &ceiling
	f.entries = map[int64][]byte{0: physical()}
	out = scanFixture(t, f, j.DefaultExecutionProvenanceLimits())
	require.NotEqual(t, j.ExecutionProvenanceComplete, out.Status)
	require.True(t, hasIssue(out, j.ExecutionProvenanceMissingMetadata))
}
func TestProvenanceFiniteLimits(t *testing.T) {
	for _, tc := range []struct {
		name      string
		reason    j.ExecutionProvenanceIssueReason
		configure func(*j.ExecutionProvenanceLimits, *provenanceFixture)
	}{
		{"pages", j.ExecutionProvenancePageLimit, func(l *j.ExecutionProvenanceLimits, f *provenanceFixture) { l.Pages = 1; l.PageRecords = 1 }},
		{"records", j.ExecutionProvenanceRecordLimit, func(l *j.ExecutionProvenanceLimits, f *provenanceFixture) { l.Records = 1 }},
		{"bytes", j.ExecutionProvenanceByteLimit, func(l *j.ExecutionProvenanceLimits, f *provenanceFixture) { l.ScannedBytes = int64(len(f.entries[0])) }},
		{"pairs", j.ExecutionProvenancePairLimit, func(l *j.ExecutionProvenanceLimits, f *provenanceFixture) { l.Pairs = 1 }},
		{"identities", j.ExecutionProvenanceIdentityLimit, func(l *j.ExecutionProvenanceLimits, f *provenanceFixture) { l.IdentityBytes = 1 }},
		{"depth", j.ExecutionProvenanceDepthLimit, func(l *j.ExecutionProvenanceLimits, f *provenanceFixture) {
			l.NestingDepth = 1
			f.entries[0] = physical(append(wireText(2, "restart"), wireBytes(26, versionRecord("v1", "", "j1", false))...))
		}},
		{"response", j.ExecutionProvenanceResponseLimit, func(l *j.ExecutionProvenanceLimits, f *provenanceFixture) {
			l.ResponseBytes = 1500
			f.entries[0] = physical(versionRecord(strings.Repeat("x", 2000), "", "j", false))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ceiling := int64(1)
			f := &provenanceFixture{ceiling: &ceiling, entries: map[int64][]byte{0: physical(versionRecord("v1", "", "j1", false)), 1: physical(versionRecord("v2", "", "j2", false))}}
			l := j.DefaultExecutionProvenanceLimits()
			tc.configure(&l, f)
			out := scanFixture(t, f, l)
			require.NotEqual(t, j.ExecutionProvenanceComplete, out.Status)
			require.True(t, hasIssue(out, tc.reason), "%+v", out)
			require.LessOrEqual(t, out.Usage.ResponseBytes, l.ResponseBytes)
		})
	}
}
func TestProvenanceAuthorizationAndTimeBudget(t *testing.T) {
	key := j.JobKey{TenantId: "tenant", JobId: "job"}
	req := j.ReadExecutionProvenanceRequest{JobKey: key, Consumer: "c2j"}
	f := &provenanceFixture{}
	for _, denial := range []error{j.ErrJobNotFound, j.ErrCellForbidden, j.ErrLeasePermissionDenied} {
		_, err := ReadExecutionProvenance(context.Background(), req, func(context.Context) error { return denial }, f)
		require.ErrorIs(t, err, denial)
		require.Zero(t, f.calls)
	}
	out, err := ReadExecutionProvenance(context.Background(), req, func(context.Context) error { return errors.New("corrupt projection") }, f)
	require.NoError(t, err)
	require.Equal(t, j.ExecutionProvenanceUnavailable, out.Status)
	require.Empty(t, out.Pairs)
	require.Zero(t, f.calls)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	out, err = ReadExecutionProvenance(ctx, req, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, f)
	require.NoError(t, err)
	require.True(t, hasIssue(out, j.ExecutionProvenanceTimeLimit))
	require.LessOrEqual(t, out.Limits.ElapsedMillis, int64(20))
	require.Zero(t, f.calls)
}

func TestProvenanceMalformedVersionsAndIssueAccounting(t *testing.T) {
	ceiling := int64(1)
	valid := versionRecord("good", "", "jobdb", false)
	// Delimited metadata with a truncated trailing map entry must not yield a pair.
	malformed := append(wireText(2, "activation"), wireBytes(5, append(append(wireText(2, "bad-jobdb"), wireBytes(3, append(wireText(1, "c2j"), wireText(2, "bad-consumer")...))...), 0xff))...)
	f := &provenanceFixture{ceiling: &ceiling, entries: map[int64][]byte{0: physical(malformed), 1: physical(valid)}}
	out := scanFixture(t, f, j.DefaultExecutionProvenanceLimits())
	require.Equal(t, j.ExecutionProvenancePartial, out.Status)
	require.Len(t, out.Pairs, 1)
	require.Equal(t, "good", out.Pairs[0].ConsumerVersion)
	require.True(t, hasIssue(out, j.ExecutionProvenanceUnreadableRecord))
	// A long prefix of groups cannot recurse through the generic wire skipper.
	groups := wireNumber(20, 2)
	for i := 0; i < 10000; i++ {
		groups = protowire.AppendTag(groups, 100, protowire.StartGroupType)
	}
	f.entries[0] = groups
	out = scanFixture(t, f, j.DefaultExecutionProvenanceLimits())
	require.True(t, hasIssue(out, j.ExecutionProvenanceUnsupportedEncoding))
	require.Len(t, out.Pairs, 1)
	ceiling = 10
	f.entries = map[int64][]byte{0: physical(valid)}
	l := j.DefaultExecutionProvenanceLimits()
	l.Issues = 2
	out = scanFixture(t, f, l)
	require.Len(t, out.Issues, 2)
	require.True(t, hasIssue(out, j.ExecutionProvenanceIssueLimit))
	require.Equal(t, 2, out.OmittedIssueCount)
	require.Nil(t, out.UnexaminedRecordCount)
	require.LessOrEqual(t, out.Usage.Records, 5)
}
func TestProvenanceNormalPageByteExhaustion(t *testing.T) {
	ceiling := int64(3)
	body := physical(versionRecord("v", "", "j", false))
	f := &provenanceFixture{ceiling: &ceiling, entries: map[int64][]byte{0: body, 1: body, 2: body, 3: body}}
	l := j.DefaultExecutionProvenanceLimits()
	l.PageBytes = int64(len(body) + 1)
	out := scanFixture(t, f, l)
	require.Equal(t, j.ExecutionProvenanceComplete, out.Status)
	require.Equal(t, 4, out.Usage.Pages)
	require.Empty(t, out.Issues)
}
