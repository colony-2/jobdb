package runtimecore

import (
	"context"
	j "github.com/colony-2/jobdb/pkg/jobdb"
	"testing"
)

func TestInfrastructureRegressionUnknownExecutionLayoutNotComplete(t *testing.T) {
	ceiling := int64(0)
	// A recognized versions tuple remains useful, but an unknown retained record
	// kind cannot establish gap-free coverage of its provenance-bearing structures.
	unknown := append(wireText(2, "future_retained_layout"), wireBytes(40, versionRecord("hidden", "", "j", false))...)
	f := &provenanceFixture{ceiling: &ceiling, entries: map[int64][]byte{0: physical(versionRecord("known", "", "j", false), unknown)}}
	out := scanFixture(t, f, j.DefaultExecutionProvenanceLimits())
	if out.Status == j.ExecutionProvenanceComplete || !hasIssue(out, j.ExecutionProvenanceUnsupportedEncoding) {
		t.Fatalf("unknown execution layout claimed complete: %+v", out)
	}
}

type reviewSkippedSource struct{}

func (reviewSkippedSource) ExecutionProvenanceCeiling(context.Context, ChapterLogKey) (*int64, error) {
	n := int64(2)
	return &n, nil
}
func (reviewSkippedSource) ReadProvenanceChapterPage(context.Context, ProvenanceChapterPageRequest) (ProvenanceChapterPage, error) {
	return ProvenanceChapterPage{NextOrdinal: 3, Entries: []ProvenanceChapterEntry{{Ordinal: 0, Body: physical(versionRecord("known", "", "j", false))}, {Ordinal: 1, Issue: j.ExecutionProvenanceOversizedRecord}, {Ordinal: 2, Body: physical(versionRecord("known", "", "j", false))}}}, nil
}
func TestInfrastructureRegressionSkippedBodyStopsContiguousCoverage(t *testing.T) {
	out, err := ReadExecutionProvenance(context.Background(), j.ReadExecutionProvenanceRequest{JobKey: j.JobKey{TenantId: "tenant", JobId: "job"}, Consumer: "c2j"}, func(context.Context) error { return nil }, reviewSkippedSource{})
	if err != nil {
		t.Fatal(err)
	}
	if out.ScannedThroughOrdinal == nil || *out.ScannedThroughOrdinal != 0 {
		t.Fatalf("skipped ordinal 1 yet scannedThroughOrdinal=%d", *out.ScannedThroughOrdinal)
	}
}
