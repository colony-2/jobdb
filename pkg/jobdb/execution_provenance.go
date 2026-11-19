package jobdb

import (
	"context"
	"errors"
)

var ErrExecutionProvenanceUnsupported = errors.New("execution provenance unsupported")
var ErrInvalidExecutionProvenanceRequest = errors.New("invalid execution provenance request")

type ExecutionProvenanceReader interface {
	ReadExecutionProvenance(context.Context, ReadExecutionProvenanceRequest) (ExecutionProvenance, error)
}
type ReadExecutionProvenanceRequest struct {
	JobKey   JobKey
	Consumer string
}
type ExecutionProvenanceStatus string

const (
	ExecutionProvenanceComplete    ExecutionProvenanceStatus = "complete"
	ExecutionProvenancePartial     ExecutionProvenanceStatus = "partial"
	ExecutionProvenanceUnavailable ExecutionProvenanceStatus = "unavailable"
	ExecutionProvenanceCapability                            = "execution_provenance_v1"
	ExecutionProvenanceScope                                 = "retained_execution_provenance"
)

type ExecutionProvenanceIssueReason string

const (
	ExecutionProvenanceMissingMetadata     ExecutionProvenanceIssueReason = "missing_metadata"
	ExecutionProvenanceIncompleteIdentity  ExecutionProvenanceIssueReason = "incomplete_identity"
	ExecutionProvenanceUnreadableRecord    ExecutionProvenanceIssueReason = "unreadable_record"
	ExecutionProvenanceUnsupportedEncoding ExecutionProvenanceIssueReason = "unsupported_encoding"
	ExecutionProvenanceMissingRecord       ExecutionProvenanceIssueReason = "missing_record"
	ExecutionProvenanceUnknownCeiling      ExecutionProvenanceIssueReason = "unknown_ceiling"
	ExecutionProvenanceRetainedUnavailable ExecutionProvenanceIssueReason = "retained_unavailable"
	ExecutionProvenanceStorageUnavailable  ExecutionProvenanceIssueReason = "storage_unavailable"
	ExecutionProvenancePageLimit           ExecutionProvenanceIssueReason = "page_limit"
	ExecutionProvenanceRecordLimit         ExecutionProvenanceIssueReason = "record_limit"
	ExecutionProvenanceByteLimit           ExecutionProvenanceIssueReason = "byte_limit"
	ExecutionProvenanceOversizedRecord     ExecutionProvenanceIssueReason = "oversized_record"
	ExecutionProvenancePairLimit           ExecutionProvenanceIssueReason = "pair_limit"
	ExecutionProvenanceIssueLimit          ExecutionProvenanceIssueReason = "issue_limit"
	ExecutionProvenanceTimeLimit           ExecutionProvenanceIssueReason = "time_limit"
	ExecutionProvenanceResponseLimit       ExecutionProvenanceIssueReason = "response_limit"
	ExecutionProvenanceDepthLimit          ExecutionProvenanceIssueReason = "depth_limit"
	ExecutionProvenanceIdentityLimit       ExecutionProvenanceIssueReason = "identity_limit"
)

type ExecutionProvenancePair struct {
	ConsumerVersion string  `json:"consumerVersion"`
	ConsumerCommit  *string `json:"consumerCommit"`
	JobDBVersion    string  `json:"jobdbVersion"`
}
type ExecutionProvenanceIssue struct {
	Reason     ExecutionProvenanceIssueReason `json:"reason"`
	Ordinal    *int64                         `json:"ordinal"`
	EndOrdinal *int64                         `json:"endOrdinal,omitempty"`
}
type ExecutionProvenanceLimits struct {
	Pages         int   `json:"pages"`
	Records       int   `json:"records"`
	ScannedBytes  int64 `json:"scannedBytes"`
	Pairs         int   `json:"pairs"`
	Issues        int   `json:"issues"`
	ElapsedMillis int64 `json:"elapsedMillis"`
	ResponseBytes int   `json:"responseBytes"`
	PageRecords   int   `json:"pageRecords"`
	PageBytes     int64 `json:"pageBytes"`
	NestingDepth  int   `json:"nestingDepth"`
	IdentityBytes int   `json:"identityBytes"`
}

func DefaultExecutionProvenanceLimits() ExecutionProvenanceLimits {
	return ExecutionProvenanceLimits{Pages: 64, Records: 10000, ScannedBytes: 16777216, Pairs: 512, Issues: 128, ElapsedMillis: 1500, ResponseBytes: 262144, PageRecords: 256, PageBytes: 1048576, NestingDepth: 64, IdentityBytes: 4096}
}

type ExecutionProvenanceUsage struct {
	Pages         int   `json:"pages"`
	Records       int   `json:"records"`
	ScannedBytes  int64 `json:"scannedBytes"`
	ElapsedMillis int64 `json:"elapsedMillis"`
	ResponseBytes int   `json:"responseBytes"`
}
type ExecutionProvenance struct {
	Capability            string                     `json:"capability"`
	Scope                 string                     `json:"scope"`
	Consumer              string                     `json:"consumer"`
	Status                ExecutionProvenanceStatus  `json:"status"`
	ThroughOrdinal        *int64                     `json:"throughOrdinal"`
	ScannedThroughOrdinal *int64                     `json:"scannedThroughOrdinal"`
	Pairs                 []ExecutionProvenancePair  `json:"pairs"`
	Issues                []ExecutionProvenanceIssue `json:"issues"`
	OmittedIssueCount     int                        `json:"omittedIssueCount"`
	OmittedPairCount      int                        `json:"omittedPairCount"`
	UnexaminedRecordCount *int64                     `json:"unexaminedRecordCount"`
	Limits                ExecutionProvenanceLimits  `json:"limits"`
	Usage                 ExecutionProvenanceUsage   `json:"usage"`
}

// ReadExecutionProvenance forwards optional support without implying capability
// from the presence of an interface on a wrapper or HTTP client.
func ReadExecutionProvenance(ctx context.Context, runtime any, req ReadExecutionProvenanceRequest) (ExecutionProvenance, error) {
	if reader, ok := runtime.(ExecutionProvenanceReader); ok {
		return reader.ReadExecutionProvenance(ctx, req)
	}
	return ExecutionProvenance{}, ErrExecutionProvenanceUnsupported
}
