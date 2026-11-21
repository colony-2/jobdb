package projectwire

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIdentityEnvelopesPreserveOpaqueBytes(t *testing.T) {
	opaque := []byte(`{ "tenantId" : "signed", "projectId":"opaque", "n":9007199254740993123456789, "a" : [ 1.0, 1 ] }`)
	raw := append([]byte(`{ "jobKey" : { "tenantId" : "c2", "jobId":"saved" }, "metadata":`), opaque...)
	raw = append(raw, []byte(`, "clientPayload":`)...)
	raw = append(raw, opaque...)
	raw = append(raw, '}')
	got, e := JSON(raw, "JobSummary", true)
	require.NoError(t, e)
	require.Contains(t, string(got), `"projectId" : "c2"`)
	require.Equal(t, 2, bytes.Count(got, opaque))
	restored, e := JSON(got, "JobSummary", false)
	require.NoError(t, e)
	require.Equal(t, raw, restored)
}
func TestIdentityFieldsRejectOtherFamilyAndDuplicates(t *testing.T) {
	for _, raw := range []string{`{"projectId":"c2","tenantId":"c2","jobId":"a"}`, `{"tenantId":"a","tenantId":"b","jobId":"a"}`} {
		_, e := JSON([]byte(raw), "JobKey", true)
		require.Error(t, e)
	}
	_, e := JSON([]byte(`{"tenantId":"c2","jobId":"a"}`), "JobKey", false)
	require.Error(t, e)
}
func TestNestedLeaseAndArrayIdentity(t *testing.T) {
	raw := []byte(`{"leases":[{"job":{"jobKey":{"tenantId":"c2","jobId":"saved"}},"clientPayload":{"tenantId":"opaque"},"leaseToken":"tenantId.signature"}]}`)
	got, e := JSON(raw, "pollWork200Response", true)
	require.NoError(t, e)
	require.Contains(t, string(got), `"projectId":"c2"`)
	require.Contains(t, string(got), `"clientPayload":{"tenantId":"opaque"}`)
	require.Contains(t, string(got), `"leaseToken":"tenantId.signature"`)
	back, e := JSON(got, "pollWork200Response", false)
	require.NoError(t, e)
	require.Equal(t, raw, back)
}
