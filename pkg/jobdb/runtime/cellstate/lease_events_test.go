package cellstate

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestLeaseEventCursorDomainAndScope(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	q, e := jobdb.NormalizeLeaseEventRequest(jobdb.ListLeaseEventsRequest{TenantID: "tenant", JobIDs: []string{"b", "a"}})
	require.NoError(t, e)
	ctx := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{Principal: "p", TenantID: "tenant", Permissions: map[string]bool{"jobs.list": true}})
	binding := LeaseEventBinding(ctx, q, []string{"c"})
	p := LeaseEventPosition{time.Now().UTC(), 15}
	token := EncodeLeaseEventCursor(key, binding, p)
	out, e := DecodeLeaseEventCursor(key, binding, token)
	require.NoError(t, e)
	require.Equal(t, p, *out)
	for _, key := range [][]byte{nil, []byte("wrong")} {
		_, e := DecodeLeaseEventCursor(key, binding, token)
		require.ErrorIs(t, e, jobdb.ErrInvalidLeaseEventPageToken)
	}
	// A structurally identical cursor signed in the cell-list domain must fail.
	parts := strings.Split(token, ".")
	raw, e := base64.RawURLEncoding.DecodeString(parts[0])
	require.NoError(t, e)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("jobdb-cell-list-v1\x00"))
	mac.Write(raw)
	crossDomain := parts[0] + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	_, e = DecodeLeaseEventCursor(key, binding, crossDomain)
	require.ErrorIs(t, e, jobdb.ErrInvalidLeaseEventPageToken)
	other := EncodeLeaseEventCursor(key, "other domain", p)
	_, e = DecodeLeaseEventCursor(key, binding, other)
	require.ErrorIs(t, e, jobdb.ErrInvalidLeaseEventPageToken)
	for _, b := range []string{LeaseEventBinding(ctx, q, []string{"other"}), LeaseEventBinding(context.Background(), q, []string{"c"})} {
		_, e := DecodeLeaseEventCursor(key, b, token)
		require.ErrorIs(t, e, jobdb.ErrInvalidLeaseEventPageToken)
	}
	q.JobIDs = []string{"a", "b", "a"}
	q, e = jobdb.NormalizeLeaseEventRequest(q)
	require.NoError(t, e)
	require.Equal(t, binding, LeaseEventBinding(ctx, q, []string{"c"}))
}
