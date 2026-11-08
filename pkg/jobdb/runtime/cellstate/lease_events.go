package cellstate

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"sort"
	"strconv"
	"strings"
	"time"
)

// LeaseEventScope uses only an already-loaded catalog and in-memory authority.
func (s *State) LeaseEventScope(ctx context.Context, req jobdb.ListLeaseEventsRequest) ([]string, error) {
	allowed, err := s.Select(ctx, req.TenantID, req.CellNames, "jobs.list")
	if err != nil {
		return nil, err
	}
	if req.CellIDs != nil {
		allowed = map[string]bool{}
		for _, id := range req.CellIDs {
			if _, ok := s.Cells[id]; !ok {
				return nil, jobdb.ErrCellNotFound
			}
			if err := jobdb.AuthorizeCell(ctx, req.TenantID, id, "jobs.list"); err != nil {
				return nil, err
			}
			allowed[id] = true
		}
	}
	ids := []string{}
	for id := range allowed {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

type LeaseEventPosition struct {
	Time time.Time
	ID   int64
}
type leaseEventCursor struct {
	Version  int
	Binding  string
	Position LeaseEventPosition
}

func LeaseEventBinding(ctx context.Context, req jobdb.ListLeaseEventsRequest, ids []string) string {
	ids = append([]string(nil), ids...)
	sort.Strings(ids)
	req.PageToken = ""
	req.CellNames = nil
	req.CellIDs = nil
	a, _ := jobdb.CellAuthorityFromContext(ctx)
	raw, _ := json.Marshal(struct {
		Request   jobdb.ListLeaseEventsRequest
		Scope     []string
		Authority jobdb.CellAuthority
	}{req, ids, a})
	// TenantID is excluded from the transport request encoding, so bind it explicitly.
	sum := sha256.Sum256(append([]byte(req.TenantID+"\x00"), raw...))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
func EncodeLeaseEventCursor(key []byte, binding string, p LeaseEventPosition) string {
	raw, _ := json.Marshal(leaseEventCursor{1, binding, p})
	m := hmac.New(sha256.New, key)
	m.Write([]byte("jobdb-lease-events-v1\x00"))
	m.Write(raw)
	return base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}
func DecodeLeaseEventCursor(key []byte, binding, token string) (*LeaseEventPosition, error) {
	if token == "" {
		return nil, nil
	}
	bad := jobdb.ErrInvalidLeaseEventPageToken
	parts := strings.Split(token, ".")
	if len(parts) != 2 || len(key) == 0 {
		return nil, bad
	}
	raw, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return nil, bad
	}
	sig, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return nil, bad
	}
	m := hmac.New(sha256.New, key)
	m.Write([]byte("jobdb-lease-events-v1\x00"))
	m.Write(raw)
	if !hmac.Equal(sig, m.Sum(nil)) {
		return nil, bad
	}
	var c leaseEventCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != 1 || c.Binding != binding || c.Position.ID < 1 || c.Position.Time.IsZero() {
		return nil, bad
	}
	return &c.Position, nil
}

// LeaseEventPage applies one-row lookahead to rows already ordered and filtered by the backend.
func LeaseEventPage(events []jobdb.LeaseEvent, size int, key []byte, binding string) jobdb.ListLeaseEventsResponse {
	out := jobdb.ListLeaseEventsResponse{Events: events}
	if out.Events == nil {
		out.Events = []jobdb.LeaseEvent{}
	}
	if len(events) > size {
		out.Events = events[:size]
		last := out.Events[size-1]
		id, _ := strconv.ParseInt(last.EventID, 10, 64)
		out.NextPageToken = EncodeLeaseEventCursor(key, binding, LeaseEventPosition{last.OccurredAt, id})
	}
	return out
}
