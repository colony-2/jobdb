package cellstate

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// JobQueryBinding binds a continuation to its principal, authorized scope and
// resolved selectors. It is not a credential: backends must reapply authorization
// on every page. Page size may change while continuing a query.
func JobQueryBinding(ctx context.Context, req jobdb.ListJobsRequest, selected []string) string {
	req.PageToken = ""
	req.PageSize = 0
	req.CellNames = nil
	a, _ := jobdb.CellAuthorityFromContext(ctx)
	selected = append([]string(nil), selected...)
	sort.Strings(selected)
	raw, _ := json.Marshal(struct {
		Request   jobdb.ListJobsRequest
		Authority jobdb.CellAuthority
		Selected  []string
	}{req, a, selected})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

type jobCursor struct {
	Version           int
	Binding, Position string
}

func WrapJobCursor(position, binding string) string {
	if position == "" {
		return ""
	}
	raw, _ := json.Marshal(jobCursor{1, binding, position})
	return base64.RawURLEncoding.EncodeToString(raw)
}
func UnwrapJobCursor(token, binding string) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", jobdb.ErrInvalidCellPageToken
	}
	var c jobCursor
	if json.Unmarshal(raw, &c) != nil || c.Version != 1 || c.Binding != binding || c.Position == "" {
		return "", jobdb.ErrInvalidCellPageToken
	}
	return c.Position, nil
}
