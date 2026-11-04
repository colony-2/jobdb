package jobdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

type CellState string

const (
	CellActive  CellState = "ACTIVE"
	CellRetired CellState = "RETIRED"
)

type Cell struct {
	TenantID    string                     `json:"tenantId"`
	ID          string                     `json:"id"`
	Name        string                     `json:"name"`
	Repo        string                     `json:"repo"`
	Description string                     `json:"description"`
	Metadata    map[string]json.RawMessage `json:"metadata"`
	State       CellState                  `json:"state"`
	Aliases     []string                   `json:"aliases"`
	Revision    string                     `json:"revision"`
	CreatedAt   time.Time                  `json:"createdAt"`
	UpdatedAt   time.Time                  `json:"updatedAt"`
	RetiredAt   *time.Time                 `json:"retiredAt,omitempty"`
}
type CellKey struct {
	TenantID string
	CellID   string
}
type CreateCellRequest struct {
	TenantID    string                     `json:"-"`
	Name        string                     `json:"name"`
	Repo        string                     `json:"repo"`
	Description string                     `json:"description,omitempty"`
	Metadata    map[string]json.RawMessage `json:"metadata,omitempty"`
	RequestID   string                     `json:"requestId,omitempty"`
}
type UpdateCellRequest struct {
	CellKey     `json:"-"`
	IfMatch     string                      `json:"-"`
	Name        *string                     `json:"name,omitempty"`
	Repo        *string                     `json:"repo,omitempty"`
	Description *string                     `json:"description,omitempty"`
	Metadata    *map[string]json.RawMessage `json:"metadata,omitempty"`
}
type RetireCellRequest struct {
	CellKey
	IfMatch string
}
type ListCellsRequest struct {
	TenantID, Name, State, PageToken string
	PageSize                         int
}
type ListCellsResponse struct {
	Cells         []Cell `json:"cells"`
	NextPageToken string `json:"nextPageToken,omitempty"`
}
type ResolvedCell struct {
	Cell     string `json:"cell"`
	CellID   string `json:"cellId"`
	CellRepo string `json:"cellRepo"`
}

func (c Cell) Resolved() ResolvedCell {
	return ResolvedCell{Cell: c.Name, CellID: c.ID, CellRepo: c.Repo}
}
func (c ResolvedCell) Handle(key JobKey, goal string) JobHandle {
	return JobHandle{JobKey: key, GoalID: goal, Cell: c.Cell, CellID: c.CellID, CellRepo: c.CellRepo}
}

type CellRuntime interface {
	CreateCell(context.Context, CreateCellRequest) (Cell, error)
	GetCell(context.Context, CellKey) (Cell, error)
	ListCells(context.Context, ListCellsRequest) (ListCellsResponse, error)
	UpdateCell(context.Context, UpdateCellRequest) (Cell, error)
	RetireCell(context.Context, RetireCellRequest) (Cell, error)
}

// CellAuthority is supplied only by trusted embedding code or HTTP authentication
// middleware. Never decode it from a request body or job metadata. A nil Cells
// map grants all cells; an empty map grants none. Permissions are independent.
// This policy covers cells, submissions, job listing and acquisition. Existing
// history/goal/mutation APIs retain deployment authorization; this is not a
// complete resource-wide RBAC system.
type CellAuthority struct {
	Principal   string
	TenantID    string
	Permissions map[string]bool
	Cells       map[string]bool
	// LeaseSubmitCells explicitly permits leases to submit to these existing cell IDs.
	LeaseSubmitCells map[string]bool
}
type cellAuthorityKey struct{}

func WithCellAuthority(ctx context.Context, a CellAuthority) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	a.Permissions = cloneCellPermissions(a.Permissions)
	a.Cells = cloneCellPermissions(a.Cells)
	a.LeaseSubmitCells = cloneCellPermissions(a.LeaseSubmitCells)
	return context.WithValue(ctx, cellAuthorityKey{}, a)
}
func cloneCellPermissions(m map[string]bool) map[string]bool {
	if m == nil {
		return nil
	}
	n := make(map[string]bool, len(m))
	for k, v := range m {
		n[k] = v
	}
	return n
}
func CellAuthorityFromContext(ctx context.Context) (CellAuthority, bool) {
	if ctx == nil {
		return CellAuthority{}, false
	}
	a, ok := ctx.Value(cellAuthorityKey{}).(CellAuthority)
	a.Permissions = cloneCellPermissions(a.Permissions)
	a.Cells = cloneCellPermissions(a.Cells)
	a.LeaseSubmitCells = cloneCellPermissions(a.LeaseSubmitCells)
	return a, ok
}
func AuthorizeCell(ctx context.Context, tenant, id, permission string) error {
	a, ok := CellAuthorityFromContext(ctx)
	if tenant == "" || !ok || a.Principal == "" || (a.TenantID != tenant && a.TenantID != "*") || !a.Permissions[permission] {
		return ErrCellForbidden
	}
	if id != "" && a.Cells != nil && !a.Cells[id] {
		return ErrCellNotFound
	}
	return nil
}

type CellError struct {
	Code   string
	Status int
}

func (e *CellError) Error() string { return e.Code }

var (
	ErrInvalidCell              = &CellError{"invalid_request", 400}
	ErrCellForbidden            = &CellError{"forbidden", 403}
	ErrCellNotFound             = &CellError{"not_found", 404}
	ErrCellConflict             = &CellError{"cell_conflict", 409}
	ErrCellRepoMismatch         = &CellError{"cell_repo_mismatch", 409}
	ErrCellRetired              = &CellError{"cell_retired", 409}
	ErrCellBusy                 = &CellError{"cell_busy", 409}
	ErrCellRequestConflict      = &CellError{"request_conflict", 409}
	ErrCellPrecondition         = &CellError{"precondition_failed", 412}
	ErrCellPreconditionRequired = &CellError{"precondition_required", 428}
)

func CellErrorStatus(err error) (int, string, bool) {
	var e *CellError
	if errors.As(err, &e) {
		return e.Status, e.Code, true
	}
	return 0, "", false
}

var cellNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func ValidateCellName(s string) error {
	if !cellNamePattern.MatchString(s) || s == "." || s == ".." {
		return ErrInvalidCell
	}
	return nil
}

// NormalizeCellRepo canonicalizes portable hosted repository locators without
// contacting Git. Unsupported transports and local paths are rejected.
func NormalizeCellRepo(s string) (string, error) {
	if s == "" || strings.TrimSpace(s) != s || strings.ContainsAny(s, "?#\\\t\r\n ") {
		return "", ErrInvalidCell
	}
	var host, path string
	if strings.Contains(s, "://") {
		u, e := url.Parse(s)
		if e != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
			return "", ErrInvalidCell
		}
		if u.Scheme != "https" && u.Scheme != "ssh" {
			return "", ErrInvalidCell
		}
		if u.Scheme == "https" && u.User != nil {
			return "", ErrInvalidCell
		}
		if u.User != nil {
			if _, password := u.User.Password(); password {
				return "", ErrInvalidCell
			}
		}
		if port := u.Port(); port != "" && !(u.Scheme == "https" && port == "443") && !(u.Scheme == "ssh" && port == "22") {
			return "", ErrInvalidCell
		}
		host = u.Hostname()
		path = strings.TrimPrefix(u.Path, "/")
	} else if at := strings.IndexByte(s, '@'); at >= 0 {
		rest := s[at+1:]
		h, p, ok := strings.Cut(rest, ":")
		if !ok || at == 0 {
			return "", ErrInvalidCell
		}
		host = h
		path = p
	} else {
		var ok bool
		host, path, ok = strings.Cut(s, "/")
		if !ok {
			return "", ErrInvalidCell
		}
	}
	host = strings.ToLower(host)
	path = strings.TrimSuffix(path, "/")
	path = strings.TrimSuffix(path, ".git")
	if host == "" || strings.ContainsAny(host, ":/@%") || host == "." || host == ".." || path == "" || strings.ContainsAny(path, ":@%") {
		return "", ErrInvalidCell
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return "", ErrInvalidCell
		}
	}
	if !utf8.ValidString(path) || len(host)+len(path)+1 > 2048 {
		return "", ErrInvalidCell
	}
	return host + "/" + path, nil
}
func ValidateCellProperties(description string, metadata map[string]json.RawMessage) error {
	if !utf8.ValidString(description) || utf8.RuneCountInString(description) > 2048 || len(metadata) > 64 {
		return ErrInvalidCell
	}
	for k, v := range metadata {
		if !utf8.ValidString(k) || utf8.RuneCountInString(k) < 1 || utf8.RuneCountInString(k) > 128 || !json.Valid(v) {
			return ErrInvalidCell
		}
	}
	raw, e := json.Marshal(metadata)
	if e != nil || len(raw) > 16384 {
		return ErrInvalidCell
	}
	return nil
}
func CheckCellRevision(header, revision string) error {
	if header == "" {
		return ErrCellPreconditionRequired
	}
	if len(header) < 3 || header[0] != '"' || header[len(header)-1] != '"' || strings.ContainsAny(header[1:len(header)-1], "\"\\,\r\n") {
		return ErrInvalidCell
	}
	if header != fmt.Sprintf("%q", revision) {
		return ErrCellPrecondition
	}
	return nil
}

func (e *CellError) Is(target error) bool {
	other, ok := target.(*CellError)
	return ok && e.Code == other.Code && e.Status == other.Status
}

var ErrInvalidCellPageToken = &CellError{Code: "invalid_page_token", Status: 400}
