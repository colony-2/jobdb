// Package cellstate implements the cell registry contract. Callers must run
// mutations in the same transaction/lock domain as job publication and leases.
package cellstate

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
)

type Creation struct {
	Principal, RequestID, Descriptor string
	Result                           jobdb.Cell
}
type State struct {
	CursorKey []byte
	Cells     map[string]jobdb.Cell
	Creations []Creation
}

func New() *State {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(err)
	}
	return &State{CursorKey: key, Cells: map[string]jobdb.Cell{}}
}
func Clone[T any](v T) T {
	raw, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	var out T
	if e = json.Unmarshal(raw, &out); e != nil {
		panic(e)
	}
	return out
}
func (s *State) ByName(name string) (jobdb.Cell, bool) {
	for _, c := range s.Cells {
		if c.Name == name {
			return c, true
		}
		for _, a := range c.Aliases {
			if a == name {
				return c, true
			}
		}
	}
	return jobdb.Cell{}, false
}
func (s *State) unique(c jobdb.Cell) error {
	for _, old := range s.Cells {
		if old.ID == c.ID {
			continue
		}
		if old.Repo == c.Repo {
			return jobdb.ErrCellConflict
		}
	}
	for _, n := range append([]string{c.Name}, c.Aliases...) {
		if old, ok := s.ByName(n); ok && old.ID != c.ID {
			return jobdb.ErrCellConflict
		}
	}
	return nil
}
func (s *State) Create(ctx context.Context, req jobdb.CreateCellRequest) (jobdb.Cell, error) {
	if e := jobdb.AuthorizeCell(ctx, req.TenantID, "", "cells.create"); e != nil {
		return jobdb.Cell{}, e
	}
	if e := jobdb.ValidateCellName(req.Name); e != nil {
		return jobdb.Cell{}, e
	}
	repo, e := jobdb.NormalizeCellRepo(req.Repo)
	if e != nil {
		return jobdb.Cell{}, e
	}
	req.Repo = repo
	if req.Metadata == nil {
		req.Metadata = map[string]json.RawMessage{}
	}
	if e = jobdb.ValidateCellProperties(req.Description, req.Metadata); e != nil {
		return jobdb.Cell{}, e
	}
	req.Metadata = canonicalMetadata(req.Metadata)
	if len(req.RequestID) > 128 {
		return jobdb.Cell{}, jobdb.ErrInvalidCell
	}
	authority, _ := jobdb.CellAuthorityFromContext(ctx)
	raw, _ := json.Marshal(req)
	descriptor := fmt.Sprintf("%x", sha256.Sum256(raw))
	if req.RequestID != "" {
		for _, r := range s.Creations {
			if r.Principal == authority.Principal && r.RequestID == req.RequestID {
				if r.Descriptor != descriptor {
					return jobdb.Cell{}, jobdb.ErrCellRequestConflict
				}
				if s.Cells[r.Result.ID].State == jobdb.CellRetired {
					return jobdb.Cell{}, jobdb.ErrCellRetired
				}
				return Clone(r.Result), nil
			}
		}
	}
	now := time.Now().UTC()
	c := jobdb.Cell{TenantID: req.TenantID, ID: ksuid.New().String(), Name: req.Name, Repo: repo, Description: req.Description, Metadata: Clone(req.Metadata), State: jobdb.CellActive, Aliases: []string{}, Revision: ksuid.New().String(), CreatedAt: now, UpdatedAt: now}
	if e = s.unique(c); e != nil {
		return jobdb.Cell{}, e
	}
	s.Cells[c.ID] = c
	if req.RequestID != "" {
		s.Creations = append(s.Creations, Creation{authority.Principal, req.RequestID, descriptor, Clone(c)})
	}
	return Clone(c), nil
}
func (s *State) Get(ctx context.Context, key jobdb.CellKey) (jobdb.Cell, error) {
	if e := jobdb.AuthorizeCell(ctx, key.TenantID, key.CellID, "cells.get"); e != nil {
		return jobdb.Cell{}, e
	}
	c, ok := s.Cells[key.CellID]
	if !ok {
		return jobdb.Cell{}, jobdb.ErrCellNotFound
	}
	return Clone(c), nil
}
func (s *State) Update(ctx context.Context, r jobdb.UpdateCellRequest) (jobdb.Cell, error) {
	c, e := s.Get(ctx, r.CellKey)
	if e != nil {
		return c, e
	}
	if e = jobdb.AuthorizeCell(ctx, r.TenantID, c.ID, "cells.update"); e != nil {
		return jobdb.Cell{}, e
	}
	if e = jobdb.CheckCellRevision(r.IfMatch, c.Revision); e != nil {
		return jobdb.Cell{}, e
	}
	if c.State != jobdb.CellActive {
		return jobdb.Cell{}, jobdb.ErrCellRetired
	}
	if r.Name == nil && r.Repo == nil && r.Description == nil && r.Metadata == nil {
		return jobdb.Cell{}, jobdb.ErrInvalidCell
	}
	old := Clone(c)
	if r.Name != nil {
		if e = jobdb.ValidateCellName(*r.Name); e != nil {
			return jobdb.Cell{}, e
		}
		if *r.Name != c.Name {
			aliases := []string{c.Name}
			for _, a := range c.Aliases {
				if a != *r.Name {
					aliases = append(aliases, a)
				}
			}
			sort.Strings(aliases)
			c.Aliases = aliases
			c.Name = *r.Name
		}
	}
	if r.Repo != nil {
		c.Repo, e = jobdb.NormalizeCellRepo(*r.Repo)
		if e != nil {
			return jobdb.Cell{}, e
		}
	}
	if r.Description != nil {
		c.Description = *r.Description
	}
	if r.Metadata != nil {
		if *r.Metadata == nil {
			return jobdb.Cell{}, jobdb.ErrInvalidCell
		}
		if e = jobdb.ValidateCellProperties(c.Description, *r.Metadata); e != nil {
			return jobdb.Cell{}, e
		}
		c.Metadata = canonicalMetadata(*r.Metadata)
	}
	if e = jobdb.ValidateCellProperties(c.Description, c.Metadata); e != nil {
		return jobdb.Cell{}, e
	}
	if e = s.unique(c); e != nil {
		return jobdb.Cell{}, e
	}
	if reflect.DeepEqual(old, c) {
		return c, nil
	}
	c.Revision = ksuid.New().String()
	c.UpdatedAt = time.Now().UTC()
	s.Cells[c.ID] = c
	return Clone(c), nil
}
func (s *State) Retire(ctx context.Context, r jobdb.RetireCellRequest, busy func(string) (bool, error)) (jobdb.Cell, error) {
	c, e := s.Get(ctx, r.CellKey)
	if e != nil {
		return c, e
	}
	if e = jobdb.AuthorizeCell(ctx, r.TenantID, c.ID, "cells.retire"); e != nil {
		return jobdb.Cell{}, e
	}
	if e = jobdb.CheckCellRevision(r.IfMatch, c.Revision); e != nil {
		return jobdb.Cell{}, e
	}
	if c.State == jobdb.CellRetired {
		return c, nil
	}
	b, e := busy(c.ID)
	if e != nil {
		return jobdb.Cell{}, e
	}
	if b {
		return jobdb.Cell{}, jobdb.ErrCellBusy
	}
	now := time.Now().UTC()
	c.State = jobdb.CellRetired
	c.RetiredAt = &now
	c.UpdatedAt = now
	c.Revision = ksuid.New().String()
	s.Cells[c.ID] = c
	return Clone(c), nil
}

type cursor struct {
	Tenant, Principal, Name, State, After string
	ExpiresAt                             int64
}

func (s *State) List(ctx context.Context, r jobdb.ListCellsRequest) (jobdb.ListCellsResponse, error) {
	out := jobdb.ListCellsResponse{Cells: []jobdb.Cell{}}
	if e := jobdb.AuthorizeCell(ctx, r.TenantID, "", "cells.list"); e != nil {
		return out, e
	}
	if r.State == "" {
		r.State = "ACTIVE"
	}
	if r.State != "ACTIVE" && r.State != "RETIRED" && r.State != "ALL" {
		return out, jobdb.ErrInvalidCell
	}
	if r.Name != "" {
		if e := jobdb.ValidateCellName(r.Name); e != nil {
			return out, e
		}
	}
	if r.PageSize == 0 {
		r.PageSize = 100
	}
	if r.PageSize < 1 || r.PageSize > 1000 {
		return out, jobdb.ErrInvalidCell
	}
	a, _ := jobdb.CellAuthorityFromContext(ctx)
	want := cursor{Tenant: r.TenantID, Principal: a.Principal, Name: r.Name, State: r.State, ExpiresAt: time.Now().Add(24 * time.Hour).Unix()}
	if r.PageToken != "" {
		parts := strings.Split(r.PageToken, ".")
		if len(parts) != 2 {
			return out, jobdb.ErrInvalidCellPageToken
		}
		raw, e := base64.RawURLEncoding.DecodeString(parts[0])
		signature, se := base64.RawURLEncoding.DecodeString(parts[1])
		mac := hmac.New(sha256.New, s.CursorKey)
		mac.Write(raw)
		if se != nil || !hmac.Equal(signature, mac.Sum(nil)) {
			return out, jobdb.ErrInvalidCellPageToken
		}
		if e != nil {
			return out, jobdb.ErrInvalidCellPageToken
		}
		var got cursor
		if json.Unmarshal(raw, &got) != nil {
			return out, jobdb.ErrInvalidCellPageToken
		}
		want.After = got.After
		want.ExpiresAt = got.ExpiresAt
		if got.ExpiresAt <= time.Now().Unix() {
			return out, jobdb.ErrInvalidCellPageToken
		}
		if got != want {
			return out, jobdb.ErrInvalidCellPageToken
		}
	}
	ids := make([]string, 0, len(s.Cells))
	for id := range s.Cells {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		c := s.Cells[id]
		if id <= want.After || a.Cells != nil && !a.Cells[id] || r.State != "ALL" && string(c.State) != r.State {
			continue
		}
		if r.Name != "" {
			found, ok := s.ByName(r.Name)
			if !ok || found.ID != id {
				continue
			}
		}
		if len(out.Cells) == r.PageSize {
			want.After = out.Cells[len(out.Cells)-1].ID
			raw, _ := json.Marshal(want)
			mac := hmac.New(sha256.New, s.CursorKey)
			mac.Write(raw)
			out.NextPageToken = base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
			break
		}
		out.Cells = append(out.Cells, Clone(c))
	}
	return out, nil
}

// Resolve validates a submission. Parent must be derived from a live lease by
// the transaction owner. Existing is the stored job snapshot for confirmations
// or restarts; confirmation allows a retired target, restart does not.
func (s *State) Resolve(ctx context.Context, tenant, name, repo string, parent *jobdb.Cell, existing *jobdb.ResolvedCell, confirmation bool) (jobdb.ResolvedCell, error) {
	if e := jobdb.ValidateCellName(name); e != nil {
		return jobdb.ResolvedCell{}, e
	}
	if parent == nil {
		if e := jobdb.AuthorizeCell(ctx, tenant, "", "jobs.submit"); e != nil {
			return jobdb.ResolvedCell{}, e
		}
	}
	if repo != "" {
		var e error
		repo, e = jobdb.NormalizeCellRepo(repo)
		if e != nil {
			return jobdb.ResolvedCell{}, e
		}
	}
	c, ok := s.ByName(name)
	if parent != nil && (!ok || c.ID != parent.ID) {
		a, trusted := jobdb.CellAuthorityFromContext(ctx)
		if !ok || !trusted || !a.LeaseSubmitCells[c.ID] || jobdb.AuthorizeCell(ctx, tenant, c.ID, "jobs.submit") != nil {
			return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
		}
	}
	if parent == nil && ok {
		if e := jobdb.AuthorizeCell(ctx, tenant, c.ID, "jobs.submit"); e != nil {
			return jobdb.ResolvedCell{}, e
		}
	}
	if existing != nil {
		if !ok || c.ID != existing.CellID || repo != "" && repo != existing.CellRepo {
			return jobdb.ResolvedCell{}, jobdb.NewExistingJobMismatchError("cell identity or repo differs")
		}
		if !confirmation && c.State == jobdb.CellRetired {
			return jobdb.ResolvedCell{}, jobdb.ErrCellRetired
		}
		out := *existing
		out.Cell = c.Name
		return out, nil
	}
	if ok {
		if c.State == jobdb.CellRetired {
			return jobdb.ResolvedCell{}, jobdb.ErrCellRetired
		}
		if repo != "" && repo != c.Repo {
			return jobdb.ResolvedCell{}, jobdb.ErrCellRepoMismatch
		}
		return c.Resolved(), nil
	}
	return jobdb.ResolvedCell{}, jobdb.ErrCellNotFound
}

// Select resolves requested names to immutable IDs before pagination/acquisition.
func (s *State) Select(ctx context.Context, tenant string, names []string, permission string) (map[string]bool, error) {
	if e := jobdb.AuthorizeCell(ctx, tenant, "", permission); e != nil {
		return nil, e
	}
	if names != nil && len(names) == 0 {
		return nil, jobdb.ErrInvalidCell
	}
	out := map[string]bool{}
	if names != nil {
		for _, name := range names {
			if e := jobdb.ValidateCellName(name); e != nil {
				return nil, e
			}
			c, ok := s.ByName(name)
			if !ok {
				return nil, jobdb.ErrCellNotFound
			}
			if e := jobdb.AuthorizeCell(ctx, tenant, c.ID, permission); e != nil {
				return nil, e
			}
			out[c.ID] = true
		}
		return out, nil
	}
	for id := range s.Cells {
		if jobdb.AuthorizeCell(ctx, tenant, id, permission) == nil {
			out[id] = true
		}
	}
	return out, nil
}

// Called only after validation, so malformed embedded JSON returns an error before cloning.
func canonicalMetadata(input map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(input))
	for k, raw := range input {
		var value any
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		_ = d.Decode(&value)
		out[k], _ = json.Marshal(value)
	}
	return out
}
