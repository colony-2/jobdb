package projectcatalog

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
)

var namePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// Service owns the catalog contract. CanDelete must establish complete trusted
// resource/external-use coverage; nil, any uncertainty or any resource refuses
// deletion. It must not write through this catalog (the transaction is held).
// Implementations must keep their probes valid until the catalog transaction
// ends. Admission serializes with deletion before independent resource effects.
type Service struct {
	Store     Store
	CanDelete func(context.Context, string) error
}

var _ jobdb.ProjectCatalog = (*Service)(nil)
var _ jobdb.ProjectAdmission = (*Service)(nil)

type Seed struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Installation is supplied by the trusted upgrader only after inventory and
// verification of every participant. Imports are always conservatively sealed.
// Manifest is the SHA-256 of that inventory, even for an explicitly fresh setup.
type Installation struct {
	Manifest string `json:"manifest"`
	Seeds    []Seed `json:"seeds"`
}

func (s *Service) Install(ctx context.Context, in Installation) error {
	if err := jobdb.RequireProjectAdministration(ctx); err != nil {
		return err
	}
	if len(in.Manifest) != 64 {
		return jobdb.ErrInvalidProject
	}
	if _, err := hex.DecodeString(in.Manifest); err != nil {
		return jobdb.ErrInvalidProject
	}
	seeds := append([]Seed{}, in.Seeds...)
	slices.SortFunc(seeds, func(a, b Seed) int { return strings.Compare(a.ID, b.ID) })
	ids, names := map[string]bool{}, map[string]bool{}
	for _, seed := range seeds {
		if !validID(seed.ID) || !namePattern.MatchString(seed.Name) || ids[seed.ID] || names[seed.Name] {
			return jobdb.ErrInvalidProject
		}
		ids[seed.ID] = true
		names[seed.Name] = true
	}
	descriptor, _ := json.Marshal(Installation{Manifest: in.Manifest, Seeds: seeds})
	digest := sha256.Sum256(descriptor)
	return s.update(ctx, false, func(p **State) error {
		if *p != nil {
			if ready(*p) && (*p).Manifest == hex.EncodeToString(digest[:]) {
				for _, seed := range seeds {
					r, ok := (*p).Projects[seed.ID]
					if !ok || r.Deleted || !r.Sealed || r.Project.Name != seed.Name {
						return jobdb.ErrProjectConflict
					}
				}
				return nil
			}
			return jobdb.ErrProjectConflict
		}
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return err
		}
		state := &State{Version: version, ID: ksuid.New().String(), Manifest: hex.EncodeToString(digest[:]), CursorKey: key, Projects: map[string]record{}, Receipts: map[string]receipt{}}
		for _, seed := range seeds {
			state.Projects[seed.ID] = record{Project: newProject(seed.ID, seed.Name, "", json.RawMessage(`{}`)), Sealed: true}
		}
		*p = state
		return nil
	})
}
func validID(id string) bool {
	if id == "c2" || id == "0" {
		return true
	}
	_, err := ksuid.Parse(id)
	return err == nil
}
func newProject(id, name, description string, metadata json.RawMessage) jobdb.Project {
	return jobdb.Project{ID: id, Name: name, Description: description, Metadata: append(json.RawMessage(nil), metadata...), Revision: ksuid.New().String(), CreatedAt: time.Now().UTC()}
}
func ready(s *State) bool {
	return s != nil && s.Version == version && s.ID != "" && len(s.CursorKey) == 32 && s.Projects != nil && s.Receipts != nil
}
func (s *Service) update(ctx context.Context, installed bool, f func(**State) error) error {
	if s == nil || s.Store == nil {
		return jobdb.ErrProjectUnavailable
	}
	err := s.Store.Update(ctx, func(p **State) error {
		if installed && !ready(*p) {
			return jobdb.ErrProjectUnavailable
		}
		return f(p)
	})
	return serviceError(err)
}
func serviceError(err error) error {
	if err == nil {
		return nil
	}
	for _, known := range []error{jobdb.ErrInvalidProject, jobdb.ErrProjectNotFound, jobdb.ErrProjectConflict, jobdb.ErrProjectPreconditionRequired, jobdb.ErrProjectRevisionMismatch, jobdb.ErrProjectUnavailable, jobdb.ErrProjectAccessDenied} {
		if errors.Is(err, known) {
			return known
		}
	}
	return jobdb.ErrProjectUnavailable
}
func (s *Service) read(ctx context.Context) (*State, error) {
	if s == nil || s.Store == nil {
		return nil, jobdb.ErrProjectUnavailable
	}
	state, err := s.Store.Read(ctx)
	if err != nil {
		return nil, serviceError(err)
	}
	if !ready(state) {
		return nil, jobdb.ErrProjectUnavailable
	}
	return state, nil
}
func normalize(q jobdb.CreateProjectRequest) (jobdb.CreateProjectRequest, string, error) {
	if !namePattern.MatchString(q.Name) || !utf8.ValidString(q.Description) || len(q.Description) > 4096 || strings.TrimSpace(q.RequestID) == "" || !utf8.ValidString(q.RequestID) || len(q.RequestID) > 256 {
		return q, "", jobdb.ErrInvalidProject
	}
	for _, r := range q.RequestID {
		if unicode.IsControl(r) {
			return q, "", jobdb.ErrInvalidProject
		}
	}
	if len(q.Metadata) == 0 {
		q.Metadata = json.RawMessage(`{}`)
	}
	canonical, err := canonicalObject(q.Metadata)
	if err != nil {
		return q, "", err
	}
	q.Metadata = canonical
	raw, err := json.Marshal(q)
	return q, string(raw), err
}

// DecodeCreate rejects supplied IDs, unknown fields, duplicate keys and trailing
// input. Only new creation requests are canonicalized, never historical bytes.
func DecodeCreate(r io.Reader) (jobdb.CreateProjectRequest, error) {
	var q jobdb.CreateProjectRequest
	raw, err := io.ReadAll(io.LimitReader(r, 80*1024+1))
	if err != nil || len(raw) > 80*1024 {
		return q, jobdb.ErrInvalidProject
	}
	if _, err = canonicalObjectLimit(raw, 80*1024); err != nil {
		return q, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&q) != nil {
		return q, jobdb.ErrInvalidProject
	}
	q, _, err = normalize(q)
	return q, err
}
func canonicalObject(raw []byte) (json.RawMessage, error) { return canonicalObjectLimit(raw, 65536) }
func canonicalObjectLimit(raw []byte, limit int) (json.RawMessage, error) {
	if !utf8.Valid(raw) || len(raw) > limit {
		return nil, jobdb.ErrInvalidProject
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	v, err := parseValue(d, 0)
	if err != nil {
		return nil, jobdb.ErrInvalidProject
	}
	if _, ok := v.(map[string]any); !ok {
		return nil, jobdb.ErrInvalidProject
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, jobdb.ErrInvalidProject
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) > limit {
		return nil, jobdb.ErrInvalidProject
	}
	return b, nil
}
func parseValue(d *json.Decoder, depth int) (any, error) {
	if depth > 32 {
		return nil, jobdb.ErrInvalidProject
	}
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t {
	case json.Delim('{'):
		obj := map[string]any{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return nil, e
			}
			key, ok := k.(string)
			if !ok {
				return nil, jobdb.ErrInvalidProject
			}
			if _, ok = obj[key]; ok {
				return nil, jobdb.ErrInvalidProject
			}
			v, e := parseValue(d, depth+1)
			if e != nil {
				return nil, e
			}
			obj[key] = v
		}
		_, err = d.Token()
		return obj, err
	case json.Delim('['):
		a := []any{}
		for d.More() {
			v, e := parseValue(d, depth+1)
			if e != nil {
				return nil, e
			}
			a = append(a, v)
		}
		_, err = d.Token()
		return a, err
	default:
		if _, bad := t.(json.Delim); bad {
			return nil, jobdb.ErrInvalidProject
		}
		return t, nil
	}
}
func ETag(revision string) string { return `"` + revision + `"` }
func parseMatch(raw string) (string, error) {
	if raw == "" {
		return "", jobdb.ErrProjectPreconditionRequired
	}
	if len(raw) != 29 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return "", jobdb.ErrInvalidProject
	}
	rev := raw[1 : len(raw)-1]
	if _, err := ksuid.Parse(rev); err != nil {
		return "", jobdb.ErrInvalidProject
	}
	return rev, nil
}
func (s *Service) CreateProject(ctx context.Context, q jobdb.CreateProjectRequest) (jobdb.ProjectCreation, error) {
	var out jobdb.ProjectCreation
	if err := jobdb.RequireProjectAdministration(ctx); err != nil {
		return out, err
	}
	q, descriptor, err := normalize(q)
	if err != nil {
		return out, err
	}
	err = s.update(ctx, true, func(p **State) error {
		state := *p
		if r, ok := state.Receipts[q.RequestID]; ok {
			if r.Descriptor != descriptor {
				return jobdb.ErrProjectConflict
			}
			out = r.Result
			return nil
		}
		for _, r := range state.Projects {
			if !r.Deleted && r.Project.Name == q.Name {
				return jobdb.ErrProjectConflict
			}
		}
		var id string
		for {
			id = ksuid.New().String()
			if _, ok := state.Projects[id]; !ok {
				break
			}
		}
		project := newProject(id, q.Name, q.Description, q.Metadata)
		body, e := json.Marshal(project)
		if e != nil {
			return e
		}
		out = jobdb.ProjectCreation{Project: project, Body: body, Status: 201, ETag: ETag(project.Revision), Location: "/v1/projects/" + url.PathEscape(id)}
		state.Projects[id] = record{Project: project}
		state.Receipts[q.RequestID] = receipt{Descriptor: descriptor, Result: out}
		return nil
	})
	return out, err
}
func (s *Service) GetProject(ctx context.Context, id string) (jobdb.Project, error) {
	if err := jobdb.RequireProjectAdministration(ctx); err != nil {
		return jobdb.Project{}, err
	}
	state, err := s.read(ctx)
	if err != nil {
		return jobdb.Project{}, err
	}
	r, ok := state.Projects[id]
	if !ok || r.Deleted {
		return jobdb.Project{}, jobdb.ErrProjectNotFound
	}
	return r.Project, nil
}

type cursor struct {
	Catalog, Name, After string
	Size                 int
}

func (s *Service) ListProjects(ctx context.Context, q jobdb.ListProjectsRequest) (jobdb.ListProjectsResponse, error) {
	out := jobdb.ListProjectsResponse{Projects: []jobdb.Project{}}
	if err := jobdb.RequireProjectAdministration(ctx); err != nil {
		return out, err
	}
	if q.PageSize == 0 {
		q.PageSize = 50
	}
	if q.PageSize < 1 || q.PageSize > 200 || q.Name != "" && !namePattern.MatchString(q.Name) {
		return out, jobdb.ErrInvalidProject
	}
	state, err := s.read(ctx)
	if err != nil {
		return out, err
	}
	cur := cursor{Catalog: state.ID, Name: q.Name, Size: q.PageSize}
	if q.PageToken != "" {
		parts := strings.Split(q.PageToken, ".")
		if len(parts) != 2 || len(q.PageToken) > 2048 {
			return out, jobdb.ErrInvalidProject
		}
		raw, e := base64.RawURLEncoding.DecodeString(parts[0])
		sig, e2 := base64.RawURLEncoding.DecodeString(parts[1])
		mac := hmac.New(sha256.New, state.CursorKey)
		mac.Write(raw)
		var got cursor
		if e != nil || e2 != nil || !hmac.Equal(sig, mac.Sum(nil)) || json.Unmarshal(raw, &got) != nil || got.Catalog != cur.Catalog || got.Name != cur.Name || got.Size != cur.Size || !validID(got.After) {
			return out, jobdb.ErrInvalidProject
		}
		cur = got
	}
	ids := []string{}
	for id, r := range state.Projects {
		if !r.Deleted && id > cur.After && (q.Name == "" || r.Project.Name == q.Name) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for i, id := range ids {
		if i == q.PageSize {
			cur.After = ids[i-1]
			raw, _ := json.Marshal(cur)
			mac := hmac.New(sha256.New, state.CursorKey)
			mac.Write(raw)
			out.NextPageToken = base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
			break
		}
		out.Projects = append(out.Projects, state.Projects[id].Project)
	}
	return out, nil
}
func (s *Service) DeleteProject(ctx context.Context, q jobdb.DeleteProjectRequest) error {
	if err := jobdb.RequireProjectAdministration(ctx); err != nil {
		return err
	}
	revision, err := parseMatch(q.IfMatch)
	if err != nil {
		return err
	}
	return s.update(ctx, true, func(p **State) error {
		r, ok := (*p).Projects[q.ID]
		if !ok || r.Deleted {
			return jobdb.ErrProjectNotFound
		}
		if r.Project.Revision != revision {
			return jobdb.ErrProjectRevisionMismatch
		}
		if r.Sealed || s.CanDelete == nil {
			return jobdb.ErrProjectConflict
		}
		if err := s.CanDelete(ctx, q.ID); err != nil {
			return jobdb.ErrProjectConflict
		}
		r.Deleted = true
		(*p).Projects[q.ID] = r
		return nil
	})
}
func (s *Service) AdmitWrite(ctx context.Context, id string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	state, err := s.read(ctx)
	if err != nil {
		return err
	}
	if r, ok := state.Projects[id]; ok && r.Sealed && !r.Deleted {
		return nil
	}
	return s.update(ctx, true, func(p **State) error {
		r, ok := (*p).Projects[id]
		if !ok || r.Deleted {
			return jobdb.ErrProjectNotFound
		}
		r.Sealed = true
		(*p).Projects[id] = r
		return nil
	})
}

// InstallationDigest gives a stable manifest digest for small explicit fixtures.
// Production upgrade manifests should hash the actual participant inventories.
func InstallationDigest(manifest []byte) string {
	sum := sha256.Sum256(manifest)
	return fmt.Sprintf("%x", sum)
}

func (s *Service) CheckProject(ctx context.Context, id string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	state, err := s.read(ctx)
	if err != nil {
		return err
	}
	r, ok := state.Projects[id]
	if !ok || r.Deleted {
		return jobdb.ErrProjectNotFound
	}
	return nil
}
