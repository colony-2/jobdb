package projectcatalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func admin() context.Context {
	return jobdb.WithProjectAdministration(context.Background(), func(context.Context) error { return nil })
}
func fresh(t *testing.T) *Service {
	t.Helper()
	s := &Service{Store: &MemoryStore{}, CanDelete: func(context.Context, string) error { return nil }}
	require.NoError(t, s.Install(admin(), Installation{Manifest: InstallationDigest([]byte("explicit empty test installation"))}))
	return s
}
func create(t *testing.T, s *Service, name string) jobdb.ProjectCreation {
	t.Helper()
	r, e := s.CreateProject(admin(), jobdb.CreateProjectRequest{Name: name, RequestID: "create-" + name})
	require.NoError(t, e)
	return r
}
func TestProjectCatalogReceiptsAndTombstones(t *testing.T) {
	s := fresh(t)
	ctx := admin()
	q := jobdb.CreateProjectRequest{Name: "test", RequestID: "original", Metadata: json.RawMessage(`{"b":2,"a":1}`)}
	first, err := s.CreateProject(ctx, q)
	require.NoError(t, err)
	_, err = ksuid.Parse(first.Project.ID)
	require.NoError(t, err)
	require.Equal(t, 201, first.Status)
	require.Equal(t, ETag(first.Project.Revision), first.ETag)
	require.JSONEq(t, `{"a":1,"b":2}`, string(first.Project.Metadata))
	q.Metadata = json.RawMessage(`{"a":1,"b":2}`)
	retry, err := s.CreateProject(ctx, q)
	require.NoError(t, err)
	require.Equal(t, first, retry)
	q.Metadata = json.RawMessage(`{"a":1.0,"b":2}`)
	_, err = s.CreateProject(ctx, q)
	require.ErrorIs(t, err, jobdb.ErrProjectConflict)
	q.Metadata = json.RawMessage(`{"a":1,"b":2}`)
	for _, change := range []jobdb.CreateProjectRequest{{Name: q.Name, RequestID: q.RequestID, Description: "different", Metadata: q.Metadata}, {Name: "different", RequestID: q.RequestID, Metadata: q.Metadata}} {
		_, err = s.CreateProject(ctx, change)
		require.ErrorIs(t, err, jobdb.ErrProjectConflict)
	}
	for _, tc := range []struct {
		match string
		want  error
	}{{"", jobdb.ErrProjectPreconditionRequired}, {"*", jobdb.ErrInvalidProject}, {"W/" + first.ETag, jobdb.ErrInvalidProject}, {first.ETag + "," + first.ETag, jobdb.ErrInvalidProject}, {ETag(ksuid.New().String()), jobdb.ErrProjectRevisionMismatch}} {
		require.ErrorIs(t, s.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: first.Project.ID, IfMatch: tc.match}), tc.want)
	}
	require.NoError(t, s.DeleteProject(ctx, jobdb.DeleteProjectRequest{ID: first.Project.ID, IfMatch: first.ETag}))
	_, err = s.GetProject(ctx, first.Project.ID)
	require.ErrorIs(t, err, jobdb.ErrProjectNotFound)
	require.ErrorIs(t, s.AdmitWrite(ctx, first.Project.ID), jobdb.ErrProjectNotFound)
	retry, err = s.CreateProject(ctx, q)
	require.NoError(t, err)
	require.Equal(t, first, retry)
	q.RequestID = "new-request"
	replacement, err := s.CreateProject(ctx, q)
	require.NoError(t, err)
	require.NotEqual(t, first.Project.ID, replacement.Project.ID)
	// Callers cannot mutate the stored receipt through a returned byte slice.
	retry.Body[0] = '!'
	retry.Project.Metadata[0] = '!'
	again, err := s.CreateProject(ctx, jobdb.CreateProjectRequest{Name: "test", RequestID: "original", Metadata: json.RawMessage(`{"a":1,"b":2}`)})
	require.NoError(t, err)
	require.Equal(t, first, again)
}
func TestProjectValidationAndAccess(t *testing.T) {
	s := fresh(t)
	for _, name := range []string{"", "A", "with space", "-a", "a-", strings.Repeat("a", 64), "café", "a_b", " a"} {
		_, err := s.CreateProject(admin(), jobdb.CreateProjectRequest{Name: name, RequestID: "r"})
		require.ErrorIs(t, err, jobdb.ErrInvalidProject)
	}
	for _, metadata := range []string{`null`, `[]`, `{"a":1,"a":2}`, `{"x":{"a":1,"a":2}}`, `{"a":1} {}`, `{"a":NaN}`, strings.Repeat(`{"a":`, 34) + `0` + strings.Repeat(`}`, 34), `{"x":"` + strings.Repeat("a", 65536) + `"}`} {
		_, err := s.CreateProject(admin(), jobdb.CreateProjectRequest{Name: "valid", RequestID: "r", Metadata: json.RawMessage(metadata)})
		require.ErrorIs(t, err, jobdb.ErrInvalidProject, metadata[:min(len(metadata), 100)])
	}
	for _, request := range []string{"", " ", "a\nb", strings.Repeat("a", 257), string([]byte{255})} {
		_, err := s.CreateProject(admin(), jobdb.CreateProjectRequest{Name: "valid", RequestID: request})
		require.ErrorIs(t, err, jobdb.ErrInvalidProject)
	}
	for _, desc := range []string{strings.Repeat("a", 4097), string([]byte{255})} {
		_, err := s.CreateProject(admin(), jobdb.CreateProjectRequest{Name: "valid", RequestID: "r", Description: desc})
		require.ErrorIs(t, err, jobdb.ErrInvalidProject)
	}
	for _, raw := range []string{`{"id":"c2","name":"valid","requestId":"r"}`, `{"name":"valid","requestId":"r","name":"other"}`, `{"name":"valid","requestId":"r","metadata":null}`, `null`, `{} {}`} {
		_, err := DecodeCreate(strings.NewReader(raw))
		require.ErrorIs(t, err, jobdb.ErrInvalidProject)
	}
	p := create(t, s, "valid")
	denied := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{Principal: "cell-admin", TenantID: "*", Permissions: map[string]bool{"cells.create": true}})
	_, err := s.CreateProject(denied, jobdb.CreateProjectRequest{Name: "valid", RequestID: "create-valid"})
	require.ErrorIs(t, err, jobdb.ErrProjectAccessDenied)
	_, err = s.GetProject(denied, p.Project.ID)
	require.ErrorIs(t, err, jobdb.ErrProjectAccessDenied)
	_, err = s.ListProjects(denied, jobdb.ListProjectsRequest{})
	require.ErrorIs(t, err, jobdb.ErrProjectAccessDenied)
	require.ErrorIs(t, s.DeleteProject(denied, jobdb.DeleteProjectRequest{ID: p.Project.ID}), jobdb.ErrProjectAccessDenied)
}
func TestProjectInstallationAndAdmission(t *testing.T) {
	s := &Service{Store: &MemoryStore{}}
	require.ErrorIs(t, s.AdmitWrite(context.Background(), "c2"), jobdb.ErrProjectUnavailable)
	in := Installation{Manifest: InstallationDigest([]byte("verified c2 and embedded zero inventory")), Seeds: []Seed{{ID: "c2", Name: "c2"}, {ID: "0", Name: "embedded"}}}
	require.NoError(t, s.Install(admin(), in))
	require.NoError(t, s.Install(admin(), in))
	for _, seed := range in.Seeds {
		p, err := s.GetProject(admin(), seed.ID)
		require.NoError(t, err)
		require.Equal(t, seed.ID, p.ID)
		require.NoError(t, s.AdmitWrite(context.Background(), seed.ID))
		require.ErrorIs(t, s.DeleteProject(admin(), jobdb.DeleteProjectRequest{ID: seed.ID, IfMatch: ETag(p.Revision)}), jobdb.ErrProjectConflict)
	}
	in.Seeds = in.Seeds[:1]
	require.ErrorIs(t, s.Install(admin(), in), jobdb.ErrProjectConflict)
	require.ErrorIs(t, s.AdmitWrite(context.Background(), "embedded"), jobdb.ErrProjectNotFound)
	p := create(t, s, "new")
	// External coverage is unknown by default, even for a never-used project.
	require.ErrorIs(t, s.DeleteProject(admin(), jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}), jobdb.ErrProjectConflict)
	s.CanDelete = func(context.Context, string) error { return nil }
	require.NoError(t, s.AdmitWrite(context.Background(), p.Project.ID))
	// A failed downstream operation and repeated admissions never clear the seal.
	require.NoError(t, s.AdmitWrite(context.Background(), p.Project.ID))
	require.ErrorIs(t, s.DeleteProject(admin(), jobdb.DeleteProjectRequest{ID: p.Project.ID, IfMatch: p.ETag}), jobdb.ErrProjectConflict)
	unchanged, err := s.GetProject(admin(), p.Project.ID)
	require.NoError(t, err)
	require.Equal(t, p.Project, unchanged)
}
func TestProjectListsBindCatalogAndQuery(t *testing.T) {
	s := fresh(t)
	for i := 0; i < 5; i++ {
		create(t, s, fmt.Sprintf("p%d", i))
	}
	seen := []string{}
	token := ""
	for {
		page, err := s.ListProjects(admin(), jobdb.ListProjectsRequest{PageSize: 2, PageToken: token})
		require.NoError(t, err)
		for _, p := range page.Projects {
			seen = append(seen, p.ID)
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	require.Len(t, seen, 5)
	require.IsIncreasing(t, seen)
	for _, q := range []jobdb.ListProjectsRequest{{PageSize: 201}, {PageSize: -1}, {PageSize: 3, PageToken: token}, {PageSize: 2, Name: "p0", PageToken: token}, {PageSize: 2, PageToken: token + "x"}} {
		_, err := s.ListProjects(admin(), q)
		require.ErrorIs(t, err, jobdb.ErrInvalidProject)
	}
	_, err := fresh(t).ListProjects(admin(), jobdb.ListProjectsRequest{PageSize: 2, PageToken: token})
	require.ErrorIs(t, err, jobdb.ErrInvalidProject)
	page, err := s.ListProjects(admin(), jobdb.ListProjectsRequest{Name: "p0"})
	require.NoError(t, err)
	require.Len(t, page.Projects, 1)
	_, err = s.GetProject(admin(), "p0")
	require.ErrorIs(t, err, jobdb.ErrProjectNotFound)
}
