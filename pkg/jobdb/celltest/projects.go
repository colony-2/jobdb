package celltest

import (
	"context"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

// UnrestrictedProjects is an explicit fixture for tests of resource semantics
// using arbitrary synthetic tenant IDs. It supplies no catalog administration.
// Project/admission tests must compose a real installed catalog instead.
// Production constructors never select this fixture.
type UnrestrictedProjects struct{}

func (UnrestrictedProjects) AdmitWrite(context.Context, string) error { return nil }
func (UnrestrictedProjects) CreateProject(context.Context, jobdb.CreateProjectRequest) (jobdb.ProjectCreation, error) {
	return jobdb.ProjectCreation{}, jobdb.ErrProjectUnavailable
}
func (UnrestrictedProjects) GetProject(context.Context, string) (jobdb.Project, error) {
	return jobdb.Project{}, jobdb.ErrProjectUnavailable
}
func (UnrestrictedProjects) ListProjects(context.Context, jobdb.ListProjectsRequest) (jobdb.ListProjectsResponse, error) {
	return jobdb.ListProjectsResponse{}, jobdb.ErrProjectUnavailable
}
func (UnrestrictedProjects) DeleteProject(context.Context, jobdb.DeleteProjectRequest) error {
	return jobdb.ErrProjectUnavailable
}

func (UnrestrictedProjects) CheckProject(context.Context, string) error { return nil }
