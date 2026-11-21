package jobdb

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Project is the public identity of a tenant. Internal tenant keys, signed
// documents and retained execution bytes continue to contain the immutable ID.
// A project name is descriptive and must never be used as a resource address.
type Project struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Metadata    json.RawMessage `json:"metadata"`
	Revision    string          `json:"revision"`
	CreatedAt   time.Time       `json:"createdAt"`
}
type CreateProjectRequest struct {
	RequestID   string          `json:"requestId"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
}

// ProjectCreation is an immutable receipt; retries return these original bytes,
// even if the project has since been deleted.
type ProjectCreation struct {
	Project  Project         `json:"project"`
	Body     json.RawMessage `json:"body"`
	Status   int             `json:"status"`
	ETag     string          `json:"etag"`
	Location string          `json:"location"`
}
type ListProjectsRequest struct {
	Name, PageToken string
	PageSize        int
}
type ListProjectsResponse struct {
	Projects      []Project `json:"projects"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
}
type DeleteProjectRequest struct{ ID, IfMatch string }

// ProjectCatalog is additive: implementing it does not change the legacy runtime
// interfaces or select a different HTTP transport for existing clients.
type ProjectCatalog interface {
	CreateProject(context.Context, CreateProjectRequest) (ProjectCreation, error)
	GetProject(context.Context, string) (Project, error)
	ListProjects(context.Context, ListProjectsRequest) (ListProjectsResponse, error)
	DeleteProject(context.Context, DeleteProjectRequest) error
}

// ProjectAdmission commits a monotonic seal before any independent resource
// effect. It grants no lease, cell permission or other resource authority.
// ProjectControl composes catalog administration with resource admission.
type ProjectControl interface {
	ProjectCatalog
	ProjectExistence
	ProjectAdmission
}

type ProjectAdmission interface {
	AdmitWrite(context.Context, string) error
}

var (
	ErrInvalidProject              = errors.New("invalid project request")
	ErrProjectNotFound             = errors.New("project not found")
	ErrProjectConflict             = errors.New("project conflict or usage cannot be disproved")
	ErrProjectPreconditionRequired = errors.New("project If-Match is required")
	ErrProjectRevisionMismatch     = errors.New("project revision mismatch")
	ErrProjectUnavailable          = errors.New("project catalog unavailable or not installed")
	ErrProjectAccessDenied         = errors.New("project administration denied")
)

type projectAdministrationKey struct{}

// WithProjectAdministration connects an existing trusted administrative ingress
// to the catalog. Never derive this callback from a lease, cell grant, request
// body or job metadata. A missing callback denies administration.
func WithProjectAdministration(ctx context.Context, authorize func(context.Context) error) context.Context {
	return context.WithValue(ctx, projectAdministrationKey{}, authorize)
}
func RequireProjectAdministration(ctx context.Context) error {
	if ctx == nil {
		return ErrProjectAccessDenied
	}
	f, ok := ctx.Value(projectAdministrationKey{}).(func(context.Context) error)
	if !ok || f == nil || f(ctx) != nil {
		return ErrProjectAccessDenied
	}
	return nil
}

// RequireProjectWrite denies unconfigured writers. Tenant is the immutable
// project ID; admission never resolves names or supplies execution authority.
func RequireProjectWrite(ctx context.Context, admission ProjectAdmission, tenant string) error {
	if admission == nil {
		return ErrProjectUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return admission.AdmitWrite(ctx, tenant)
}

// ProjectExistence checks active identity for public project readers without
// creating a first-use seal or granting administration.
type ProjectExistence interface {
	CheckProject(context.Context, string) error
}
