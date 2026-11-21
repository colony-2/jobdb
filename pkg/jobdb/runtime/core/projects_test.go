package runtimecore

import (
	"context"
	"encoding/json"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/stretchr/testify/require"
	"testing"
)

type projectPreparationProbe struct {
	Scheduler
	calls int
}

func (p *projectPreparationProbe) PrepareJob(context.Context, JobPreparation) (PreparedJob, error) {
	p.calls++
	return PreparedJob{}, jobdb.ErrConflict
}
func TestProjectAdmissionPrecedesCorePreparation(t *testing.T) {
	ctx := jobdb.WithProjectAdministration(context.Background(), func(context.Context) error { return nil })
	catalog := &projectcatalog.Service{Store: &projectcatalog.MemoryStore{}}
	require.NoError(t, catalog.Install(ctx, projectcatalog.Installation{Manifest: projectcatalog.InstallationDigest([]byte("core isolated fixture"))}))
	scheduler := &projectPreparationProbe{}
	rt, e := NewRuntime(Config{Projects: catalog, Configurations: &configuration.MemoryStore{Projects: catalog}, Scheduler: scheduler, Chapters: struct{ ChapterLog }{}, Schemas: struct{ SchemaStore }{}})
	require.NoError(t, e)
	_, e = rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "unknown", Cell: "test", JobType: "work", Data: jobdb.NewTaskDataOrPanic(nil)}})
	require.ErrorIs(t, e, jobdb.ErrProjectNotFound)
	require.Zero(t, scheduler.calls)
	_, e = rt.schemas.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{TenantId: "unknown", Schema: json.RawMessage(`{"chapterShape":true}`)})
	require.ErrorIs(t, e, jobdb.ErrProjectNotFound)
	_, e = rt.SetTenantConfigurationValue(ctx, jobdb.SetTenantConfigurationValueRequest{TenantID: "unknown", Name: "value", Value: json.RawMessage(`1`)})
	require.ErrorIs(t, e, jobdb.ErrProjectNotFound)
}
