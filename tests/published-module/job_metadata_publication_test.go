package publication_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/projectcatalog"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
)

func TestPublishedJobMetadata(t *testing.T) {
	ctx := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{
		Principal: "consumer", TenantID: "c2",
		Permissions: map[string]bool{"cells.create": true, "jobs.submit": true, "jobs.list": true},
	})
	admin := jobdb.WithProjectAdministration(ctx, func(context.Context) error { return nil })
	catalog := &projectcatalog.Service{Store: &projectcatalog.MemoryStore{}}
	if err := catalog.Install(admin, projectcatalog.Installation{Manifest: projectcatalog.InstallationDigest([]byte("metadata smoke")), Seeds: []projectcatalog.Seed{{ID: "c2", Name: "c2"}}}); err != nil {
		t.Fatal(err)
	}
	runtime := toy.New(toy.WithProjects(catalog))
	cell, err := runtime.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "c2", Name: "test", Repo: "example.test/test"})
	if err != nil {
		t.Fatal(err)
	}
	job, err := runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "c2", Cell: cell.Name, JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	if err != nil {
		t.Fatal(err)
	}
	var writer jobdb.JobMetadataRuntime = runtime
	var zero jobdb.MetadataVersion
	const value = `{"score":9007199254740993,"note":null}`
	check := func(entry jobdb.JobMetadataEntry, err error) {
		t.Helper()
		if err != nil || entry.Version != 1 || !entry.Present || string(entry.Value) != value {
			t.Fatalf("entry=%+v (%s), err=%v", entry, entry.Value, err)
		}
	}
	check(writer.EditJobMetadata(ctx, job.JobKey, jobdb.JobMetadataEdit{Key: "annotation", ExpectedVersion: &zero, Set: json.RawMessage(value)}))
	info, err := runtime.GetJob(ctx, job.JobKey)
	check(info.ExtendedMetadata["annotation"], err)
	page, err := runtime.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"c2"}})
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].JobKey != job.JobKey {
		t.Fatalf("jobs=%+v, err=%v", page.Jobs, err)
	}
	check(page.Jobs[0].ExtendedMetadata["annotation"], nil)
	t.Logf("wrote annotation and read identical JSON at version 1 in detail and list")
}
