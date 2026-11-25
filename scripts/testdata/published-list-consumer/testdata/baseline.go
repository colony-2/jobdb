package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	ctx := celltest.Context(context.Background())
	if os.Args[1] == "serve" {
		runtime, err := sqlite.NewFromConfig(ctx, sqlite.Config{DBPath: os.Args[2], Projects: celltest.UnrestrictedProjects{}})
		must(err)
		handle, err := celltest.Submit(runtime, ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "active", Cell: "test", CellRepo: "example.test/test", JobType: "listed", Data: jobdb.NewTaskDataOrPanic(1)}})
		must(err)
		lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: handle.JobKey, Routes: []jobdb.Route{{JobType: "listed"}}, LeaseDuration: time.Hour})
		must(err)
		if lease == nil {
			panic("missing baseline lease")
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		must(err)
		fmt.Println("http://" + listener.Addr().String())
		must(http.Serve(listener, celltest.Handler(remote.NewServer(runtime))))
		return
	}
	var client *remote.Runtime
	var err error
	if os.Args[2] == "tenants" {
		client, err = remote.New(os.Args[3], nil)
	} else {
		client, err = remote.NewProjectRuntime(os.Args[3], nil)
	}
	must(err)
	page, err := client.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{"tenant"}, JobTypes: []string{"listed"}, CellNames: []string{"test"}, PageSize: 1})
	must(err)
	must(json.NewEncoder(os.Stdout).Encode(page))
}
