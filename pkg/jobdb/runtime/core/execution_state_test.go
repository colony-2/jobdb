package runtimecore_test

import (
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestRescheduleRequiresExplicitTaskCoordinates(t *testing.T) {
	if _, err := jobdb.RescheduleTaskWait(jobdb.Route{JobType: "job", TaskType: "task"}, nil); err == nil {
		t.Fatal("task route accepted without coordinates")
	}
	task := &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: "hash", ResumeJobType: "job"}
	if _, err := jobdb.RescheduleTaskWait(jobdb.Route{JobType: "job", TaskType: "task"}, task); err != nil {
		t.Fatal(err)
	}
	if _, err := jobdb.RescheduleTaskWait(jobdb.Route{JobType: "job"}, task); err == nil {
		t.Fatal("job route accepted task coordinates")
	}
}
