package runtimecore_test

import (
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestRescheduleRequiresExplicitTaskCoordinates(t *testing.T) {
	if _, err := jobdb.RescheduleTaskWait("job:task", nil); err == nil {
		t.Fatal("task route accepted without coordinates")
	}
	task := &jobdb.TaskWait{InputOrdinal: 0, OutputOrdinal: 1, InputHash: "hash", ResumeNeed: "job"}
	if _, err := jobdb.RescheduleTaskWait("job:task", task); err != nil {
		t.Fatal(err)
	}
	if _, err := jobdb.RescheduleTaskWait("job", task); err == nil {
		t.Fatal("job route accepted task coordinates")
	}
}
