package engineconformance_test

import (
	"context"

	"github.com/colony-2/swf-go/pkg/swf"
)

type testJobGetter interface {
	GetJob(context.Context, swf.JobKey) (swf.JobInfo, error)
}

func jobStatusForTest(getter testJobGetter, ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
	job, err := getter.GetJob(ctx, jobKey)
	return job.Status, err
}

func jobResultForTest(getter testJobGetter, ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) {
	job, err := getter.GetJob(ctx, jobKey)
	if err != nil {
		return nil, err
	}
	return swf.ExtractTaskDataResult(job.Data)
}
