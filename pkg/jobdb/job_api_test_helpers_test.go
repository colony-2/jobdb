package jobdb

import "context"

type testJobGetter interface {
	GetJob(context.Context, JobKey) (JobInfo, error)
}

func jobStatusForTest(getter testJobGetter, ctx context.Context, jobKey JobKey) (JobStatus, error) {
	job, err := getter.GetJob(ctx, jobKey)
	return job.Status, err
}

func jobResultForTest(getter testJobGetter, ctx context.Context, jobKey JobKey) (TaskData, error) {
	job, err := getter.GetJob(ctx, jobKey)
	if err != nil {
		return nil, err
	}
	return ExtractTaskDataResult(job.Data)
}
