package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type runtimeListedTaskHandle struct {
	runtime       WorkflowRuntime
	jobKey        JobKey
	metadata      json.RawMessage
	createdAt     time.Time
	route         Route
	resumeJobType string
	taskType      string
	inputOrdinal  int64
	outputOrdinal int64
	inputHash     string
}

func findWaitingTasksFromRuntime(ctx context.Context, runtime WorkflowRuntime, req FindTasksWaitingRequest) ([]TaskHandle, error) {
	if err := (Route{JobType: req.JobType, TaskType: req.TaskType}).Validate(); err != nil {
		return nil, err
	}
	if req.TaskType == "" {
		return nil, fmt.Errorf("task type required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if len(req.TenantIds) == 0 {
		return nil, fmt.Errorf("tenant_ids is required for task discovery")
	}
	route := workerRoute(req.JobType, req.TaskType)
	pageToken := ""
	handles := make([]TaskHandle, 0)
	remaining := req.Limit
	for {
		pageSize := MaxListJobsPageSize
		if remaining > 0 && remaining < pageSize {
			pageSize = remaining
		}
		resp, err := runtime.ListJobs(ctx, ListJobsRequest{
			TenantIds:      req.TenantIds,
			Statuses:       []JobStatus{JobStatusReady},
			JobTasks:       []JobTaskFilter{{JobType: req.JobType, TaskType: req.TaskType}},
			MetadataFilter: req.MetadataFilter,
			PageSize:       pageSize,
			PageToken:      pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, job := range resp.Jobs {
			if currentNeedFromSummary(job) != route {
				continue
			}
			info, err := runtime.GetJob(ctx, job.JobKey)
			if err != nil {
				return nil, err
			}
			if info.NextRoute != nil && *info.NextRoute != route {
				continue // The task changed between listing and inspection.
			}
			handle, ok := taskHandleFromJobSummary(runtime, job, info)
			if !ok {
				continue
			}
			handles = append(handles, handle)
			if remaining > 0 && len(handles) >= req.Limit {
				return handles, nil
			}
		}
		if resp.NextPageToken == "" {
			return handles, nil
		}
		pageToken = resp.NextPageToken
		if remaining > 0 {
			remaining = req.Limit - len(handles)
			if remaining <= 0 {
				return handles, nil
			}
		}
	}
}

func getWaitingTaskFromRuntime(ctx context.Context, runtime WorkflowRuntime, key JobKey) (TaskHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	resp, err := runtime.ListJobs(ctx, ListJobsRequest{
		TenantIds: []string{key.TenantId},
		JobKeys:   []JobKey{key},
		PageSize:  1,
	})
	if err != nil {
		return nil, err
	}
	for _, job := range resp.Jobs {
		if job.JobKey != key {
			continue
		}
		info, err := runtime.GetJob(ctx, job.JobKey)
		if err != nil {
			return nil, err
		}
		handle, ok := taskHandleFromJobSummary(runtime, job, info)
		if !ok {
			break
		}
		return handle, nil
	}
	return nil, ErrJobNotFound
}

func taskHandleFromJobSummary(runtime WorkflowRuntime, summary JobSummary, info jobdb.JobInfo) (TaskHandle, bool) {
	wait := info.ExecutionState.TaskWait
	routeSnapshot := info.NextRoute
	if routeSnapshot == nil {
		routeSnapshot = summary.NextRoute
	} // Older servers omit the additive detail field.
	if wait == nil || routeSnapshot == nil || routeSnapshot.TaskType == "" {
		return nil, false
	}
	route := *routeSnapshot
	resumeJobType := wait.ResumeJobType
	if resumeJobType == "" {
		return nil, false
	}
	taskType := route.TaskType
	inputHash := wait.InputHash
	return &runtimeListedTaskHandle{
		runtime:       runtime,
		jobKey:        summary.JobKey,
		metadata:      append(json.RawMessage(nil), summary.Metadata...),
		createdAt:     summary.CreatedAt,
		route:         route,
		resumeJobType: resumeJobType,
		taskType:      taskType,
		inputOrdinal:  wait.InputOrdinal,
		outputOrdinal: wait.OutputOrdinal,
		inputHash:     inputHash,
	}, true
}

func (h *runtimeListedTaskHandle) JobKey() JobKey               { return h.jobKey }
func (h *runtimeListedTaskHandle) TaskOrdinalToComplete() int64 { return h.outputOrdinal }
func (h *runtimeListedTaskHandle) TaskType() string             { return h.taskType }
func (h *runtimeListedTaskHandle) CreatedAt() time.Time         { return h.createdAt }
func (h *runtimeListedTaskHandle) Metadata() json.RawMessage {
	return append(json.RawMessage(nil), h.metadata...)
}

func (h *runtimeListedTaskHandle) Data() (TaskData, error) {
	chapter, err := h.runtime.GetChapter(context.Background(), ChapterRef{
		JobKey:  h.jobKey,
		Ordinal: h.inputOrdinal,
	})
	if err != nil {
		return nil, err
	}
	if body, ok := chapter.Body.(ExecutionChapter); ok {
		record, err := jobdb.DecodeExecutionRecord(body.Record)
		if err != nil {
			return nil, err
		}
		if record.Kind != "wait" || record.Input == nil {
			return nil, fmt.Errorf("waiting task has no input snapshot")
		}
		return snapshotData(record.Input, h.runtime, h.jobKey.TenantId), nil
	}
	return chapterToTaskData(h.runtime, h.jobKey, chapter)
}

func (h *runtimeListedTaskHandle) Finish(ctx context.Context, taskData TaskData) error {
	return h.runtime.CompleteTaskIfWaiting(ctx, CompleteTaskIfWaitingRequest{
		JobKey:        h.jobKey,
		Route:         h.route,
		ResumeJobType: h.resumeJobType,
		InputOrdinal:  h.inputOrdinal,
		OutputOrdinal: h.outputOrdinal,
		InputHash:     h.inputHash,
		Data:          taskData,
	})
}

func currentNeedFromSummary(job JobSummary) Route {
	if job.NextRoute == nil {
		return Route{}
	}
	return *job.NextRoute
}
