package swf

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type runtimeListedTaskHandle struct {
	runtime       WorkflowRuntime
	jobKey        JobKey
	metadata      json.RawMessage
	createdAt     time.Time
	capability    string
	resumeNeed    string
	taskType      string
	inputOrdinal  int64
	outputOrdinal int64
	inputHash     string
}

func findWaitingTasksFromRuntime(ctx context.Context, runtime WorkflowRuntime, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(tenantIds) == 0 {
		return nil, fmt.Errorf("tenant_ids is required for task discovery")
	}
	capability := workerCapability(jobType, taskType)
	pageToken := ""
	handles := make([]TaskHandle, 0)
	for {
		resp, err := runtime.ListJobs(ctx, ListJobsRequest{
			TenantIds: tenantIds,
			Statuses:  []JobStatus{JobStatusReady},
			JobTasks:  []JobTaskFilter{{JobType: jobType, TaskType: taskType}},
			PageSize:  MaxListJobsPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		for _, job := range resp.Jobs {
			if currentNeedFromSummary(job) != capability {
				continue
			}
			handle, ok := taskHandleFromJobSummary(runtime, job)
			if !ok {
				continue
			}
			handles = append(handles, handle)
		}
		if resp.NextPageToken == "" {
			return handles, nil
		}
		pageToken = resp.NextPageToken
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
		handle, ok := taskHandleFromJobSummary(runtime, job)
		if !ok {
			break
		}
		return handle, nil
	}
	return nil, ErrJobNotFound
}

func taskHandleFromJobSummary(runtime WorkflowRuntime, summary JobSummary) (TaskHandle, bool) {
	if summary.TaskWaitInput == nil || summary.TaskWaitOutput == nil || summary.TaskWaitNext == nil || summary.NextNeed == nil {
		return nil, false
	}
	capability := *summary.NextNeed
	if capability == "" || !strings.Contains(capability, ":") {
		return nil, false
	}
	resumeNeed := *summary.TaskWaitNext
	if resumeNeed == "" {
		return nil, false
	}
	taskType := taskTypeFromCapability(capability)
	if taskType == "" || taskType == capability {
		return nil, false
	}
	inputHash := ""
	if summary.TaskWaitInputHash != nil {
		inputHash = *summary.TaskWaitInputHash
	}
	return &runtimeListedTaskHandle{
		runtime:       runtime,
		jobKey:        summary.JobKey,
		metadata:      append(json.RawMessage(nil), summary.Metadata...),
		createdAt:     summary.CreatedAt,
		capability:    capability,
		resumeNeed:    resumeNeed,
		taskType:      taskType,
		inputOrdinal:  *summary.TaskWaitInput,
		outputOrdinal: *summary.TaskWaitOutput,
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
	return storedChapterToTaskData(h.runtime, h.jobKey, chapter)
}

func (h *runtimeListedTaskHandle) Finish(ctx context.Context, taskData TaskData) error {
	return h.runtime.CompleteTaskIfWaiting(ctx, CompleteTaskIfWaitingRequest{
		JobKey:        h.jobKey,
		Capability:    h.capability,
		ResumeNeed:    h.resumeNeed,
		InputOrdinal:  h.inputOrdinal,
		OutputOrdinal: h.outputOrdinal,
		InputHash:     h.inputHash,
		Data:          taskData,
	})
}

func currentNeedFromSummary(job JobSummary) string {
	if job.NextNeed == nil {
		return ""
	}
	return *job.NextNeed
}
