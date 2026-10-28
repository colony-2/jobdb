package jobdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/colony-2/jobdb/pkg/internal/executionmeta"
)

const DefaultMaxChapterApplicationPayloadBytes int64 = 256 * 1024
const ChapterApplicationPayloadTooLargeCode = "chapter_application_payload_too_large"

// ChapterApplicationPayloadError describes a rejected write without retaining
// its application bytes. Diagnostics are safe to persist in a terminal error.
type ChapterApplicationPayloadError struct {
	JobKey               *JobKey               `json:"jobKey,omitempty"`
	LogicalID            string                `json:"logicalId,omitempty"`
	RecordKind           string                `json:"recordKind,omitempty"`
	Attempt              int                   `json:"attempt,omitempty"`
	DiagnosticsTruncated bool                  `json:"diagnosticsTruncated,omitempty"`
	LimitBytes           int64                 `json:"limitBytes"`
	ActualBytes          int64                 `json:"actualBytes"`
	ExcessBytes          int64                 `json:"excessBytes"`
	Ordinal              int64                 `json:"ordinal"`
	TaskType             string                `json:"taskType,omitempty"`
	Contributors         []PayloadContribution `json:"contributors,omitempty"`
	OmittedFields        int                   `json:"omittedFields,omitempty"`
	OmittedBytes         int64                 `json:"omittedBytes,omitempty"`
}

var ErrChapterApplicationPayloadTooLarge = errors.New("chapter application payload too large")

func (e *ChapterApplicationPayloadError) Unwrap() error      { return ErrChapterApplicationPayloadTooLarge }
func (e *ChapterApplicationPayloadError) NonRetryable() bool { return true }
func (e *ChapterApplicationPayloadError) Error() string {
	s := fmt.Sprintf("Application payload limit exceeded at chapter %d: %d bytes; limit %d; excess %d. Store large content in artifacts.", e.Ordinal, e.ActualBytes, e.LimitBytes, e.ExcessBytes)
	if e.LogicalID != "" {
		s += fmt.Sprintf(" Invocation %q.", e.LogicalID)
	}
	if len(e.Contributors) > 0 {
		p := e.Contributors[0]
		s += fmt.Sprintf(" Largest payload: %q (%d bytes)", p.Location, p.Bytes)
		if p.JSON != nil {
			for i, f := range p.JSON.Largest {
				if i >= 3 {
					break
				}
				s += fmt.Sprintf("; %q %d bytes (%s)", f.Path, f.Bytes, f.Kind)
			}
		}
	}
	return s
}
func (e *ChapterApplicationPayloadError) AppError() *AppError {
	raw, _ := json.Marshal(e)
	var stats map[string]any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	_ = d.Decode(&stats)
	return &AppError{Payload: AppErrorPayload{Message: e.Error(), Attrs: map[string]any{"code": ChapterApplicationPayloadTooLargeCode, "nonRetryable": true, "payloadSize": stats}}}
}
func IsChapterPayloadFailure(err error) bool {
	if errors.Is(err, ErrChapterApplicationPayloadTooLarge) {
		return true
	}
	var app AppError
	return errors.As(err, &app) && app.Payload.Attrs["code"] == ChapterApplicationPayloadTooLargeCode
}

type PayloadContribution struct {
	Location string            `json:"location"`
	Bytes    int64             `json:"bytes"`
	JSON     *JSONPayloadStats `json:"json,omitempty"`
}
type applicationField struct {
	location string
	data     []byte
}

// ValidateChapterApplicationPayload counts only application fields. Metadata,
// error envelopes, lifecycle structure and artifact descriptors do not count.
func ValidateChapterApplicationPayload(ctx context.Context, ch Chapter, limit int64) error {
	if limit < 0 {
		return fmt.Errorf("chapter application payload limit cannot be negative")
	}
	if limit == 0 {
		limit = DefaultMaxChapterApplicationPayloadBytes
	}
	var fields []applicationField
	var logicalID, recordKind string
	var attempt int
	add := func(path string, data []byte) {
		if len(data) > 0 {
			fields = append(fields, applicationField{path, data})
		}
	}
	outcome := func(path string, o ChapterOutcome) {
		switch x := o.(type) {
		case ApplicationOutputOutcome:
			add(path, x.Output.Data)
		case *ApplicationOutputOutcome:
			if x != nil {
				add(path, x.Output.Data)
			}
		}
	}
	var record func(string, []byte, int) error
	record = func(path string, raw []byte, depth int) error {
		if depth > 64 {
			return fmt.Errorf("execution reconstruction depth exceeds 64")
		}
		r, err := DecodeExecutionRecord(raw)
		if path == "" {
			logicalID, recordKind, attempt = boundedPath(r.ID), r.Kind, r.Attempt
		}
		if err != nil {
			return err
		}
		if r.Input != nil {
			add(path+"input.data", r.Input.Data)
		}
		if r.Output != nil {
			add(path+"output.data", r.Output.Data)
		}
		if r.Kind != "continuation" && r.Kind != "dependency_binding" {
			add(path+"payload", r.Payload)
		}
		if r.Kind == "restart" && len(r.Descriptor) > 0 {
			var rs []json.RawMessage
			if err = json.Unmarshal(r.Descriptor, &rs); err != nil {
				return err
			}
			for i, v := range rs {
				if err = record(fmt.Sprintf("%sretained[%d].", path, i), v, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	var body func(ChapterBody) error
	body = func(b ChapterBody) error {
		switch b := b.(type) {
		case JobStartChapter:
			add("input.data", b.Input.Data)
		case *JobStartChapter:
			if b != nil {
				return body(*b)
			}
		case RestartExtraChapter:
			add("output.data", b.Output.Data)
		case *RestartExtraChapter:
			if b != nil {
				return body(*b)
			}
		case JobAttemptOutcomeChapter:
			outcome("output.data", b.Outcome)
		case *JobAttemptOutcomeChapter:
			if b != nil {
				return body(*b)
			}
		case TaskAttemptOutcomeChapter:
			outcome("output.data", b.Outcome)
		case *TaskAttemptOutcomeChapter:
			if b != nil {
				return body(*b)
			}
		case ExecutionChapter:
			return record("", b.Record, 0)
		case *ExecutionChapter:
			if b != nil {
				return body(*b)
			}
		}
		return nil
	}
	// The reserved top-level input is application data even though the public
	// chapter presents it alongside metadata. User metadata lives under metadata.
	if input, ok := ch.Metadata.Fields["input"]; ok {
		raw, err := json.Marshal(chapterMetadataValueToAny(input))
		if err != nil {
			return fmt.Errorf("encode chapter input: %w", err)
		}
		add("chapter.input", raw)
	}
	if err := body(ch.Body); err != nil {
		return err
	}
	prefix := ch.Metadata.Fields["metadata"].Map[executionmeta.Key].String
	if prefix != "" {
		rs, err := executionmeta.Decode(prefix)
		if err != nil {
			return err
		}
		for i, r := range rs {
			if err = record(fmt.Sprintf("reconstruction[%d].", i), r, 0); err != nil {
				return err
			}
		}
	}
	var total int64
	for _, f := range fields {
		total += int64(len(f.data))
	}
	if total <= limit {
		return nil
	}
	e := &ChapterApplicationPayloadError{LogicalID: logicalID, RecordKind: recordKind, Attempt: attempt, LimitBytes: limit, ActualBytes: total, ExcessBytes: total - limit, Ordinal: ch.Ordinal, TaskType: boundedPath(ch.TaskType)}
	sort.SliceStable(fields, func(i, j int) bool { return len(fields[i].data) > len(fields[j].data) })
	budget := 8 * 1024 * 1024
	tokens := 100000
	paths := 10
	for i, f := range fields {
		if i >= 16 {
			e.OmittedFields++
			e.OmittedBytes += int64(len(f.data))
			continue
		}
		stats := analyzeJSONPayload(ctx, f.data, &budget, &tokens, paths)
		paths -= len(stats.Largest)
		e.Contributors = append(e.Contributors, PayloadContribution{Location: boundedPath(f.location), Bytes: int64(len(f.data)), JSON: stats})
	}
	// Keep the error and its public rendering bounded independently of the
	// application budget. Exact totals survive diagnostic truncation.
	for {
		raw, _ := json.Marshal(e)
		if len(raw)+len(e.Error()) <= 7500 {
			break
		}
		changed := false
		for i := len(e.Contributors) - 1; i >= 0; i-- {
			if e.Contributors[i].JSON != nil {
				e.Contributors[i].JSON = nil
				e.DiagnosticsTruncated = true
				changed = true
				break
			}
		}
		if !changed {
			break
		}
	}
	return e
}

// PayloadFailureChapter is a terminal control outcome, never application data.
func PayloadFailureChapter(ch Chapter, failure *ChapterApplicationPayloadError) Chapter {
	return Chapter{Ordinal: ch.Ordinal, TaskType: ch.TaskType, CreatedAt: ch.CreatedAt, Body: JobAttemptOutcomeChapter{Outcome: AppErrorOutcome{Error: failure.AppError().Payload}}}
}
func ChapterPayloadFailure(ch Chapter) bool {
	body, ok := ch.Body.(JobAttemptOutcomeChapter)
	if !ok {
		return false
	}
	out, ok := body.Outcome.(AppErrorOutcome)
	return ok && out.Error.Attrs["code"] == ChapterApplicationPayloadTooLargeCode
}
