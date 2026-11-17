package runtimetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// restartSourceSnapshot owns public observations, never readers, opener closures,
// errors or reader caches. It is private to the restart conformance assertion.
type restartSourceSnapshot struct {
	Info jobdb.JobInfo // Data is captured separately below.
	Data restartDataSnapshot
}
type restartDataSnapshot struct {
	Present       bool
	Outer         restartDataValue
	ResultPresent bool
	Result        restartDataValue
	Outcome       *restartOutcome
}
type restartDataValue struct {
	Envelope        bool
	Kind            string
	Payload         jobdb.Data
	PayloadOutcome  *restartOutcome
	Artifacts       []restartArtifactSnapshot
	ArtifactOutcome *restartOutcome
}
type restartArtifactSnapshot struct {
	Name    string
	Key     jobdb.ArtifactKey
	Content []byte
}
type restartOutcome struct {
	Message                                                               string
	Unfinished, Cancelled, Failed, GoalFailed, ContextCancelled, Deadline bool
	NonRetryablePresent, NonRetryable                                     bool
	Cancellation                                                          *string
	Cause                                                                 *restartOutcome
	App                                                                   *jobdb.AppErrorPayload
	System                                                                *jobdb.SystemErrorPayload
	Timeout                                                               *jobdb.TimeoutPayload
	Goal                                                                  *jobdb.GoalFailure
}

func captureRestartSource(info jobdb.JobInfo) (restartSourceSnapshot, error) {
	data := info.Data
	info.Data = nil
	owned, err := copyRestartValue(info)
	if err != nil {
		return restartSourceSnapshot{}, fmt.Errorf("source job fields: %w", err)
	}
	out := restartSourceSnapshot{Info: owned, Data: restartDataSnapshot{Present: data != nil}}
	if data == nil {
		return out, nil
	}
	result, outcome := jobdb.ExtractTaskDataResult(data)
	out.Data.Outcome, err = captureRestartOutcome(outcome, false, 0)
	if err != nil {
		return restartSourceSnapshot{}, fmt.Errorf("extract source data: %w", err)
	}
	out.Data.Outer, err = captureRestartDataValue(data)
	if err != nil {
		return restartSourceSnapshot{}, fmt.Errorf("outer source data: %w", err)
	}
	out.Data.ResultPresent = result != nil
	if result != nil {
		out.Data.Result, err = captureRestartDataValue(result)
		if err != nil {
			return restartSourceSnapshot{}, fmt.Errorf("extracted source data: %w", err)
		}
	}
	return out, nil
}

func captureRestartDataValue(data jobdb.TaskData) (restartDataValue, error) {
	var out restartDataValue
	if envelope, ok := data.(*jobdb.EnvelopedTaskData); ok && envelope != nil {
		out.Envelope, out.Kind = true, envelope.Kind
	}
	payload, outcome := data.GetData()
	var err error
	out.PayloadOutcome, err = captureRestartOutcome(outcome, false, 0)
	if err != nil {
		return out, fmt.Errorf("payload: %w", err)
	}
	out.Payload, err = copyRestartValue(payload)
	if err != nil {
		return out, err
	}
	artifacts, outcome := data.GetArtifacts()
	out.ArtifactOutcome, err = captureRestartOutcome(outcome, false, 0)
	if err != nil {
		return out, fmt.Errorf("artifact list: %w", err)
	}
	if artifacts != nil {
		out.Artifacts = make([]restartArtifactSnapshot, 0, len(artifacts))
	}
	for i, artifact := range artifacts {
		if artifact == nil {
			return out, fmt.Errorf("artifact %d is nil", i)
		}
		name := artifact.Name()
		key, err := artifact.ArtifactKey()
		if err != nil {
			return out, fmt.Errorf("artifact %q key: %w", name, err)
		}
		reader, err := artifact.Open()
		if err != nil {
			return out, fmt.Errorf("artifact %q open: %w", name, err)
		}
		content, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return out, fmt.Errorf("artifact %q read/close: %w", name, err)
		}
		// SizeBytes is persisted. Size() may change when Open populates a cache.
		out.Artifacts = append(out.Artifacts, restartArtifactSnapshot{Name: name, Key: key, Content: bytes.Clone(content)})
	}
	sort.Slice(out.Artifacts, func(i, j int) bool {
		a, b := out.Artifacts[i], out.Artifacts[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Key.JobId != b.Key.JobId {
			return a.Key.JobId < b.Key.JobId
		}
		if a.Key.TaskOrdinal != b.Key.TaskOrdinal {
			return a.Key.TaskOrdinal < b.Key.TaskOrdinal
		}
		if a.Key.Name != b.Key.Name {
			return a.Key.Name < b.Key.Name
		}
		if a.Key.SizeBytes != b.Key.SizeBytes {
			return a.Key.SizeBytes < b.Key.SizeBytes
		}
		return bytes.Compare(a.Content, b.Content) < 0
	})
	return out, nil
}

func captureRestartOutcome(err error, cause bool, depth int) (*restartOutcome, error) {
	if err == nil {
		return nil, nil
	}
	if depth > 64 {
		return nil, fmt.Errorf("cyclic or excessively nested deferred outcome")
	}
	out := &restartOutcome{Message: err.Error(),
		Unfinished: errors.Is(err, jobdb.ErrJobNotComplete), Cancelled: errors.Is(err, jobdb.ErrJobCancelled),
		Failed: errors.Is(err, jobdb.ErrJobFailed), GoalFailed: errors.Is(err, jobdb.ErrGoalFailed),
		ContextCancelled: errors.Is(err, context.Canceled), Deadline: errors.Is(err, context.DeadlineExceeded)}
	var nr jobdb.NonRetryableError
	if errors.As(err, &nr) {
		out.NonRetryablePresent, out.NonRetryable = true, nr.NonRetryable()
	}
	var cancelled *jobdb.JobCancelledError
	if errors.As(err, &cancelled) && cancelled != nil {
		detail := cancelled.Detail
		out.Cancellation = &detail
	}
	var failed *jobdb.JobFailedError
	if errors.As(err, &failed) && failed != nil {
		var captureErr error
		out.Cause, captureErr = captureRestartOutcome(failed.Cause, true, depth+1)
		if captureErr != nil {
			return nil, captureErr
		}
	}
	var app jobdb.AppError
	if errors.As(err, &app) {
		out.App = &app.Payload
	}
	var system jobdb.SystemError
	if errors.As(err, &system) {
		out.System = &system.Payload
	}
	var timeout jobdb.TimeoutError
	if errors.As(err, &timeout) {
		out.Timeout = &timeout.Payload
	}
	var goal *jobdb.GoalFailedError
	if errors.As(err, &goal) && goal != nil {
		out.Goal = &goal.Failure
	}
	if !cause && !out.Unfinished && !out.Cancelled && !out.Failed && !out.GoalFailed &&
		!out.ContextCancelled && !out.Deadline && out.App == nil && out.System == nil && out.Timeout == nil {
		return nil, fmt.Errorf("unexpected deferred read error: %w", err)
	}
	return copyRestartValue(out)
}

// copyRestartValue preserves concrete scalar types, nil/empty distinctions and
// exact time values. It copies each occurrence independently, even shared input
// pointers. Unknown mutable representations fail rather than escape as aliases.
func copyRestartValue[T any](value T) (T, error) {
	var zero T
	copied, err := copyRestartReflect(reflect.ValueOf(value), 0)
	if err != nil {
		return zero, err
	}
	if !copied.IsValid() {
		return zero, nil
	}
	return copied.Interface().(T), nil
}
func copyRestartReflect(v reflect.Value, depth int) (reflect.Value, error) {
	if !v.IsValid() {
		return v, nil
	}
	if depth > 64 {
		return reflect.Value{}, fmt.Errorf("cyclic or excessively nested snapshot value %s", v.Type())
	}
	if v.Type() == reflect.TypeFor[time.Time]() {
		return v, nil
	}
	out := reflect.New(v.Type()).Elem()
	switch v.Kind() {
	case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		out.Set(v)
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return out, nil
		}
		copied, err := copyRestartReflect(v.Elem(), depth+1)
		if err != nil {
			return reflect.Value{}, err
		}
		if v.Kind() == reflect.Pointer {
			out.Set(reflect.New(v.Type().Elem()))
			out.Elem().Set(copied)
		} else {
			out.Set(copied)
		}
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice {
			if v.IsNil() {
				return out, nil
			}
			out.Set(reflect.MakeSlice(v.Type(), v.Len(), v.Len()))
		}
		for i := 0; i < v.Len(); i++ {
			copied, err := copyRestartReflect(v.Index(i), depth+1)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("[%d]: %w", i, err)
			}
			out.Index(i).Set(copied)
		}
	case reflect.Map:
		if v.IsNil() {
			return out, nil
		}
		if v.Type().Key().Kind() != reflect.String {
			return reflect.Value{}, fmt.Errorf("unsupported map key %s", v.Type())
		}
		out.Set(reflect.MakeMapWithSize(v.Type(), v.Len()))
		iter := v.MapRange()
		for iter.Next() {
			copied, err := copyRestartReflect(iter.Value(), depth+1)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("[%s]: %w", iter.Key(), err)
			}
			out.SetMapIndex(iter.Key(), copied)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if !field.IsExported() {
				return reflect.Value{}, fmt.Errorf("unsupported private field %s.%s", v.Type(), field.Name)
			}
			copied, err := copyRestartReflect(v.Field(i), depth+1)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("%s: %w", field.Name, err)
			}
			out.Field(i).Set(copied)
		}
	default:
		return reflect.Value{}, fmt.Errorf("unsupported snapshot value %s", v.Type())
	}
	return out, nil
}

func captureRestartChapters(chapters []jobdb.Chapter) ([]jobdb.Chapter, error) {
	for _, chapter := range chapters {
		switch chapter.Body.(type) {
		case jobdb.JobStartChapter, *jobdb.JobStartChapter,
			jobdb.JobAttemptOutcomeChapter, *jobdb.JobAttemptOutcomeChapter,
			jobdb.TaskAttemptOutcomeChapter, *jobdb.TaskAttemptOutcomeChapter,
			jobdb.RestartExtraChapter, *jobdb.RestartExtraChapter,
			jobdb.ExecutionChapter, *jobdb.ExecutionChapter:
		default:
			return nil, fmt.Errorf("chapter %d: unsupported body %T", chapter.Ordinal, chapter.Body)
		}
	}
	return copyRestartValue(chapters)
}
