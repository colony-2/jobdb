package runtimetest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/configuration"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"github.com/stretchr/testify/require"
)

// Only read ports are implemented. Any attempted storage mutation panics through
// the nil embedded interface, rather than changing the fixture between reads.
type snapshotScheduler struct{ runtimecore.Scheduler }

func (snapshotScheduler) GetJob(_ context.Context, key jobdb.JobKey) (runtimecore.StoredJob, error) {
	return runtimecore.StoredJob{JobKey: key, Store: jobdb.JobStoreArchived, Status: jobdb.JobStatusCancelled,
		Completion: &runtimecore.CompletionSnapshot{Status: "cancelled", Detail: "fixed cancellation"}}, nil
}

type snapshotSchemas struct{ runtimecore.SchemaStore }
type snapshotChapters struct {
	runtimecore.ChapterLog
	chapter runtimecore.EncodedChapter
	opens   *map[string]int
}

func (s snapshotChapters) Count(context.Context, runtimecore.ChapterLogKey) (int64, error) {
	return 1, nil
}
func (s snapshotChapters) Get(_ context.Context, _ runtimecore.ChapterLogKey, ordinal int64) (runtimecore.EncodedChapter, error) {
	if ordinal != 0 {
		return runtimecore.EncodedChapter{}, jobdb.ErrChapterNotFound
	}
	return s.chapter, nil
}
func (s snapshotChapters) OpenArtifact(_ context.Context, lookup runtimecore.ArtifactLookup) (jobdb.ArtifactReader, error) {
	(*s.opens)[lookup.Name]++
	switch lookup.Name {
	case "recipe":
		return jobdb.NewArtifactFromBytes("recipe", []byte("setup: 7\nadd: 2\n")), nil
	case "empty":
		return jobdb.NewArtifactFromBytes("empty", []byte{}), nil
	}
	return nil, fmt.Errorf("unexpected artifact %q", lookup.Name)
}
func TestRestartSourceSnapshotIndependentCoreReads(t *testing.T) {
	opens := map[string]int{}
	chapter, err := runtimecore.EncodeChapter(runtimecore.EncodeChapterRequest{Chapter: jobdb.Chapter{
		Ordinal: 0, TaskType: "recipe", CreatedAt: time.Date(2026, 11, 16, 19, 15, 23, 123, time.UTC),
		Body:      jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`{"original":true}`)}},
		Artifacts: []jobdb.StoredArtifact{{Name: "recipe", Digest: "recipe-digest", Size: 16}, {Name: "empty", Digest: "empty-digest", Size: 0}},
	}})
	require.NoError(t, err)
	rt, err := runtimecore.NewRuntime(runtimecore.Config{Configurations: &configuration.MemoryStore{}, Scheduler: snapshotScheduler{},
		Chapters: snapshotChapters{chapter: chapter, opens: &opens}, Schemas: snapshotSchemas{}})
	require.NoError(t, err)
	key := jobdb.JobKey{TenantId: "tenant", JobId: "source"}
	first, err := rt.GetJob(context.Background(), key)
	require.NoError(t, err)
	second, err := rt.GetJob(context.Background(), key)
	require.NoError(t, err)
	require.False(t, reflect.DeepEqual(first, second), "the original assertion must reproduce the representation mismatch")
	data, err := jobdb.ExtractTaskDataResult(first.Data)
	require.NoError(t, err)
	artifacts, err := data.GetArtifacts()
	require.NoError(t, err)
	require.Zero(t, artifacts[0].Size())
	reader, err := artifacts[0].Open()
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, int64(16), artifacts[0].Size())
	other, _ := jobdb.ExtractTaskDataResult(second.Data)
	otherArtifacts, _ := other.GetArtifacts()
	require.Zero(t, otherArtifacts[0].Size(), "only one reconstructed reader was opened")
	before, err := captureRestartSource(first)
	require.NoError(t, err)
	after, err := captureRestartSource(second)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Len(t, before.Data.Result.Artifacts, 2)
	require.Equal(t, 4, opens["empty"], "both observations of both empty artifacts must be opened")
}

func snapshotTime() time.Time { return time.Date(2026, 11, 16, 19, 15, 23, 123, time.UTC) }
func snapshotFailure() *jobdb.GoalFailure {
	return &jobdb.GoalFailure{Code: "goal_failed", GoalID: "goal", JobID: "job", Message: "same",
		TerminalJobIDs: []string{"terminal"}, NonTerminalJobIDs: []string{"pending"}, FailedAt: snapshotTime()}
}
func snapshotInfo() jobdb.JobInfo {
	deadline := snapshotTime()
	index := 1
	invocation, total := jobdb.Duration(13), jobdb.Duration(17)
	failure := snapshotFailure()
	return jobdb.JobInfo{Cell: "cell", CellID: "id", CellRepo: "repo", NextRoute: &jobdb.Route{JobType: "job", TaskType: "task"},
		CompletionStatus: "done", CompletionDetail: "detail", WaitFor: []string{"wait"},
		BlockingWait: &jobdb.DependencyWaitInfo{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, WaitID: "wait", State: jobdb.DependencyFailed,
			Deadline: &deadline, FailedDependencyIndex: &index, Failure: failure, Dependencies: []jobdb.DependencyResolution{{Target: jobdb.DependencyTarget{JobID: "job", GoalID: "goal"}, Condition: jobdb.JobPrereqSuccess,
				State: jobdb.DependencyFailed, SatisfiedByJobID: "other", BlockedReason: "blocked", Failure: failure}}},
		GoalID: "goal", OtherGoalIDs: []string{"other"}, ClientPayload: json.RawMessage(`{"client":1}`), ClientPayloadRevision: 2,
		ExecutionState: jobdb.ExecutionState{RunPolicy: jobdb.RunPolicy{Retry: jobdb.RetryPolicy{InitialInterval: 1, BackoffCoefficient: 2, MaximumInterval: 3, MaximumAttempts: 4,
			NonRetryableErrorTypes: []string{"type"}}, InvocationTimeout: &invocation, TotalTimeout: &total}, TaskWait: &jobdb.TaskWait{InputOrdinal: 1, OutputOrdinal: 2, InputHash: "hash", ResumeJobType: "resume"}},
		Status: jobdb.JobStatusCompleted, SchemaHash: "schema"}
}

type snapshotChange struct {
	name   string
	change func()
}

// Enumerate each populated leaf and each pointer/slice/map presence transition.
// A fresh fixture is used for every change; map values commit back through parents.
func snapshotChanges(v reflect.Value, path string, commit func()) []snapshotChange {
	var changes []snapshotChange
	add := func(name string, f func()) { changes = append(changes, snapshotChange{name, func() { f(); commit() }}) }
	if v.Type() == reflect.TypeFor[time.Time]() {
		add(path, func() { v.Set(reflect.ValueOf(v.Interface().(time.Time).Add(time.Nanosecond))) })
		return changes
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			changes = append(changes, snapshotChanges(v.Field(i), path+"."+v.Type().Field(i).Name, commit)...)
		}
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		add(path+"/nil", func() { v.SetZero() })
		changes = append(changes, snapshotChanges(v.Elem(), path, commit)...)
	case reflect.Interface:
		if v.IsNil() {
			return nil
		}
		tmp := reflect.New(v.Elem().Type()).Elem()
		tmp.Set(v.Elem())
		changes = append(changes, snapshotChanges(tmp, path, func() { v.Set(tmp); commit() })...)
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		add(path+"/nil", func() { v.SetZero() })
		if v.Len() > 0 {
			add(path+"/empty", func() { v.Set(reflect.MakeSlice(v.Type(), 0, 0)) })
		}
		for i := 0; i < v.Len(); i++ {
			changes = append(changes, snapshotChanges(v.Index(i), fmt.Sprintf("%s[%d]", path, i), commit)...)
		}
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		add(path+"/nil", func() { v.SetZero() })
		add(path+"/empty", func() { v.Set(reflect.MakeMap(v.Type())) })
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
		for _, key := range keys {
			tmp := reflect.New(v.Type().Elem()).Elem()
			tmp.Set(v.MapIndex(key))
			changes = append(changes, snapshotChanges(tmp, path+"["+key.String()+"]", func() { v.SetMapIndex(key, tmp); commit() })...)
		}
	case reflect.String:
		add(path, func() { v.SetString(v.String() + "changed") })
	case reflect.Bool:
		add(path, func() { v.SetBool(!v.Bool()) })
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		add(path, func() { v.SetInt(v.Int() + 1) })
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		add(path, func() { v.SetUint(v.Uint() + 1) })
	case reflect.Float32, reflect.Float64:
		add(path, func() { v.SetFloat(v.Float() + 1) })
	default:
		panic("uncovered mutation type " + v.Type().String())
	}
	return changes
}
func TestRestartSourceSnapshotPublicFields(t *testing.T) {
	typ := reflect.TypeFor[jobdb.JobInfo]()
	var fields []string
	for i := 0; i < typ.NumField(); i++ {
		fields = append(fields, typ.Field(i).Name)
	}
	require.Equal(t, []string{"Cell", "CellID", "CellRepo", "NextRoute", "CompletionStatus", "CompletionDetail", "WaitFor", "BlockingWait", "GoalID", "OtherGoalIDs", "ClientPayload", "ClientPayloadRevision", "ExecutionState", "Status", "Data", "SchemaHash"}, fields)
	fixture := snapshotInfo()
	changes := snapshotChanges(reflect.ValueOf(&fixture).Elem(), "JobInfo", func() {})
	covered := map[string]bool{"Data": true} // Data's independent matrix follows below.
	for i, change := range changes {
		t.Run(change.name, func(t *testing.T) {
			info := snapshotInfo()
			before, err := captureRestartSource(info)
			require.NoError(t, err)
			expected, err := captureRestartSource(snapshotInfo())
			require.NoError(t, err)
			snapshotChanges(reflect.ValueOf(&info).Elem(), "JobInfo", func() {})[i].change()
			after, err := captureRestartSource(info)
			require.NoError(t, err)
			require.Equal(t, expected, before, "baseline changed through an alias")
			require.NotEqual(t, before, after)
		})
		for _, field := range fields {
			if len(change.name) >= len("JobInfo."+field) && change.name[:len("JobInfo."+field)] == "JobInfo."+field {
				covered[field] = true
			}
		}
	}
	for _, field := range fields {
		require.True(t, covered[field], "missing mutation for %s", field)
	}
	// Shared failures must become independent even within the owned baseline.
	owned, err := captureRestartSource(snapshotInfo())
	require.NoError(t, err)
	require.NotSame(t, owned.Info.BlockingWait.Failure, owned.Info.BlockingWait.Dependencies[0].Failure)
}

type snapshotCarrier struct {
	*jobdb.SimpleTaskData
	result                               jobdb.TaskData
	extractErr, payloadErr, artifactsErr error
}

func (c *snapshotCarrier) TaskDataResult() (jobdb.TaskData, error) { return c.result, c.extractErr }
func (c *snapshotCarrier) GetData() (jobdb.Data, error)            { return c.Data, c.payloadErr }
func (c *snapshotCarrier) GetArtifacts() ([]jobdb.Artifact, error) {
	return c.Artifacts, c.artifactsErr
}
func snapshotData(data jobdb.TaskData) jobdb.JobInfo { return jobdb.JobInfo{Data: data} }
func TestRestartSourceSnapshotDataObservations(t *testing.T) {
	plain := func(payload []byte) jobdb.TaskData { return &jobdb.SimpleTaskData{Data: payload} }
	variants := []struct {
		name string
		data jobdb.TaskData
	}{
		{"nil", nil}, {"empty", plain([]byte{})}, {"nil-payload", plain(nil)}, {"json", plain([]byte(`{"a":1}`))}, {"whitespace", plain([]byte(`{ "a":1}`))},
		{"envelope", &jobdb.EnvelopedTaskData{SimpleTaskData: jobdb.SimpleTaskData{Data: []byte(`{"a":1}`)}, Kind: "App"}},
		{"other-kind", &jobdb.EnvelopedTaskData{SimpleTaskData: jobdb.SimpleTaskData{Data: []byte(`{"a":1}`)}, Kind: "Other"}},
		{"absent-result", &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{}}},
		{"unfinished", &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{}, extractErr: jobdb.ErrJobNotComplete, payloadErr: jobdb.ErrJobNotComplete}},
		{"present-result", &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{}, result: plain([]byte("result"))}},
		{"extract-cancelled", &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{}, result: plain(nil), extractErr: jobdb.ErrJobCancelled}},
		{"payload-cancelled", &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{}, result: plain(nil), payloadErr: jobdb.ErrJobCancelled}},
		{"artifacts-cancelled", &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{}, result: plain(nil), artifactsErr: jobdb.ErrJobCancelled}},
		{"returned-bytes-with-error", &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{Data: []byte("kept")}, extractErr: jobdb.ErrJobCancelled, payloadErr: jobdb.ErrJobCancelled}},
	}
	for i, a := range variants {
		t.Run(a.name, func(t *testing.T) {
			before, err := captureRestartSource(snapshotData(a.data))
			require.NoError(t, err)
			for _, b := range variants[i+1:] {
				after, err := captureRestartSource(snapshotData(b.data))
				require.NoError(t, err)
				require.NotEqual(t, before, after, "%s vs %s", a.name, b.name)
			}
		})
	}
	for _, method := range []string{"extraction", "payload", "artifacts"} {
		t.Run("unexpected/"+method, func(t *testing.T) {
			carrier := &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{}}
			switch method {
			case "extraction":
				carrier.extractErr = errors.New("transport")
			case "payload":
				carrier.payloadErr = errors.New("transport")
			case "artifacts":
				carrier.artifactsErr = errors.New("transport")
			}
			_, err := captureRestartSource(snapshotData(carrier))
			require.ErrorContains(t, err, "transport")
		})
	}
	payload := []byte("payload")
	carrier := &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{Data: payload}, result: plain(payload), extractErr: jobdb.ErrJobCancelled, payloadErr: jobdb.ErrJobCancelled}
	before, err := captureRestartSource(snapshotData(carrier))
	require.NoError(t, err)
	expected, err := captureRestartSource(snapshotData(&snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{Data: []byte("payload")}, result: plain([]byte("payload")), extractErr: jobdb.ErrJobCancelled, payloadErr: jobdb.ErrJobCancelled}))
	require.NoError(t, err)
	payload[0] = 'X'
	after, err := captureRestartSource(snapshotData(carrier))
	require.NoError(t, err)
	require.Equal(t, expected, before)
	require.NotEqual(t, before, after)
	// Extraction and outer methods are observed independently of the extracted value.
	result := plain([]byte("one"))
	carrier = &snapshotCarrier{SimpleTaskData: &jobdb.SimpleTaskData{Data: []byte("outer")}, result: result}
	before, err = captureRestartSource(snapshotData(carrier))
	require.NoError(t, err)
	result.(*jobdb.SimpleTaskData).Data = []byte("two")
	after, err = captureRestartSource(snapshotData(carrier))
	require.NoError(t, err)
	require.NotEqual(t, before, after)
}

func snapshotApp() jobdb.AppErrorPayload {
	return jobdb.AppErrorPayload{Message: "same", Level: "warn", Attrs: map[string]any{"nested": map[string]any{"list": []any{json.Number("9007199254740993"), int64(5), []byte("raw")}}, "pointer": &jobdb.InputReference{Ordinal: 4, Hash: "attr"}}, InputRef: &jobdb.InputReference{Ordinal: 1, Hash: "hash"}, Stacktrace: []string{"frame"}}
}
func snapshotSystem() jobdb.SystemErrorPayload {
	return jobdb.SystemErrorPayload{Message: "same", Component: "runner", Code: "code", Retryable: true, InputRef: &jobdb.InputReference{Ordinal: 1, Hash: "hash"}, Stacktrace: []string{"frame"}}
}
func snapshotTimeout() jobdb.TimeoutPayload {
	return jobdb.TimeoutPayload{Message: "same", Scope: "total", After: 13, Retryable: true, InputRef: &jobdb.InputReference{Ordinal: 1, Hash: "hash"}, Kind: "task", Component: "runner", Code: "timeout"}
}
func checkOutcomeChanges[T any](t *testing.T, factory func() T, wrap func(T) error) {
	t.Helper()
	fixture := factory()
	changes := snapshotChanges(reflect.ValueOf(&fixture).Elem(), "outcome", func() {})
	for i, change := range changes {
		t.Run(change.name, func(t *testing.T) {
			value := factory()
			before, err := captureRestartOutcome(wrap(value), false, 0)
			require.NoError(t, err)
			expected, err := captureRestartOutcome(wrap(factory()), false, 0)
			require.NoError(t, err)
			snapshotChanges(reflect.ValueOf(&value).Elem(), "outcome", func() {})[i].change()
			after, err := captureRestartOutcome(wrap(value), false, 0)
			require.NoError(t, err)
			require.Equal(t, expected, before, "baseline aliases the original outcome")
			require.NotEqual(t, before, after)
		})
	}
}
func TestRestartSourceSnapshotOutcomes(t *testing.T) {
	for _, classification := range []struct {
		name string
		err  error
		want restartOutcome
	}{
		{"unfinished", jobdb.ErrJobNotComplete, restartOutcome{Unfinished: true}},
		{"cancelled", jobdb.ErrJobCancelled, restartOutcome{Cancelled: true}},
		{"failed", jobdb.ErrJobFailed, restartOutcome{Failed: true}},
		{"goal-failed", jobdb.ErrGoalFailed, restartOutcome{GoalFailed: true}},
		{"context-cancelled", context.Canceled, restartOutcome{ContextCancelled: true}},
		{"deadline", context.DeadlineExceeded, restartOutcome{Deadline: true}},
	} {
		t.Run("classification/"+classification.name, func(t *testing.T) {
			got, err := captureRestartOutcome(snapshotFixedMessage{error: classification.err}, false, 0)
			require.NoError(t, err)
			classification.want.Message = "same"
			require.Equal(t, &classification.want, got)
		})
	}
	t.Run("app", func(t *testing.T) {
		checkOutcomeChanges(t, snapshotApp, func(p jobdb.AppErrorPayload) error { return &jobdb.JobFailedError{Cause: &jobdb.AppError{Payload: p}} })
	})
	t.Run("system", func(t *testing.T) {
		checkOutcomeChanges(t, snapshotSystem, func(p jobdb.SystemErrorPayload) error {
			return &jobdb.JobFailedError{Cause: &jobdb.SystemError{Payload: p}}
		})
	})
	t.Run("timeout", func(t *testing.T) {
		checkOutcomeChanges(t, snapshotTimeout, func(p jobdb.TimeoutPayload) error {
			return &jobdb.JobFailedError{Cause: &jobdb.TimeoutError{Payload: p}}
		})
	})
	t.Run("goal", func(t *testing.T) {
		checkOutcomeChanges(t, func() jobdb.GoalFailure { return *snapshotFailure() }, func(f jobdb.GoalFailure) error { return &jobdb.GoalFailedError{Failure: f} })
	})
	t.Run("cancelled", func(t *testing.T) {
		checkOutcomeChanges(t, func() jobdb.JobCancelledError { return jobdb.JobCancelledError{Detail: "same"} }, func(e jobdb.JobCancelledError) error { return snapshotFixedMessage{error: &e} })
	})
	for _, pair := range [][2]error{{jobdb.AppError{Payload: snapshotApp()}, &jobdb.AppError{Payload: snapshotApp()}}, {jobdb.SystemError{Payload: snapshotSystem()}, &jobdb.SystemError{Payload: snapshotSystem()}}, {jobdb.TimeoutError{Payload: snapshotTimeout()}, &jobdb.TimeoutError{Payload: snapshotTimeout()}}} {
		a, err := captureRestartOutcome(pair[0], false, 0)
		require.NoError(t, err)
		b, err := captureRestartOutcome(pair[1], false, 0)
		require.NoError(t, err)
		require.Equal(t, a, b)
	}
	variants := []error{
		nil, jobdb.ErrJobNotComplete, jobdb.ErrJobCancelled, jobdb.ErrJobFailed,
		&jobdb.JobCancelledError{Detail: "same"}, &jobdb.JobFailedError{Cause: errors.New("same")},
		jobdb.AppError{Payload: snapshotApp()}, jobdb.SystemError{Payload: snapshotSystem()},
		jobdb.TimeoutError{Payload: snapshotTimeout()}, &jobdb.GoalFailedError{Failure: *snapshotFailure()},
		context.Canceled, context.DeadlineExceeded,
		// Equal messages must not conceal a changed errors.Is classification.
		snapshotFixedMessage{error: jobdb.ErrJobNotComplete},
		snapshotFixedMessage{error: jobdb.ErrJobCancelled},
		snapshotFixedMessage{error: jobdb.ErrJobFailed},
		snapshotFixedMessage{error: context.Canceled},
		snapshotFixedMessage{error: context.DeadlineExceeded},
		&snapshotRetryError{retry: false}, &snapshotRetryError{retry: true},
	}
	for i, a := range variants {
		before, err := captureRestartOutcome(a, false, 0)
		require.NoError(t, err)
		for _, b := range variants[i+1:] {
			after, err := captureRestartOutcome(b, false, 0)
			require.NoError(t, err)
			require.NotEqual(t, before, after)
		}
	}
	// Numeric identity and precision are semantic values, not JSON conversions.
	numeric := snapshotApp()
	before, err := captureRestartOutcome(jobdb.AppError{Payload: numeric}, false, 0)
	require.NoError(t, err)
	list := numeric.Attrs["nested"].(map[string]any)["list"].([]any)
	list[1] = float64(5)
	after, err := captureRestartOutcome(jobdb.AppError{Payload: numeric}, false, 0)
	require.NoError(t, err)
	expected, err := captureRestartOutcome(jobdb.AppError{Payload: snapshotApp()}, false, 0)
	require.NoError(t, err)
	require.Equal(t, expected, before)
	require.NotEqual(t, before, after)
	list[0] = json.Number("9007199254740992")
	afterPrecisionChange, err := captureRestartOutcome(jobdb.AppError{Payload: numeric}, false, 0)
	require.NoError(t, err)
	require.NotEqual(t, after, afterPrecisionChange)

	unsupported := snapshotApp()
	unsupported.Attrs["unsupported"] = func() {}
	_, err = captureRestartOutcome(jobdb.AppError{Payload: unsupported}, false, 0)
	require.ErrorContains(t, err, "unsupported")
}

type snapshotFixedMessage struct{ error }

func (snapshotFixedMessage) Error() string   { return "same" }
func (e snapshotFixedMessage) Unwrap() error { return e.error }

type snapshotRetryError struct{ retry bool }

func (e *snapshotRetryError) Error() string        { return "same" }
func (e *snapshotRetryError) NonRetryable() bool   { return e.retry }
func (e *snapshotRetryError) Is(target error) bool { return target == jobdb.ErrJobFailed }

type snapshotArtifact struct {
	jobdb.Artifact
	name                               string
	key                                jobdb.ArtifactKey
	content                            []byte
	keyErr, openErr, readErr, closeErr error
	opens                              int
}

func (a *snapshotArtifact) Name() string                            { return a.name }
func (a *snapshotArtifact) ArtifactKey() (jobdb.ArtifactKey, error) { return a.key, a.keyErr }
func (a *snapshotArtifact) Size() int64                             { panic("reader cache size is not a persisted observation") }
func (a *snapshotArtifact) Open() (io.ReadCloser, error) {
	a.opens++
	if a.openErr != nil {
		return nil, a.openErr
	}
	return &snapshotReader{Reader: bytes.NewReader(a.content), readErr: a.readErr, closeErr: a.closeErr}, nil
}

type snapshotReader struct {
	*bytes.Reader
	readErr, closeErr error
}

func (r *snapshotReader) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	return r.Reader.Read(p)
}
func (r *snapshotReader) Close() error { return r.closeErr }
func snapshotArtifacts() []*snapshotArtifact {
	return []*snapshotArtifact{
		{name: "recipe", key: jobdb.ArtifactKey{JobId: "job", TaskOrdinal: 2, Name: "persisted", SizeBytes: 123}, content: []byte("recipe")},
		{name: "empty", key: jobdb.ArtifactKey{JobId: "job", TaskOrdinal: 2, Name: "empty", SizeBytes: 0}, content: []byte{}},
	}
}
func snapshotArtifactInfo(artifacts []*snapshotArtifact) jobdb.JobInfo {
	list := make([]jobdb.Artifact, len(artifacts))
	for i, a := range artifacts {
		list[i] = a
	}
	return snapshotData(&jobdb.SimpleTaskData{Artifacts: list})
}
func TestRestartSourceSnapshotArtifacts(t *testing.T) {
	baseline := func() restartSourceSnapshot {
		out, err := captureRestartSource(snapshotArtifactInfo(snapshotArtifacts()))
		require.NoError(t, err)
		return out
	}
	cases := []struct {
		name   string
		mutate func([]*snapshotArtifact) []*snapshotArtifact
	}{
		{"missing", func(a []*snapshotArtifact) []*snapshotArtifact { return a[:1] }},
		{"extra", func(a []*snapshotArtifact) []*snapshotArtifact {
			return append(a, &snapshotArtifact{name: "extra", key: jobdb.ArtifactKey{Name: "extra"}})
		}},
		{"duplicate", func(a []*snapshotArtifact) []*snapshotArtifact { return append(a, a[1]) }},
		{"name", func(a []*snapshotArtifact) []*snapshotArtifact { a[0].name = "renamed"; return a }},
		{"key-job", func(a []*snapshotArtifact) []*snapshotArtifact { a[0].key.JobId = "changed"; return a }},
		{"key-ordinal", func(a []*snapshotArtifact) []*snapshotArtifact { a[0].key.TaskOrdinal++; return a }},
		{"key-name", func(a []*snapshotArtifact) []*snapshotArtifact { a[0].key.Name = "changed"; return a }},
		{"key-persisted-size", func(a []*snapshotArtifact) []*snapshotArtifact { a[0].key.SizeBytes++; return a }},
		{"bytes", func(a []*snapshotArtifact) []*snapshotArtifact { a[0].content[0] = 'X'; return a }},
		{"empty-becomes-populated", func(a []*snapshotArtifact) []*snapshotArtifact { a[1].content = []byte("X"); return a }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			artifacts := snapshotArtifacts()
			before, err := captureRestartSource(snapshotArtifactInfo(artifacts))
			require.NoError(t, err)
			changed := tc.mutate(artifacts)
			after, err := captureRestartSource(snapshotArtifactInfo(changed))
			require.NoError(t, err)
			require.Equal(t, baseline(), before, "baseline retained artifact state")
			require.NotEqual(t, before, after)
		})
	}
	artifacts := snapshotArtifacts()
	before, err := captureRestartSource(snapshotArtifactInfo(artifacts))
	require.NoError(t, err)
	require.Equal(t, 2, artifacts[1].opens, "empty artifact read in outer and extracted observations")
	duplicateInfo := snapshotArtifactInfo(append(artifacts, artifacts[1]))
	withDuplicate, err := captureRestartSource(duplicateInfo)
	require.NoError(t, err)
	require.Len(t, withDuplicate.Data.Outer.Artifacts, 3)
	require.NotEqual(t, before, withDuplicate)
	duplicateInfo.Data.(*jobdb.SimpleTaskData).Artifacts[0], duplicateInfo.Data.(*jobdb.SimpleTaskData).Artifacts[2] = duplicateInfo.Data.(*jobdb.SimpleTaskData).Artifacts[2], duplicateInfo.Data.(*jobdb.SimpleTaskData).Artifacts[0]
	reorderedDuplicate, err := captureRestartSource(duplicateInfo)
	require.NoError(t, err)
	require.Equal(t, withDuplicate, reorderedDuplicate)
	artifacts[0], artifacts[1] = artifacts[1], artifacts[0]
	reordered, err := captureRestartSource(snapshotArtifactInfo(artifacts))
	require.NoError(t, err)
	require.Equal(t, before, reordered)
	for _, operation := range []string{"key", "open", "read", "close"} {
		t.Run("failure/"+operation, func(t *testing.T) {
			artifacts := snapshotArtifacts()
			failure := errors.New("broken " + operation)
			switch operation {
			case "key":
				artifacts[1].keyErr = failure
			case "open":
				artifacts[1].openErr = failure
			case "read":
				artifacts[1].readErr = failure
			case "close":
				artifacts[1].closeErr = failure
			}
			_, err := captureRestartSource(snapshotArtifactInfo(artifacts))
			require.ErrorContains(t, err, "empty")
			require.ErrorContains(t, err, failure.Error())
		})
	}
	// Expected deferred outcomes must not suppress artifact bytes or membership.
	a := snapshotArtifacts()
	carrier := &snapshotCarrier{SimpleTaskData: snapshotArtifactInfo(a).Data.(*jobdb.SimpleTaskData), artifactsErr: jobdb.ErrJobCancelled}
	before, err = captureRestartSource(snapshotData(carrier))
	require.NoError(t, err)
	a[0].content[0] = 'X'
	after, err := captureRestartSource(snapshotData(carrier))
	require.NoError(t, err)
	require.NotEqual(t, before, after)
}

func snapshotChapterFixture() []jobdb.Chapter {
	return []jobdb.Chapter{{Ordinal: 1, TaskType: "type", InputHash: "hash", CreatedAt: snapshotTime(),
		Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte(`{"input":true}`)}},
		Metadata: jobdb.ChapterMetadata{Fields: map[string]jobdb.ChapterMetadataValue{"nested": {Kind: jobdb.ChapterMetadataMap, Map: map[string]jobdb.ChapterMetadataValue{
			"list": {Kind: jobdb.ChapterMetadataList, List: []jobdb.ChapterMetadataValue{{Kind: jobdb.ChapterMetadataString, String: "value", Int: 3, Bool: true, Double: 1.5}}}}}}},
		Artifacts: []jobdb.StoredArtifact{{Name: "recipe", Digest: "digest", Size: 12}, {Name: "empty", Digest: "empty-digest", Size: 0}},
	}}
}
func TestRestartSourceSnapshotExactChapters(t *testing.T) {
	fixture := snapshotChapterFixture()
	changes := snapshotChanges(reflect.ValueOf(&fixture).Elem(), "chapters", func() {})
	for i, change := range changes {
		t.Run(change.name, func(t *testing.T) {
			chapters := snapshotChapterFixture()
			before, err := captureRestartChapters(chapters)
			require.NoError(t, err)
			snapshotChanges(reflect.ValueOf(&chapters).Elem(), "chapters", func() {})[i].change()
			require.Equal(t, snapshotChapterFixture(), before, "chapter baseline changed through an alias")
			require.NotEqual(t, before, chapters, "exact chapter equality must detect the mutation")
		})
	}
	for _, name := range []string{"descriptor-order", "extra-descriptor", "duplicate-descriptor"} {
		t.Run(name, func(t *testing.T) {
			chapters := snapshotChapterFixture()
			before, err := captureRestartChapters(chapters)
			require.NoError(t, err)
			switch name {
			case "descriptor-order":
				chapters[0].Artifacts[0], chapters[0].Artifacts[1] = chapters[0].Artifacts[1], chapters[0].Artifacts[0]
			case "extra-descriptor":
				chapters[0].Artifacts = append(chapters[0].Artifacts, jobdb.StoredArtifact{Name: "extra"})
			case "duplicate-descriptor":
				chapters[0].Artifacts = append(chapters[0].Artifacts, chapters[0].Artifacts[0])
			}
			require.Equal(t, snapshotChapterFixture(), before)
			require.NotEqual(t, before, chapters)
		})
	}
	// Constructors make every expected value independently, without using the
	// copier under test or retaining the mutable input of an earlier case.
	bodies := []func() jobdb.ChapterBody{
		func() jobdb.ChapterBody {
			return jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: []byte("start")}}
		},
		func() jobdb.ChapterBody { return jobdb.ExecutionChapter{Record: json.RawMessage(`{"kind":"header"}`)} },
		func() jobdb.ChapterBody {
			return jobdb.RestartExtraChapter{Output: jobdb.ApplicationOutputBytes{Data: []byte("extra")}}
		},
	}
	for _, outcome := range []func() jobdb.ChapterOutcome{
		func() jobdb.ChapterOutcome {
			return jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte("output")}}
		},
		func() jobdb.ChapterOutcome { return jobdb.AppErrorOutcome{Error: snapshotApp()} },
		func() jobdb.ChapterOutcome { return jobdb.SystemErrorOutcome{Error: snapshotSystem()} },
		func() jobdb.ChapterOutcome { return jobdb.TimeoutOutcome{Timeout: snapshotTimeout()} },
	} {
		for _, pointerOutcome := range []bool{false, true} {
			makeOutcome := func() jobdb.ChapterOutcome {
				value := outcome()
				if !pointerOutcome {
					return value
				}
				pointer := reflect.New(reflect.TypeOf(value))
				pointer.Elem().Set(reflect.ValueOf(value))
				return pointer.Interface().(jobdb.ChapterOutcome)
			}
			bodies = append(bodies,
				func() jobdb.ChapterBody { return jobdb.JobAttemptOutcomeChapter{Outcome: makeOutcome()} },
				func() jobdb.ChapterBody { return jobdb.TaskAttemptOutcomeChapter{Outcome: makeOutcome()} })
		}
	}
	for _, body := range append([]func() jobdb.ChapterBody(nil), bodies...) {
		bodies = append(bodies, func() jobdb.ChapterBody {
			value := body()
			pointer := reflect.New(reflect.TypeOf(value))
			pointer.Elem().Set(reflect.ValueOf(value))
			return pointer.Interface().(jobdb.ChapterBody)
		})
	}
	for i, body := range bodies {
		t.Run(fmt.Sprintf("body-%d-%T", i, body()), func(t *testing.T) {
			factory := func() []jobdb.Chapter { return []jobdb.Chapter{{Body: body()}} }
			fixture := factory()
			changes := snapshotChanges(reflect.ValueOf(&fixture).Elem(), "chapter", func() {})
			for j, change := range changes {
				t.Run(change.name, func(t *testing.T) {
					chapters := factory()
					before, err := captureRestartChapters(chapters)
					require.NoError(t, err)
					snapshotChanges(reflect.ValueOf(&chapters).Elem(), "chapter", func() {})[j].change()
					require.Equal(t, factory(), before)
					require.NotEqual(t, before, chapters)
				})
			}
		})
	}
	_, err := captureRestartChapters([]jobdb.Chapter{{Body: unsupportedSnapshotBody{}}})
	require.ErrorContains(t, err, "unsupported body")
	_, err = captureRestartChapters([]jobdb.Chapter{{Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{Error: jobdb.AppErrorPayload{Attrs: map[string]any{"bad": make(chan int)}}}}}})
	require.ErrorContains(t, err, "unsupported")
}

type unsupportedSnapshotBody struct{ jobdb.ChapterBody }
