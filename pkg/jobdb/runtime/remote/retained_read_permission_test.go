package remote_test

// This fixture uses only public interfaces. The publication verifier copies it
// unchanged (apart from the package name) into its downloaded-module consumer.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
)

func retainedCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type retainedCall struct {
	Operation string
	Key       jobdb.JobKey
}
type retainedGate struct {
	jobdb.ConsumerRuntime
	jobdb.ConfigurationAdmin
	jobdb.JobSchemaRegistry
	mu      sync.Mutex
	denied  retainedCall
	failure error
	calls   []retainedCall
}

var _ jobdb.ConsumerRuntime = (*retainedGate)(nil)

func (g *retainedGate) set(op string, key jobdb.JobKey, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.denied = retainedCall{op, key}
	g.failure = err
}
func (g *retainedGate) visit(op string, key jobdb.JobKey) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := retainedCall{op, key}
	g.calls = append(g.calls, c)
	if c == g.denied {
		return g.failure
	}
	return nil
}
func (g *retainedGate) trace() []retainedCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]retainedCall(nil), g.calls...)
}
func (g *retainedGate) GetJob(ctx context.Context, k jobdb.JobKey) (jobdb.JobInfo, error) {
	if err := g.visit("job", k); err != nil {
		return jobdb.JobInfo{}, err
	}
	return g.ConsumerRuntime.GetJob(ctx, k)
}
func (g *retainedGate) ListChapters(ctx context.Context, q jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	if err := g.visit("list", q.JobKey); err != nil {
		return nil, err
	}
	return g.ConsumerRuntime.ListChapters(ctx, q)
}
func (g *retainedGate) GetChapter(ctx context.Context, q jobdb.ChapterRef) (jobdb.Chapter, error) {
	if err := g.visit("chapter", q.JobKey); err != nil {
		return jobdb.Chapter{}, err
	}
	return g.ConsumerRuntime.GetChapter(ctx, q)
}
func (g *retainedGate) OpenArtifact(ctx context.Context, q jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	if err := g.visit("artifact", q.JobKey); err != nil {
		return nil, err
	}
	r, err := g.ConsumerRuntime.OpenArtifact(ctx, q)
	if err != nil {
		return nil, err
	}
	return retainedReader{r, g, q.JobKey}, nil
}

type retainedReader struct {
	jobdb.ArtifactReader
	gate *retainedGate
	key  jobdb.JobKey
}

func (r retainedReader) Open() (io.ReadCloser, error) {
	if err := r.gate.visit("open", r.key); err != nil {
		return nil, err
	}
	return r.ArtifactReader.Open()
}

type retainedFixture struct {
	backend            *sqlite.Runtime
	gate               *retainedGate
	ctx                context.Context
	selected, producer jobdb.JobKey
	payload            []byte
}

func newRetainedFixture(t *testing.T) retainedFixture {
	t.Helper()
	ctx := celltest.Context(context.Background())
	cfg := sqlite.Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "retained.db"), MaxInlineArtifactBytes: 1}
	r, err := sqlite.NewFromConfig(ctx, cfg)
	retainedCheck(t, err)
	_, err = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "reads", Repo: "example.test/reads"})
	retainedCheck(t, err)
	payload := bytes.Repeat([]byte{0, 255, 1, 2, 3, 10, 128}, 4096)
	keys := make([]jobdb.JobKey, 0, 2)
	for i, name := range []string{"selected", "producer"} {
		content := payload
		if i == 0 {
			content = []byte("selected-job-decoy")
		}
		h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "reads", JobType: "read", Data: jobdb.NewTaskDataOrPanic(name, jobdb.NewArtifactFromBytes("same.bin", content))}})
		retainedCheck(t, err)
		lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "fixture", Routes: []jobdb.Route{{JobType: "read"}}, LeaseDuration: time.Minute})
		retainedCheck(t, err)
		if lease == nil {
			t.Fatal("missing fixture lease")
		}
		final := jobdb.Chapter{Ordinal: 1, TaskType: "read", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"retained-result"`)}}}}
		retainedCheck(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &final}))
		keys = append(keys, h.JobKey)
	}
	format := func() int {
		db, err := sql.Open("sqlite", cfg.DBPath)
		retainedCheck(t, err)
		defer db.Close()
		var v int
		retainedCheck(t, db.QueryRow("PRAGMA user_version").Scan(&v))
		return v
	}
	originalFormat := format()
	if originalFormat != 13 {
		t.Fatalf("unexpected supported storage format: %d", originalFormat)
	}
	// Reopen the retained database before any authorization test. No conversion.
	retainedCheck(t, r.Close(ctx))
	r, err = sqlite.NewFromConfig(ctx, cfg)
	retainedCheck(t, err)
	t.Cleanup(func() { retainedCheck(t, r.Close(ctx)) })
	if format() != originalFormat {
		t.Fatal("reopening converted storage")
	}
	gate := &retainedGate{ConsumerRuntime: r, ConfigurationAdmin: r, JobSchemaRegistry: r}
	if _, ok := reflect.TypeOf(gate).MethodByName("AddGoalCandidate"); ok {
		t.Fatal("decorator exposed retired mutation")
	}
	return retainedFixture{r, gate, ctx, keys[0], keys[1], payload}
}

// Normalize lazy task data, so snapshots compare records and bytes rather than
// backend/client implementations. Chapters include ordering and descriptors.
func retainedJob(t *testing.T, info jobdb.JobInfo, want string) []byte {
	t.Helper()
	if info.Status != jobdb.JobStatusCompleted || info.CompletionStatus != "success" {
		t.Fatalf("terminal record: %+v", info)
	}
	data, err := info.Data.GetData()
	retainedCheck(t, err)
	raw, err := json.Marshal(data)
	retainedCheck(t, err)
	if string(raw) != fmt.Sprintf("%q", want) {
		t.Fatalf("input changed: %s", raw)
	}
	info.Data = nil
	// The wire decoder materializes empty lists; nil and empty carry the same
	// public meaning. Compare normalized logical metadata across both adapters.
	if len(info.WaitFor) == 0 {
		info.WaitFor = nil
	}
	if len(info.OtherGoalIDs) == 0 {
		info.OtherGoalIDs = nil
	}
	out, err := json.Marshal(struct {
		Info jobdb.JobInfo
		Data json.RawMessage
	}{info, raw})
	retainedCheck(t, err)
	return out
}
func retainedArtifact(t *testing.T, rt jobdb.ConsumerRuntime, ctx context.Context, k jobdb.JobKey) ([]byte, error) {
	t.Helper()
	reader, err := rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: k, Ordinal: 0, Name: "same.bin"})
	if err != nil {
		if reader != nil {
			t.Fatal("denied lookup returned reader")
		}
		return nil, err
	}
	stream, err := reader.Open()
	if err != nil {
		if stream != nil {
			stream.Close()
			t.Fatal("denied open returned stream")
		}
		return nil, err
	}
	defer stream.Close()
	b, err := io.ReadAll(stream)
	if err == nil && (reader.Name() != "same.bin" || reader.Size() != int64(len(b))) {
		t.Fatal("artifact descriptor changed")
	}
	return b, err
}
func (f retainedFixture) snapshot(t *testing.T) []byte {
	t.Helper()
	var out [][]byte
	for _, k := range []jobdb.JobKey{f.selected, f.producer} {
		info, err := f.backend.GetJob(f.ctx, k)
		retainedCheck(t, err)
		out = append(out, retainedJob(t, info, "retained-result"))
		chapters, err := f.backend.ListChapters(f.ctx, jobdb.ListChaptersRequest{JobKey: k})
		retainedCheck(t, err)
		if len(chapters) != 2 || chapters[0].Ordinal != 0 || chapters[1].Ordinal != 1 || len(chapters[0].Artifacts) != 1 {
			t.Fatalf("retained chapters: %+v", chapters)
		}
		b, err := json.Marshal(chapters)
		retainedCheck(t, err)
		out = append(out, b)
		b, err = retainedArtifact(t, f.backend, f.ctx, k)
		retainedCheck(t, err)
		out = append(out, b)
	}
	b, err := json.Marshal(out)
	retainedCheck(t, err)
	return b
}

type retainedTransport func(*http.Request) (*http.Response, error)

func (f retainedTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRetainedReadPermissionDenied(t *testing.T) {
	f := newRetainedFixture(t)
	before := f.snapshot(t)
	server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(f.gate)))
	defer server.Close()
	var status int
	var raw []byte
	client, err := remote.New(server.URL, &http.Client{Transport: retainedTransport(func(req *http.Request) (*http.Response, error) {
		resp, err := server.Client().Transport.RoundTrip(req)
		if err != nil {
			return nil, err
		}
		status = resp.StatusCode
		raw, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewReader(raw))
		return resp, err
	})})
	retainedCheck(t, err)
	for _, mode := range []string{"direct", "http"} {
		rt := jobdb.ConsumerRuntime(f.gate)
		if mode == "http" {
			rt = client
		}
		for _, wrapped := range []bool{false, true} {
			for _, op := range []string{"job", "list", "chapter", "artifact", "open"} {
				t.Run(fmt.Sprintf("%s/%s/wrapped=%t", mode, op, wrapped), func(t *testing.T) {
					t.Cleanup(func() { f.gate.set("", jobdb.JobKey{}, nil) })
					key := f.selected
					if op == "artifact" || op == "open" {
						key = f.producer
					}
					readFrom := func(reader jobdb.ConsumerRuntime) ([]byte, error) {
						switch op {
						case "job":
							info, err := reader.GetJob(f.ctx, key)
							if err != nil {
								if !reflect.DeepEqual(info, jobdb.JobInfo{}) {
									t.Fatal("denied metadata returned")
								}
								return nil, err
							}
							return retainedJob(t, info, "retained-result"), nil
						case "list":
							chapters, err := reader.ListChapters(f.ctx, jobdb.ListChaptersRequest{JobKey: key})
							if err != nil {
								if len(chapters) != 0 {
									t.Fatal("denied chapters returned")
								}
								return nil, err
							}
							return json.Marshal(chapters)
						case "chapter":
							chapter, err := reader.GetChapter(f.ctx, jobdb.ChapterRef{JobKey: key, Ordinal: 0})
							if err != nil {
								if !reflect.DeepEqual(chapter, jobdb.Chapter{}) {
									t.Fatal("denied chapter returned")
								}
								return nil, err
							}
							return json.Marshal(chapter)
						default:
							return retainedArtifact(t, reader, f.ctx, key)
						}
					}
					read := func() ([]byte, error) { return readFrom(rt) }
					// A previously authorized selected-job check cannot authorize future reads.
					_, err := rt.GetJob(f.ctx, f.selected)
					retainedCheck(t, err)
					expectedResult, err := readFrom(f.backend)
					retainedCheck(t, err)
					allowed, err := read()
					retainedCheck(t, err)
					if !bytes.Equal(allowed, expectedResult) {
						t.Fatalf("authorized %s result differs from retained backend: got %s want %s", op, allowed, expectedResult)
					}
					if (op == "artifact" || op == "open") && !bytes.Equal(allowed, f.payload) {
						t.Fatal("wrong original-producer bytes")
					}
					failure := error(jobdb.ErrLeasePermissionDenied)
					if wrapped {
						failure = fmt.Errorf("private-wrapper-marker: %w", failure)
					}
					f.gate.set(op, key, failure)
					if key == f.producer {
						_, err := rt.GetJob(f.ctx, f.selected)
						retainedCheck(t, err)
					}
					start := len(f.gate.trace())
					status = 0
					raw = nil
					denied, err := read()
					if !errors.Is(err, jobdb.ErrLeasePermissionDenied) || jobdb.IsExecutionInfrastructureError(err) || errors.Is(err, jobdb.ErrExecutionLeaseLost) || len(denied) != 0 {
						t.Fatalf("denial identity/data: %v %q", err, denied)
					}
					expected := []retainedCall{{op, key}}
					if op == "open" {
						expected = []retainedCall{{"artifact", key}, {"open", key}}
					}
					if got := f.gate.trace()[start:]; !reflect.DeepEqual(got, expected) {
						t.Fatalf("retry/reference changed: got %+v want %+v", got, expected)
					}
					if mode == "http" {
						var envelope struct{ Code, Message string }
						retainedCheck(t, json.Unmarshal(raw, &envelope))
						if status != 403 || envelope.Code != "lease_permission_denied" || envelope.Message != jobdb.ErrLeasePermissionDenied.Error() {
							t.Fatalf("unsafe HTTP denial: %d %s", status, raw)
						}
						var fields map[string]json.RawMessage
						retainedCheck(t, json.Unmarshal(raw, &fields))
						if len(fields) != 2 {
							t.Fatalf("unexpected denial data: %s", raw)
						}
					}
					f.gate.set("", jobdb.JobKey{}, nil)
					restored, err := read()
					retainedCheck(t, err)
					if !bytes.Equal(allowed, restored) {
						t.Fatal("authorized result changed after revocation")
					}
					if !bytes.Equal(before, f.snapshot(t)) {
						t.Fatal("reads mutated retained records or artifact bytes")
					}
				})
			}
		}
	}
}

func TestRetainedReadInfrastructureControl(t *testing.T) {
	f := newRetainedFixture(t)
	source := f.selected
	failure := jobdb.ClassifyExecutionInfrastructure(errors.New("private-storage-cause"), jobdb.ExecutionInfrastructureRuntimeIO, "get_job", &source, nil)
	f.gate.set("job", source, failure)
	server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(f.gate)))
	defer server.Close()
	client, err := remote.New(server.URL, server.Client())
	retainedCheck(t, err)
	_, err = client.GetJob(f.ctx, source)
	var detail *jobdb.ExecutionInfrastructureError
	if !errors.As(err, &detail) || errors.Is(err, jobdb.ErrLeasePermissionDenied) || detail.Reason != jobdb.ExecutionInfrastructureRuntimeIO || detail.Operation != "get_job" || detail.JobKey == nil || *detail.JobKey != source {
		t.Fatalf("infrastructure diagnostic changed: %v", err)
	}
	resp, err := server.Client().Get(server.URL + "/v1/tenants/tenant/jobs/" + source.JobId)
	retainedCheck(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	retainedCheck(t, err)
	if resp.StatusCode != 500 || bytes.Contains(b, []byte("private-storage-cause")) {
		t.Fatalf("unsafe infrastructure response: %d %s", resp.StatusCode, b)
	}
}

func TestRetainedReaderRevocation(t *testing.T) {
	f := newRetainedFixture(t)
	ref := jobdb.ArtifactRef{JobKey: f.producer, Ordinal: 0, Name: "same.bin"}
	reader, err := f.gate.OpenArtifact(f.ctx, ref)
	retainedCheck(t, err)
	f.gate.set("open", f.producer, fmt.Errorf("private-after-lookup: %w", jobdb.ErrLeasePermissionDenied))
	stream, err := reader.Open()
	if stream != nil {
		stream.Close()
		t.Fatal("revoked reader yielded stream")
	}
	if !errors.Is(err, jobdb.ErrLeasePermissionDenied) {
		t.Fatalf("revocation after lookup lost: %v", err)
	}
	f.gate.set("", jobdb.JobKey{}, nil)
	stream, err = reader.Open()
	retainedCheck(t, err)
	b, err := io.ReadAll(stream)
	retainedCheck(t, err)
	retainedCheck(t, stream.Close())
	if !bytes.Equal(b, f.payload) {
		t.Fatal("restored reader changed bytes")
	}
}
