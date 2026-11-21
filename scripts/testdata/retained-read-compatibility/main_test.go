package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"debug/buildinfo"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
)

const baselineCommit = "f885723cc03ce9c8b52365390dca9601109d2c7e"
const baselineVersion = "v0.0.94"
const baselineModule = "github.com/colony-2/jobdb"

//go:embed testdata/baseline.go.txt
var baselineSource string

func compatCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

type baselineRequest struct {
	Mode, URL, Path, Operation string
	Key                        jobdb.JobKey
	Denied                     bool
	Expected                   []byte
}

func baselineBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	compatCheck(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.invalid/jobdb-baseline\n\ngo 1.25.5\n\nrequire "+baselineModule+" "+baselineVersion+"\n"), 0600))
	compatCheck(t, os.WriteFile(filepath.Join(dir, "main.go"), []byte(baselineSource), 0600))
	// This helper is separately compiled; no local module, workspace, vendor or
	// modfile override may supply its dependency. Transport configuration survives.
	env := []string{}
	for _, v := range os.Environ() {
		k := strings.SplitN(v, "=", 2)[0]
		if k != "GOWORK" && k != "GOFLAGS" && k != "GOTOOLCHAIN" && k != "GOPROXY" && k != "GOPRIVATE" {
			env = append(env, v)
		}
	}
	env = append(env, "GOWORK=off", "GOFLAGS=", "GOTOOLCHAIN=local", "GOPROXY=direct", "GOPRIVATE=github.com/colony-2/*")
	run := func(args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Dir = dir
		cmd.Env = env
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go %v: %v\n%s", args, err, b)
		}
		return b
	}
	run("mod", "tidy")
	var resolved struct {
		Path, Version string
		Replace       any
		Origin        struct{ VCS, URL, Hash string }
	}
	compatCheck(t, json.Unmarshal(run("list", "-m", "-json", baselineModule+"@"+baselineCommit), &resolved))
	if resolved.Path != baselineModule || resolved.Version != baselineVersion || resolved.Replace != nil || resolved.Origin.Hash != baselineCommit || resolved.Origin.VCS != "git" || strings.TrimSpace(resolved.Origin.URL) == "" {
		t.Fatalf("wrong baseline resolution: %+v", resolved)
	}

	var downloaded struct {
		Path, Version string
		Replace       any
		Origin        struct{ VCS, URL, Hash string }
	}
	compatCheck(t, json.Unmarshal(run("mod", "download", "-json", baselineModule+"@"+baselineVersion), &downloaded))
	if downloaded.Path != baselineModule || downloaded.Version != baselineVersion || downloaded.Replace != nil || downloaded.Origin.Hash != baselineCommit || downloaded.Origin.VCS != "git" || strings.TrimSpace(downloaded.Origin.URL) == "" {
		t.Fatalf("wrong baseline download: %+v", downloaded)
	}
	binary := filepath.Join(dir, "baseline-helper")
	run("build", "-o", binary, ".")
	build, err := buildinfo.ReadFile(binary)
	compatCheck(t, err)
	found := false
	for _, dep := range build.Deps {
		if dep.Replace != nil {
			t.Fatalf("baseline binary replacement: %+v", dep)
		}
		if dep.Path == baselineModule {
			found = dep.Version == baselineVersion
		}
	}
	if !found {
		t.Fatalf("baseline binary did not link exact module: %+v", build)
	}
	t.Logf("baseline client: %s@%s (%s)", baselineModule, baselineVersion, baselineCommit)
	return binary
}
func runBaseline(t *testing.T, binary string, q baselineRequest) []byte {
	t.Helper()
	b, err := json.Marshal(q)
	compatCheck(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary)
	cmd.Stdin = bytes.NewReader(b)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("baseline %s/%s denied=%t: %v\n%s", q.Mode, q.Operation, q.Denied, err, out)
	}
	return out
}

type compatGate struct {
	jobdb.ConsumerRuntime
	jobdb.ConfigurationAdmin
	jobdb.JobSchemaRegistry
	mu      sync.Mutex
	op      string
	key     jobdb.JobKey
	failure error
}

func (g *compatGate) set(op string, k jobdb.JobKey, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.op = op
	g.key = k
	g.failure = err
}
func (g *compatGate) deny(op string, k jobdb.JobKey) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if op == g.op && k == g.key {
		return g.failure
	}
	return nil
}
func (g *compatGate) GetJob(c context.Context, k jobdb.JobKey) (jobdb.JobInfo, error) {
	if err := g.deny("job", k); err != nil {
		return jobdb.JobInfo{}, err
	}
	return g.ConsumerRuntime.GetJob(c, k)
}
func (g *compatGate) ListChapters(c context.Context, q jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	if err := g.deny("list", q.JobKey); err != nil {
		return nil, err
	}
	return g.ConsumerRuntime.ListChapters(c, q)
}
func (g *compatGate) GetChapter(c context.Context, q jobdb.ChapterRef) (jobdb.Chapter, error) {
	if err := g.deny("chapter", q.JobKey); err != nil {
		return jobdb.Chapter{}, err
	}
	return g.ConsumerRuntime.GetChapter(c, q)
}
func (g *compatGate) OpenArtifact(c context.Context, q jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	if err := g.deny("artifact", q.JobKey); err != nil {
		return nil, err
	}
	r, err := g.ConsumerRuntime.OpenArtifact(c, q)
	if err != nil {
		return nil, err
	}
	return compatReader{r, g, q.JobKey}, nil
}

type compatReader struct {
	jobdb.ArtifactReader
	gate *compatGate
	key  jobdb.JobKey
}

func (r compatReader) Open() (io.ReadCloser, error) {
	if err := r.gate.deny("open", r.key); err != nil {
		return nil, err
	}
	return r.ArtifactReader.Open()
}
func storageInventory(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	out := map[string][32]byte{}
	compatCheck(t, filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			out[rel] = sha256.Sum256(b)
		}
		return nil
	}))
	return out
}
func TestRetainedReadBaselineCompatibility(t *testing.T) {
	binary := baselineBinary(t)
	ctx := celltest.Context(context.Background())
	payload := bytes.Repeat([]byte{0, 255, 10, 128}, 4096)
	t.Run("client", func(t *testing.T) {
		cfg := sqlite.Config{Projects: celltest.UnrestrictedProjects{}, DBPath: filepath.Join(t.TempDir(), "current.db"), MaxInlineArtifactBytes: 1}
		r, err := sqlite.NewFromConfig(ctx, cfg)
		compatCheck(t, err)
		_, err = r.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: "tenant", Name: "compat", Repo: "example.test/compat"})
		compatCheck(t, err)
		keys := []jobdb.JobKey{}
		for i := 0; i < 2; i++ {
			content := payload
			if i == 0 {
				content = []byte("selected-decoy")
			}
			h, err := r.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", Cell: "compat", JobType: "read", Data: jobdb.NewTaskDataOrPanic("input", jobdb.NewArtifactFromBytes("same.bin", content))}})
			compatCheck(t, err)
			lease, err := r.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: h.JobKey, WorkerID: "compat", Routes: []jobdb.Route{{JobType: "read"}}, LeaseDuration: time.Minute})
			compatCheck(t, err)
			if lease == nil {
				t.Fatal("missing lease")
			}
			chapter := jobdb.Chapter{Ordinal: 1, TaskType: "read", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`"retained-result"`)}}}}
			compatCheck(t, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &chapter}))
			keys = append(keys, h.JobKey)
		}
		compatCheck(t, r.Close(ctx))
		r, err = sqlite.NewFromConfig(ctx, cfg)
		compatCheck(t, err)
		defer r.Close(ctx)
		gate := &compatGate{ConsumerRuntime: r, ConfigurationAdmin: r, JobSchemaRegistry: r}
		server := httptest.NewServer(celltest.Handler(remote.NewConsumerServer(gate)))
		defer server.Close()
		for _, wrapped := range []bool{false, true} {
			for _, op := range []string{"job", "list", "chapter", "artifact", "open"} {
				t.Run(fmt.Sprintf("%s/wrapped=%t", op, wrapped), func(t *testing.T) {
					key := keys[0]
					if op == "artifact" || op == "open" {
						key = keys[1]
					}
					q := baselineRequest{URL: server.URL, Operation: op, Key: key, Expected: payload}
					runBaseline(t, binary, q)
					// Selected metadata remains accessible when only the producer is revoked.
					failure := error(jobdb.ErrLeasePermissionDenied)
					if wrapped {
						failure = fmt.Errorf("private-baseline-wrapper: %w", failure)
					}
					gate.set(op, key, failure)
					if key == keys[1] {
						runBaseline(t, binary, baselineRequest{URL: server.URL, Operation: "job", Key: keys[0]})
					}
					q.Denied = true
					runBaseline(t, binary, q)
					gate.set("", jobdb.JobKey{}, nil)
					q.Denied = false
					runBaseline(t, binary, q)
				})
			}
		}
	})
	t.Run("baseline_storage_assessment", func(t *testing.T) {
		root := t.TempDir()
		q := baselineRequest{Mode: "create", Path: filepath.Join(root, "baseline.db"), Expected: payload}
		compatCheck(t, json.Unmarshal(runBaseline(t, binary, q), &q.Key))
		before := storageInventory(t, root)
		db, err := sql.Open("sqlite", q.Path)
		compatCheck(t, err)
		var format int
		compatCheck(t, db.QueryRow("PRAGMA user_version").Scan(&format))
		compatCheck(t, db.Close())
		if format != 11 {
			t.Fatalf("unexpected actual baseline format: %d", format)
		}
		opened, err := sqlite.NewFromConfig(ctx, sqlite.Config{Projects: celltest.UnrestrictedProjects{}, DBPath: q.Path})
		if opened != nil {
			opened.Close(ctx)
		}
		if !errors.Is(err, sqlite.ErrMigrationRequired) {
			t.Fatalf("unexpected old-storage assessment: %v", err)
		}
		var required *sqlite.MigrationRequiredError
		if !errors.As(err, &required) || required.SourceFormat != format {
			t.Fatalf("lost format diagnostic: %v", err)
		}
		if !reflect.DeepEqual(before, storageInventory(t, root)) {
			t.Fatal("baseline storage or associated artifact files changed")
		}
		q.Mode = "reopen"
		runBaseline(t, binary, q)
		t.Logf("OLD_STORAGE_COMPATIBILITY_FAILED: baseline format %d refused without conversion: %v; bytes unchanged and baseline reader reopened records/artifacts", format, err)
	})
}
