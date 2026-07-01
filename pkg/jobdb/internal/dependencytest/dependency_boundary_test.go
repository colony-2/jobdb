package dependencytest

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestEmbeddedConsumerDoesNotImportAdvancedBlobProviders(t *testing.T) {
	repoRoot := findRepoRoot(t)
	dir := t.TempDir()

	goMod := `module jobdb-embedded-consumer-test

go 1.25.5

require github.com/colony-2/jobdb v0.0.0

replace github.com/colony-2/jobdb => ` + filepath.ToSlash(repoRoot) + `
`
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0o644); err != nil {
		t.Fatalf("write go.mod: %v", err)
	}

	mainGo := `package main

import (
	_ "github.com/colony-2/jobdb/pkg/jobdb"
	_ "github.com/colony-2/jobdb/pkg/workflow"
	_ "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	_ "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
)

func main() {}
`
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(mainGo), 0o644); err != nil {
		t.Fatalf("write main.go: %v", err)
	}

	cmd := exec.Command("go", "list", "-mod=mod", "-deps", ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps failed: %v\n%s", err, out)
	}

	deps := map[string]bool{}
	for _, dep := range strings.Fields(string(out)) {
		deps[dep] = true
	}

	forbidden := []string{
		"gocloud.dev/blob",
		"gocloud.dev/blob/s3blob",
		"gocloud.dev/blob/gcsblob",
		"gocloud.dev/blob/azureblob",
		"gocloud.dev/blob/fileblob",
		"gocloud.dev/blob/memblob",
		"github.com/aws/aws-sdk-go-v2/service/s3",
		"cloud.google.com/go/storage",
		"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob",
	}
	for _, pkg := range forbidden {
		if deps[pkg] {
			t.Fatalf("embedded consumer imports forbidden provider dependency %s", pkg)
		}
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("find repo root from %s: %v", file, err)
	}
	return root
}
