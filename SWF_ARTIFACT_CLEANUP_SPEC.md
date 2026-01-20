# SWF Artifact Cleanup/Dispose Enhancement

## Overview

Enhance the `swf.Artifact` interface to support lifecycle management via cleanup callbacks and provide high-level utility constructors that abstract away strata implementation details. This enables artifacts backed by temporary resources (files, directories) to properly clean up when SWF is done consuming them, while keeping users in the SWF API surface.

## Problem Statement

Currently, when creating artifacts from temporary files or directories, we face a cleanup dilemma:

**Eager cleanup (current approach):**
```go
func CreateArtifact() (swf.Artifact, error) {
    tempDir, _ := os.MkdirTemp("", "artifact-*")
    defer os.RemoveAll(tempDir) // Cleanup immediately

    // Write data to temp file
    filePath := filepath.Join(tempDir, "data.bin")
    os.WriteFile(filePath, data, 0644)

    // Must read into memory because temp dir is cleaned up
    data, _ := os.ReadFile(filePath)
    return swf.NewArtifactFromBytes("artifact", data)
    // Temp dir cleaned up here
}
```

**Problems:**
- Forces eager loading into memory (defeats lazy artifact design)
- Duplicates data (file → memory → upload)
- Wastes memory for large artifacts
- Can't use streaming/lazy patterns
- Users must interact with strata APIs directly

**Desired pattern (with cleanup support):**
```go
func CreateArtifact() (swf.Artifact, error) {
    tempDir, _ := os.MkdirTemp("", "artifact-*")

    // Write data to temp file
    filePath := filepath.Join(tempDir, "data.bin")
    os.WriteFile(filePath, data, 0644)

    // Create lazy artifact with automatic cleanup
    // No strata APIs needed!
    return swf.NewArtifactFromFile("artifact", filePath)
}
```

**Benefits:**
- Lazy file reading (stream directly from disk)
- No memory duplication
- Automatic resource cleanup
- No direct strata API usage required
- Clean, simple API

## Proposed Design

### Updated Artifact Interface

Update the core Artifact interface to include cleanup:

```go
// Artifact represents a file-like resource that can be consumed by tasks
// and persisted in workflow storage. Artifacts support lazy loading and
// automatic cleanup of temporary resources.
type Artifact interface {
    // Name returns the artifact name (e.g., "output.tar.gz")
    Name() string

    // Open returns a ReadCloser to stream the artifact contents.
    // Multiple calls to Open() may return independent readers.
    Open() (io.ReadCloser, error)

    // Size returns the artifact size in bytes, or -1 if unknown.
    Size() int64

    // Cleanup is called by SWF after the artifact has been fully consumed
    // and is no longer needed. Implementations should clean up any temporary
    // resources (files, directories, connections, etc.).
    //
    // Cleanup may be called multiple times and must be idempotent.
    // Cleanup must not return an error that would halt workflow execution.
    // Errors should be logged but not propagated.
    //
    // For artifacts without cleanup needs, return nil.
    Cleanup() error
}
```

### Utility Constructors

Provide high-level constructors for common artifact patterns, abstracting away strata implementation details:

```go
// NewArtifactFromBytes creates an in-memory artifact from bytes.
// No cleanup needed (no temporary resources).
//
// Example:
//   art := swf.NewArtifactFromBytes("config.json", jsonBytes)
func NewArtifactFromBytes(name string, data []byte) Artifact

// NewArtifactFromReader creates an artifact from an io.Reader.
// The reader will be consumed on first Open() call.
// If size is unknown, pass -1.
//
// Example:
//   art := swf.NewArtifactFromReader("output.txt", reader, 1024)
func NewArtifactFromReader(name string, r io.Reader, size int64) Artifact

// NewArtifactFromFile creates a lazy file-based artifact.
// The file is streamed on Open() without loading into memory.
// The file will be automatically removed when SWF is done (cleanup).
//
// Example:
//   art, _ := swf.NewArtifactFromFile("build.tar.gz", "/tmp/build.tar.gz")
func NewArtifactFromFile(name string, filePath string) (Artifact, error)

// NewArtifactFromFileNoCleanup creates a lazy file-based artifact
// without automatic cleanup. Use this for non-temporary files.
//
// Example:
//   art, _ := swf.NewArtifactFromFileNoCleanup("input.txt", "/data/input.txt")
func NewArtifactFromFileNoCleanup(name string, filePath string) (Artifact, error)

// NewArtifactFromDir creates an artifact from a directory by creating
// a tar.gz archive. The directory and archive will be cleaned up when
// SWF is done.
//
// Example:
//   art, _ := swf.NewArtifactFromDir("project-src", "/tmp/build-output")
func NewArtifactFromDir(name string, dirPath string) (Artifact, error)

// NewArtifact creates a custom artifact with full control.
// Provide opener function and optional cleanup function.
// This is the low-level API for advanced use cases.
//
// Example:
//   art := swf.NewArtifact("custom", func() (io.ReadCloser, int64, error) {
//       f, _ := os.Open(path)
//       info, _ := f.Stat()
//       return f, info.Size(), nil
//   }, func() error {
//       return os.Remove(path)
//   })
func NewArtifact(
    name string,
    opener func() (io.ReadCloser, int64, error),
    cleanup func() error,
) Artifact
```

### Implementation Types

Internal implementation types for the constructors above:

```go
// bytesArtifact - in-memory artifact
type bytesArtifact struct {
    name string
    data []byte
}

func (a *bytesArtifact) Name() string { return a.name }
func (a *bytesArtifact) Size() int64  { return int64(len(a.data)) }
func (a *bytesArtifact) Open() (io.ReadCloser, error) {
    return io.NopCloser(bytes.NewReader(a.data)), nil
}
func (a *bytesArtifact) Cleanup() error { return nil }

// readerArtifact - one-time reader artifact
type readerArtifact struct {
    name   string
    reader io.Reader
    size   int64
    once   sync.Once
}

func (a *readerArtifact) Name() string { return a.name }
func (a *readerArtifact) Size() int64  { return a.size }
func (a *readerArtifact) Open() (io.ReadCloser, error) {
    var r io.Reader
    a.once.Do(func() { r = a.reader })
    if r == nil {
        return nil, fmt.Errorf("reader already consumed")
    }
    if rc, ok := r.(io.ReadCloser); ok {
        return rc, nil
    }
    return io.NopCloser(r), nil
}
func (a *readerArtifact) Cleanup() error { return nil }

// fileArtifact - file-based artifact with optional cleanup
type fileArtifact struct {
    name      string
    path      string
    size      int64
    autoClean bool
    cleaned   atomic.Bool
}

func (a *fileArtifact) Name() string { return a.name }
func (a *fileArtifact) Size() int64  { return a.size }
func (a *fileArtifact) Open() (io.ReadCloser, error) {
    return os.Open(a.path)
}
func (a *fileArtifact) Cleanup() error {
    if !a.autoClean {
        return nil
    }
    if !a.cleaned.CompareAndSwap(false, true) {
        return nil // Already cleaned (idempotent)
    }
    return os.Remove(a.path)
}

// dirArtifact - directory artifact with tar.gz archive
type dirArtifact struct {
    name    string
    tarPath string
    dirPath string
    size    int64
    cleaned atomic.Bool
}

func (a *dirArtifact) Name() string { return a.name }
func (a *dirArtifact) Size() int64  { return a.size }
func (a *dirArtifact) Open() (io.ReadCloser, error) {
    return os.Open(a.tarPath)
}
func (a *dirArtifact) Cleanup() error {
    if !a.cleaned.CompareAndSwap(false, true) {
        return nil // Already cleaned (idempotent)
    }
    // Clean up both tar and original directory
    var errs []error
    if err := os.Remove(a.tarPath); err != nil {
        errs = append(errs, err)
    }
    if err := os.RemoveAll(a.dirPath); err != nil {
        errs = append(errs, err)
    }
    return errors.Join(errs...)
}

// customArtifact - custom artifact with user-provided functions
type customArtifact struct {
    name    string
    opener  func() (io.ReadCloser, int64, error)
    cleanup func() error
    size    int64
    cleaned atomic.Bool
}

func (a *customArtifact) Name() string { return a.name }
func (a *customArtifact) Size() int64  { return a.size }
func (a *customArtifact) Open() (io.ReadCloser, error) {
    rc, size, err := a.opener()
    if err != nil {
        return nil, err
    }
    a.size = size
    return rc, nil
}
func (a *customArtifact) Cleanup() error {
    if a.cleanup == nil {
        return nil
    }
    if !a.cleaned.CompareAndSwap(false, true) {
        return nil // Already cleaned (idempotent)
    }
    return a.cleanup()
}
```

### SWF Engine Integration

SWF engine must call cleanup after consuming artifacts:

```go
// In task execution logic
func (e *Engine) executeTask(ctx context.Context, task Task, input TaskData) (TaskData, error) {
    // Get input artifacts
    artifacts, _ := input.GetArtifacts()

    // Defer cleanup for all artifacts
    defer func() {
        for _, art := range artifacts {
            if err := art.Cleanup(); err != nil {
                // Log error but don't fail task
                log.Warn("artifact cleanup failed", "name", art.Name(), "error", err)
            }
        }
    }()

    // Execute task...
    output, err := task.Run(ctx, input)

    // ... handle output artifacts cleanup similarly ...

    return output, err
}
```

**Cleanup lifecycle:**
1. SWF receives artifacts as task input/output
2. SWF uploads artifacts to storage backend
3. After upload completes successfully, SWF calls `Cleanup()` on each artifact
4. Errors logged but not propagated (cleanup must not fail workflows)

### Strata Artifact Adapter

Provide adapter to wrap strata artifacts as swf artifacts:

```go
// FromStrataArtifact wraps a strata.Artifact as a swf.Artifact.
// This is used internally by SWF to bridge strata artifacts.
// Users should not need to call this directly.
func FromStrataArtifact(strataArt strata.Artifact) Artifact {
    return &strataArtifactAdapter{art: strataArt}
}

type strataArtifactAdapter struct {
    art strata.Artifact
}

func (a *strataArtifactAdapter) Name() string {
    return a.art.Name()
}

func (a *strataArtifactAdapter) Open() (io.ReadCloser, error) {
    return a.art.Open()
}

func (a *strataArtifactAdapter) Size() int64 {
    return a.art.Size()
}

func (a *strataArtifactAdapter) Cleanup() error {
    // Check if strata artifact has cleanup interface
    if cleanup, ok := a.art.(interface{ Cleanup() error }); ok {
        return cleanup.Cleanup()
    }
    return nil
}
```

### Idempotency Guarantee

Cleanup implementations must be idempotent because:
- SWF may retry cleanup on transient failures
- Multiple code paths might trigger cleanup
- Defensive programming principle

All built-in implementations use `atomic.Bool` to ensure cleanup runs only once:

```go
func (a *fileArtifact) Cleanup() error {
    if !a.autoClean {
        return nil
    }
    if !a.cleaned.CompareAndSwap(false, true) {
        return nil // Already cleaned (idempotent)
    }
    return os.Remove(a.path)
}
```

## Use Cases

### 1. Temporary File Artifacts

**Current (eager loading with strata):**
```go
func CreateTempArtifact() (swf.Artifact, error) {
    tempDir, _ := os.MkdirTemp("", "artifact-*")
    defer os.RemoveAll(tempDir) // Immediate cleanup

    // Generate data and write to temp file
    filePath := filepath.Join(tempDir, "data.bin")
    generateData(filePath)

    // Must read into memory
    data, _ := os.ReadFile(filePath)

    // Direct strata API usage
    return strata.NewArtifactFromBytes("output", data), nil
}
```

**Future (lazy loading with swf utilities):**
```go
func CreateTempArtifact() (swf.Artifact, error) {
    tempDir, _ := os.MkdirTemp("", "artifact-*")

    // Generate data and write to temp file
    filePath := filepath.Join(tempDir, "data.bin")
    generateData(filePath)

    // Lazy file-based artifact with automatic cleanup
    // No strata APIs needed!
    return swf.NewArtifactFromFile("output", filePath)
}
```

### 2. Large Build Artifacts

**Scenario:** Operation produces large build output (Docker images, compiled binaries, etc.)

```go
func createBuildArtifact(buildDir string) (swf.Artifact, error) {
    // Tar up build directory
    tarPath := "/tmp/build-output.tar.gz"
    createTarball(buildDir, tarPath)

    // Simple, clean API with automatic cleanup
    return swf.NewArtifactFromFile("build-output.tar.gz", tarPath)
}
```

### 3. Directory Artifacts

**Scenario:** Package entire directory as artifact

```go
func createProjectSnapshot(projectDir string) (swf.Artifact, error) {
    // Automatically creates tar.gz and cleans up
    return swf.NewArtifactFromDir("project-snapshot", projectDir)
}
```

### 4. Streaming Database Exports

**Scenario:** Export database to artifact without loading into memory

```go
func createDatabaseExport(db *sql.DB, query string) (swf.Artifact, error) {
    tempFile, _ := os.CreateTemp("", "db-export-*.csv")

    // Write query results to temp file
    rows, _ := db.Query(query)
    writer := csv.NewWriter(tempFile)
    // ... write rows ...
    tempFile.Close()

    // Clean, simple API
    return swf.NewArtifactFromFile("export.csv", tempFile.Name())
}
```

### 5. Custom Artifact with Advanced Control

**Scenario:** Need full control over artifact behavior

```go
func createCustomArtifact() (swf.Artifact, error) {
    resource := acquireResource()

    return swf.NewArtifact(
        "custom-data",
        func() (io.ReadCloser, int64, error) {
            data, err := resource.Read()
            return io.NopCloser(bytes.NewReader(data)), int64(len(data)), err
        },
        func() error {
            return resource.Release()
        },
    ), nil
}
```

## Implementation Plan

### Phase 1: Update Interface and Add Constructors

1. Update `Artifact` interface to include `Cleanup() error`
2. Add utility constructors: `NewArtifactFromBytes`, `NewArtifactFromFile`, etc.
3. Implement internal artifact types with cleanup support
4. Add `FromStrataArtifact` adapter
5. Add unit tests for all constructors and cleanup behavior

**Files:**
- `swf-go/pkg/swf/artifact.go` - interface and implementations
- `swf-go/pkg/swf/artifact_test.go` - tests

### Phase 2: Integrate into SWF Engine

1. Update task execution to call cleanup after artifact consumption
2. Add cleanup to artifact upload pipeline
3. Ensure cleanup called on error paths (via defer)
4. Add observability (log cleanup calls and errors)

**Files:**
- `swf-go/pkg/engine/task_executor.go` - cleanup integration
- `swf-go/pkg/storage/artifact_uploader.go` - cleanup after upload

### Phase 3: Update Documentation

1. Document utility constructors with examples
2. Add migration guide for existing artifact usage
3. Clearly state: users should never need to interact with strata APIs directly
4. Add cookbook examples for common patterns

## Testing Strategy

### Unit Tests

```go
func TestNewArtifactFromFile_Cleanup(t *testing.T) {
    // Create temp file
    f, _ := os.CreateTemp("", "test-*.txt")
    path := f.Name()
    f.WriteString("test data")
    f.Close()

    // Create artifact
    art, err := swf.NewArtifactFromFile("test.txt", path)
    require.NoError(t, err)

    // Verify file exists
    assert.FileExists(t, path)

    // Open and read
    rc, _ := art.Open()
    data, _ := io.ReadAll(rc)
    rc.Close()
    assert.Equal(t, "test data", string(data))

    // Cleanup
    err = art.Cleanup()
    assert.NoError(t, err)

    // Verify file removed
    assert.NoFileExists(t, path)
}

func TestCleanup_Idempotent(t *testing.T) {
    f, _ := os.CreateTemp("", "test-*.txt")
    path := f.Name()
    f.Close()

    art, _ := swf.NewArtifactFromFile("test.txt", path)

    // Call cleanup multiple times
    err1 := art.Cleanup()
    err2 := art.Cleanup()
    err3 := art.Cleanup()

    assert.NoError(t, err1)
    assert.NoError(t, err2) // Should be idempotent
    assert.NoError(t, err3) // Should be idempotent
}

func TestNewArtifactFromDir(t *testing.T) {
    // Create temp directory with files
    dir, _ := os.MkdirTemp("", "test-dir-*")
    os.WriteFile(filepath.Join(dir, "file1.txt"), []byte("data1"), 0644)
    os.WriteFile(filepath.Join(dir, "file2.txt"), []byte("data2"), 0644)

    // Create artifact
    art, err := swf.NewArtifactFromDir("mydir", dir)
    require.NoError(t, err)

    // Verify it's a tar.gz
    assert.Contains(t, art.Name(), ".tar.gz")

    // Cleanup
    err = art.Cleanup()
    assert.NoError(t, err)

    // Verify both dir and tar removed
    assert.NoDirExists(t, dir)
}
```

### Integration Tests

```go
func TestEngine_CleansUpArtifactsAfterTaskExecution(t *testing.T) {
    tempFile, _ := os.CreateTemp("", "test-*.bin")
    tempPath := tempFile.Name()
    tempFile.WriteString("artifact data")
    tempFile.Close()

    art, _ := swf.NewArtifactFromFile("test.bin", tempPath)
    input := swf.NewTaskData(map[string]interface{}{}, art)

    // Verify file exists before execution
    assert.FileExists(t, tempPath)

    // Execute task
    _, err := engine.ExecuteTask(ctx, task, input)
    require.NoError(t, err)

    // Verify cleanup called - file should be removed
    assert.NoFileExists(t, tempPath)
}
```

## Backwards Compatibility

**Breaking change for Artifact interface:**
- All existing Artifact implementations must add `Cleanup() error` method
- For implementations without cleanup needs, simply return `nil`

**Migration path:**
1. Update `swf.Artifact` interface to include `Cleanup() error`
2. Update all internal implementations to implement cleanup
3. For strata artifacts, use adapter that checks for optional cleanup interface
4. External implementations must be updated (breaking change)

**For consumers:**
- Replace strata artifact constructors with swf constructors
- Example: `strata.NewArtifactFromBytes(...)` → `swf.NewArtifactFromBytes(...)`
- No other code changes required

## Performance Considerations

**Benefits:**
- Reduced memory usage (no eager loading)
- Reduced disk I/O (stream directly vs read→memory→upload)
- Better scalability for large artifacts

**Costs:**
- Minimal overhead (one atomic operation per cleanup call)
- Temp resources held slightly longer (until after upload)

**Typical lifecycle:**
```
[Create artifact] → [Upload to storage] → [Cleanup] = ~seconds to minutes
```

For git thin packs (typically < 10MB), holding temp file for upload duration is negligible.

## Error Handling

**Cleanup errors must not fail workflows:**

```go
if err := art.Cleanup(); err != nil {
    // Log but don't propagate
    log.Warn("artifact cleanup failed",
        "artifact", art.Name(),
        "error", err)
}
```

**Rationale:**
- Cleanup is a "best effort" operation
- Main workflow succeeded (artifact uploaded)
- Temp file leaks are undesirable but not critical
- OS will eventually clean up /tmp

**Observability:**
- Log all cleanup calls (debug level)
- Log cleanup errors (warn level)
- Track cleanup failure metrics

## Key Design Decisions

### 1. Cleanup on Main Interface vs Optional Interface

**Decision:** Add cleanup to main `Artifact` interface

**Rationale:**
- Simpler API - no type assertions needed
- Forces all implementations to think about cleanup
- Implementations without cleanup just return `nil`
- More discoverable and explicit

**Trade-off:**
- Breaking change (must update all implementations)
- Worth it for cleaner, more maintainable API

### 2. High-Level Utility Constructors

**Decision:** Provide `NewArtifactFrom*` constructors

**Rationale:**
- Users never need to interact with strata APIs directly
- Matches upstream strata.artifact design patterns
- Makes common cases simple: `NewArtifactFromFile(...)`
- Still allows advanced control via `NewArtifact(...)`
- Better discoverability and usability

### 3. Automatic Cleanup for Temp Files

**Decision:** `NewArtifactFromFile` automatically cleans up by default

**Rationale:**
- Common case: temp files should be cleaned up
- Explicit opt-out available: `NewArtifactFromFileNoCleanup`
- Prevents accidental resource leaks
- Matches user expectations ("temp file artifact")

## Success Criteria

- [ ] `Artifact` interface includes `Cleanup() error` method
- [ ] All utility constructors implemented and documented
- [ ] `NewArtifactFromFile`, `NewArtifactFromDir`, `NewArtifactFromBytes`, etc.
- [ ] Strata adapter implemented for bridging
- [ ] SWF engine calls cleanup after artifact consumption
- [ ] Cleanup errors logged but don't fail workflows
- [ ] Unit tests verify cleanup behavior and idempotency
- [ ] Integration tests verify engine integration
- [ ] No memory regression in artifact handling
- [ ] Documentation clearly states: no direct strata API usage needed
- [ ] Users can create all artifact types via swf constructors

## Future Enhancements

### Context-Aware Cleanup

```go
// Add context to cleanup for cancellation support
type Artifact interface {
    // ...
    CleanupWithContext(ctx context.Context) error
}
```

### Cleanup Observability Dashboard

```go
// Cleanup metrics exposed for monitoring
type CleanupMetrics struct {
    TotalCalls      int
    SuccessCalls    int
    FailedCalls     int
    AvgDuration     time.Duration
    TotalBytesFreed int64
}
```

### Async Cleanup Pool

```go
// Cleanup in background worker pool to avoid blocking
type CleanupPool struct {
    workers int
    queue   chan Artifact
}

func (p *CleanupPool) ScheduleCleanup(art Artifact) {
    p.queue <- art
}
```

**Caveat:** Must ensure cleanup completes before process exit.

### Smart Temp File Management

```go
// Automatic temp directory with scoped cleanup
func NewTempArtifactScope() *ArtifactScope {
    return &ArtifactScope{
        tempDir: os.MkdirTemp("", "swf-artifacts-*"),
    }
}

func (s *ArtifactScope) NewArtifactFromFile(name string) (Artifact, error) {
    // Creates file in scoped temp dir
    // Auto-cleanup when scope closes
}
```
