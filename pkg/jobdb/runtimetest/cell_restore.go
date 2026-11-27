package runtimetest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

// CellRestoreFixture connects the restore conformance checks to a fresh runtime.
// WithAuthority must supply trusted authority, including for HTTP runtimes.
// Use a context or request credential that cannot be confused with production
// authority. SetProjectAdmission replaces the fixture's normally permissive
// write gate; nil resets it. Reopen, if set, reopens the same persistent stores
// after retirement using only the existing format.
type CellRestoreFixture struct {
	Runtime             jobdb.ConsumerRuntime
	WithAuthority       func(context.Context, jobdb.CellAuthority) context.Context
	SetProjectAdmission func(func(context.Context, string) error)
	Reopen              func() jobdb.ConsumerRuntime
}

// RunCellRestoreConformance checks the optional restore contract, including the
// persistence drill for fixtures that provide Reopen. RunCellConformance still
// covers the mandatory cell contract independently of restore support.
func RunCellRestoreConformance(test *testing.T, fixture CellRestoreFixture) {
	test.Helper()
	runtime := fixture.Runtime
	restore := func() jobdb.CellRestoreRuntime {
		capability, ok := runtime.(jobdb.CellRestoreRuntime)
		require.True(test, ok, "runtime must implement CellRestoreRuntime")
		return capability
	}
	restore()
	tenant := "restore-" + ksuid.New().String()
	base, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx := fixture.WithAuthority(base, jobdb.CellAuthority{
		Principal: "admin", TenantID: tenant,
		Permissions: map[string]bool{"cells.create": true, "cells.get": true, "cells.list": true, "cells.update": true, "cells.retire": true, "cells.restore": true,
			"jobs.submit": true, "jobs.acquire": true, "jobs.get": true},
	})
	cell, err := runtime.CreateCell(ctx, jobdb.CreateCellRequest{
		TenantID: tenant, Name: "original", Repo: "github.com/org/restore", Description: "preserve me",
		Metadata: map[string]json.RawMessage{"nested": json.RawMessage(`{"keep":[true,null,3]}`)},
	})
	require.NoError(test, err)
	key := jobdb.CellKey{TenantID: tenant, CellID: cell.ID}
	etag := func(cell jobdb.Cell) string { return fmt.Sprintf("%q", cell.Revision) }
	name := "renamed"
	cell, err = runtime.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: key, IfMatch: etag(cell), Name: &name})
	require.NoError(test, err)
	require.Equal(test, []string{"original"}, cell.Aliases)
	retireReq := jobdb.RetireCellRequest{CellKey: key, IfMatch: etag(cell)}
	submit := func(id string) (jobdb.JobHandle, error) {
		return runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: tenant, JobID: id, Cell: "original",
			JobType: "work", Data: jobdb.NewTaskDataOrPanic(1)}})
	}
	acquire := func(job jobdb.JobHandle) (jobdb.ExecutionLease, error) {
		return runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{JobKey: job.JobKey, WorkerID: "worker", Routes: []jobdb.Route{{JobType: "work"}}})
	}
	old, err := submit("history")
	require.NoError(test, err)
	_, err = runtime.RetireCell(ctx, retireReq)
	require.ErrorIs(test, err, jobdb.ErrCellBusy)
	lease, err := acquire(old)
	require.NoError(test, err)
	require.NotNil(test, lease)
	const contents = "retained artifact"
	ref := jobdb.ChapterRef{JobKey: old.JobKey, Ordinal: 1}
	require.NoError(test, runtime.PutChapter(ctx, jobdb.PutChapterRequest{
		LeaseID: lease.LeaseID(), LeaseToken: leaseTokenForTest(lease), Ref: ref,
		Chapter: appTaskAttemptChapterForTest(test, ref.Ordinal, "manual", "input-hash", []byte(`1`), nil),
		ArtifactUploads: []jobdb.ArtifactUpload{{Name: "history.txt", Size: int64(len(contents)), Open: func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(contents)), nil
		}}},
	}))
	require.NoError(test, lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "success", Chapter: &jobdb.Chapter{Ordinal: 2, TaskType: "work", CreatedAt: time.Now().UTC(), Body: jobdb.JobAttemptOutcomeChapter{
		Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`1`)}},
	}}}))
	history, err := runtime.GetJob(ctx, old.JobKey)
	require.NoError(test, err)
	retired, err := runtime.RetireCell(ctx, retireReq)
	require.NoError(test, err)
	require.Equal(test, jobdb.CellRetired, retired.State)
	require.NotNil(test, retired.RetiredAt)
	if fixture.Reopen != nil {
		runtime = fixture.Reopen()
	}
	assertStored := func(expected jobdb.Cell) {
		test.Helper()
		stored, err := runtime.GetCell(ctx, key)
		require.NoError(test, err)
		require.Equal(test, expected, stored)
	}
	assertHistory := func() {
		test.Helper()
		stored, err := runtime.GetJob(ctx, old.JobKey)
		require.NoError(test, err)
		require.Equal(test, history, stored)
		chapter, err := runtime.GetChapter(ctx, ref)
		require.NoError(test, err)
		require.Len(test, chapter.Artifacts, 1)
		artifact, err := runtime.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: old.JobKey, Ordinal: 1, Name: "history.txt", Digest: chapter.Artifacts[0].Digest})
		require.NoError(test, err)
		reader, err := artifact.Open()
		require.NoError(test, err)
		defer reader.Close()
		data, err := io.ReadAll(reader)
		require.NoError(test, err)
		require.Equal(test, contents, string(data))
	}
	assertMembership := func(state string, expected ...jobdb.Cell) {
		test.Helper()
		list, err := runtime.ListCells(ctx, jobdb.ListCellsRequest{TenantID: tenant, State: state})
		require.NoError(test, err)
		require.ElementsMatch(test, expected, list.Cells)
	}
	assertStored(retired)
	assertHistory()
	assertMembership("ACTIVE")
	assertMembership("RETIRED", retired)
	_, err = submit("rejected")
	require.ErrorIs(test, err, jobdb.ErrCellRetired)
	_, err = runtime.UpdateCell(ctx, jobdb.UpdateCellRequest{CellKey: key, IfMatch: etag(retired), Name: &name})
	require.ErrorIs(test, err, jobdb.ErrCellRetired)

	req := jobdb.RestoreCellRequest{CellKey: key, IfMatch: etag(retired)}
	for _, check := range []struct {
		name, validator string
		want            error
	}{
		{"missing", "", jobdb.ErrCellPreconditionRequired},
		{"weak", "W/" + req.IfMatch, jobdb.ErrInvalidCell},
		{"unquoted", retired.Revision, jobdb.ErrInvalidCell},
		{"wildcard", "*", jobdb.ErrInvalidCell},
		{"list", req.IfMatch + `, "other"`, jobdb.ErrInvalidCell},
		{"space in tag", `"bad tag"`, jobdb.ErrInvalidCell},
		{"stale", etag(cell), jobdb.ErrCellPrecondition},
	} {
		test.Run(check.name, func(test *testing.T) {
			_, err := restore().RestoreCell(ctx, jobdb.RestoreCellRequest{CellKey: key, IfMatch: check.validator})
			require.ErrorIs(test, err, check.want)
		})
	}
	grants := map[string]bool{"cells.get": true, "cells.restore": true}
	for _, check := range []struct {
		name      string
		authority jobdb.CellAuthority
		want      error
	}{
		{"untrusted", jobdb.CellAuthority{}, jobdb.ErrCellForbidden},
		{"retire only", jobdb.CellAuthority{Principal: "user", TenantID: tenant, Permissions: map[string]bool{"cells.get": true, "cells.retire": true}}, jobdb.ErrCellForbidden},
		{"no read grant", jobdb.CellAuthority{Principal: "user", TenantID: tenant, Permissions: map[string]bool{"cells.restore": true}}, jobdb.ErrCellForbidden},
		{"wrong tenant", jobdb.CellAuthority{Principal: "user", TenantID: "another", Permissions: grants}, jobdb.ErrCellForbidden},
		{"hidden cell", jobdb.CellAuthority{Principal: "user", TenantID: tenant, Permissions: grants, Cells: map[string]bool{"another-cell": true}}, jobdb.ErrCellNotFound},
	} {
		test.Run(check.name, func(test *testing.T) {
			_, err := restore().RestoreCell(fixture.WithAuthority(base, check.authority), req)
			require.ErrorIs(test, err, check.want)
		})
	}
	missing := req
	missing.CellID = "missing"
	_, err = restore().RestoreCell(ctx, missing)
	require.ErrorIs(test, err, jobdb.ErrCellNotFound)

	fixture.SetProjectAdmission(func(_ context.Context, _ string) error { return jobdb.ErrProjectAccessDenied })
	_, err = restore().RestoreCell(ctx, req)
	fixture.SetProjectAdmission(nil)
	if status, code, ok := jobdb.CellErrorStatus(err); ok {
		require.Equal(test, 403, status)
		require.Equal(test, "project_access_denied", code)
	} else {
		require.ErrorIs(test, err, jobdb.ErrProjectAccessDenied)
	}
	assertStored(retired)

	begun := time.Now()
	restored, err := restore().RestoreCell(fixture.WithAuthority(base, jobdb.CellAuthority{Principal: "restorer", TenantID: tenant,
		Permissions: grants, Cells: map[string]bool{key.CellID: true}}), req)
	require.NoError(test, err)
	require.NotEmpty(test, restored.Revision)
	require.NotEqual(test, retired.Revision, restored.Revision)
	require.True(test, restored.UpdatedAt.After(retired.UpdatedAt))
	require.False(test, restored.UpdatedAt.Before(begun))
	require.False(test, restored.UpdatedAt.After(time.Now()))
	expected := retired
	expected.State, expected.RetiredAt = jobdb.CellActive, nil
	expected.Revision, expected.UpdatedAt = restored.Revision, restored.UpdatedAt
	require.Equal(test, expected, restored)
	assertStored(restored)
	_, err = restore().RestoreCell(ctx, req)
	require.ErrorIs(test, err, jobdb.ErrCellPrecondition)
	req.IfMatch = etag(restored)
	retry, err := restore().RestoreCell(ctx, req)
	require.NoError(test, err)
	require.Equal(test, restored, retry)
	assertStored(restored)
	assertMembership("ACTIVE", restored)
	assertMembership("RETIRED")
	assertHistory()
	oldLease, err := acquire(old)
	require.NoError(test, err)
	require.Nil(test, oldLease, "restore must not restart old work")

	test.Run("retirement wins after preflight", func(test *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		var gated atomic.Bool
		fixture.SetProjectAdmission(func(ctx context.Context, _ string) error {
			if gated.CompareAndSwap(false, true) {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		defer fixture.SetProjectAdmission(nil)
		result := make(chan error, 1)
		go func() { _, err := restore().RestoreCell(ctx, req); result <- err }()
		select {
		case <-entered:
		case <-base.Done():
			test.Fatal("restore did not reach project admission")
		}
		winner, err := runtime.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: key, IfMatch: req.IfMatch})
		require.NoError(test, err)
		require.Equal(test, jobdb.CellRetired, winner.State)
		release <- struct{}{}
		require.ErrorIs(test, <-result, jobdb.ErrCellPrecondition)
		assertStored(winner)
		_, err = restore().RestoreCell(ctx, jobdb.RestoreCellRequest{CellKey: key, IfMatch: etag(winner)})
		require.NoError(test, err)
	})

	fresh, err := submit("fresh")
	require.NoError(test, err)
	require.Equal(test, key.CellID, fresh.CellID)
	activeLease, err := acquire(fresh)
	require.NoError(test, err)
	require.NotNil(test, activeLease)
}
