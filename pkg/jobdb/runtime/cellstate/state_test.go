package cellstate_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/cellstate"
	"github.com/stretchr/testify/require"
)

func TestAuthorityAndIdempotency(t *testing.T) {
	s := cellstate.New()
	ctx := celltest.Context(context.Background())
	req := jobdb.CreateCellRequest{TenantID: "t", Name: "one", Repo: "github.com/org/one", RequestID: "create"}
	_, err := s.Create(context.Background(), req)
	require.ErrorIs(t, err, jobdb.ErrCellForbidden)
	c, err := s.Create(ctx, req)
	require.NoError(t, err)
	again, err := s.Create(ctx, req)
	require.NoError(t, err)
	require.Equal(t, c, again)
	req.Repo = "github.com/org/two"
	_, err = s.Create(ctx, req)
	require.ErrorIs(t, err, jobdb.ErrCellRequestConflict)
	other, err := s.Create(ctx, jobdb.CreateCellRequest{TenantID: "t", Name: "two", Repo: req.Repo})
	require.NoError(t, err)
	a := jobdb.CellAuthority{Principal: "restricted", TenantID: "t", Permissions: map[string]bool{"cells.list": true, "cells.get": true, "jobs.list": true}, Cells: map[string]bool{c.ID: true}}
	restricted := jobdb.WithCellAuthority(context.Background(), a)
	a.Cells[other.ID] = true // caller mutation must not broaden authority
	page, err := s.List(restricted, jobdb.ListCellsRequest{TenantID: "t"})
	require.NoError(t, err)
	require.Len(t, page.Cells, 1)
	_, err = s.Get(restricted, jobdb.CellKey{TenantID: "t", CellID: other.ID})
	require.ErrorIs(t, err, jobdb.ErrCellNotFound)
	_, err = s.Select(restricted, "t", []string{"two"}, "jobs.list")
	require.ErrorIs(t, err, jobdb.ErrCellNotFound)
	_, err = s.Select(restricted, "t", []string{}, "jobs.list")
	require.ErrorIs(t, err, jobdb.ErrInvalidCell)
	page, err = s.List(ctx, jobdb.ListCellsRequest{TenantID: "t", PageSize: 1})
	require.NoError(t, err)
	_, err = s.List(restricted, jobdb.ListCellsRequest{TenantID: "t", PageToken: page.NextPageToken})
	require.ErrorIs(t, err, jobdb.ErrInvalidCellPageToken)
	metadata := map[string]json.RawMessage{"value": json.RawMessage(`{"a":1}`)}
	updated, err := s.Update(ctx, jobdb.UpdateCellRequest{CellKey: jobdb.CellKey{TenantID: "t", CellID: c.ID}, IfMatch: fmt.Sprintf("%q", c.Revision), Metadata: &metadata})
	require.NoError(t, err)
	metadata["value"][0] = '['
	got, err := s.Get(ctx, jobdb.CellKey{TenantID: "t", CellID: c.ID})
	require.NoError(t, err)
	require.Equal(t, updated, got)
}

func TestRepoNormalization(t *testing.T) {
	for _, input := range []string{"GitHub.com/Org/Repo", "https://GitHub.com/Org/Repo.git/", "ssh://git@GitHub.com:22/Org/Repo.git", "git@GitHub.com:Org/Repo.git"} {
		got, err := jobdb.NormalizeCellRepo(input)
		require.NoError(t, err, input)
		require.Equal(t, "github.com/Org/Repo", got)
	}
	for _, input := range []string{"", "file:///tmp/repo", "/tmp/repo", "https://user:password@github.com/org/repo", "https://github.com/org/../repo", "https://github.com/org/repo?token=secret", "ssh://git@github.com:2222/org/repo", "github.com/org//repo", "github.com/org/%2e%2e"} {
		_, err := jobdb.NormalizeCellRepo(input)
		require.Error(t, err, input)
	}
	for _, name := range []string{"", ".", "..", "has space", "has/slash"} {
		require.Error(t, jobdb.ValidateCellName(name))
	}
}

func TestLeaseSubmissionDefaultsToTenantAndHonorsExplicitScope(t *testing.T) {
	s := cellstate.New()
	ctx := celltest.Context(context.Background())
	creator := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{Principal: "creator", TenantID: "t", Permissions: map[string]bool{"cells.create": true}})
	root, err := s.Create(creator, jobdb.CreateCellRequest{TenantID: "t", Name: "root", Repo: "example.test/root"})
	require.NoError(t, err)
	other, err := s.Create(ctx, jobdb.CreateCellRequest{TenantID: "t", Name: "other", Repo: "example.test/other"})
	require.NoError(t, err)
	for _, caller := range []context.Context{context.Background(), ctx} {
		for _, parent := range []*jobdb.Cell{&root, &other} {
			for _, name := range []string{"root", "other"} {
				_, err := s.Resolve(caller, "t", name, "", parent, nil, false)
				require.NoError(t, err)
			}
		}
	}
	restricted := jobdb.WithCellAuthority(context.Background(), jobdb.CellAuthority{Principal: "worker", TenantID: "t", Permissions: map[string]bool{"jobs.submit": true}, Cells: map[string]bool{other.ID: true}})
	target, err := s.Resolve(restricted, "t", "other", "", &root, nil, false)
	require.NoError(t, err)
	require.Equal(t, other.Resolved(), target)
	_, err = s.Resolve(restricted, "t", "root", "", &root, nil, false)
	require.ErrorIs(t, err, jobdb.ErrCellNotFound, "owning cell has no implicit exemption from the list")
	_, err = s.Resolve(creator, "t", "root", "", &root, nil, false)
	require.ErrorIs(t, err, jobdb.ErrCellForbidden, "cell creation does not grant job submission")
	for _, parent := range []*jobdb.Cell{nil, &root, &other} {
		_, err = s.Resolve(ctx, "t", "missing", "example.test/missing", parent, nil, false)
		require.ErrorIs(t, err, jobdb.ErrCellNotFound)
	}
	require.Len(t, s.Cells, 2)
}
