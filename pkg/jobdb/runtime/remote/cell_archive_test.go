package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/celltest"
	"github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/stretchr/testify/require"
)

type archiveProjectPolicy struct {
	celltest.UnrestrictedProjects
	blocked bool
}

func (policy *archiveProjectPolicy) AdmitWrite(context.Context, string) error {
	if policy.blocked {
		return jobdb.ErrProjectAccessDenied
	}
	return nil
}

func TestCellArchiveHTTP(t *testing.T) {
	for _, family := range []string{"tenants", "projects"} {
		t.Run(family, func(t *testing.T) {
			const scope = "archive-http"
			policy := &archiveProjectPolicy{}
			backend := toy.New(toy.WithProjects(policy))
			ctx := celltest.Context(context.Background())
			create := func(name string) jobdb.Cell {
				cell, err := backend.CreateCell(ctx, jobdb.CreateCellRequest{TenantID: scope, Name: name, Repo: "example.test/archive/" + name})
				require.NoError(t, err)
				return cell
			}
			cell := create("example")
			key := jobdb.CellKey{TenantID: scope, CellID: cell.ID}
			base := "/v1/" + family + "/" + scope + "/cells"
			identity := "tenantId"
			if family == "projects" {
				identity = "projectId"
			}
			etag := fmt.Sprintf("%q", cell.Revision)
			handler := NewServer(backend)
			for _, action := range []string{"archive", "unarchive"} {
				for _, test := range []struct {
					name       string
					headers    []string
					authority  string
					wantStatus int
				}{
					{"missing", nil, "ok", 428},
					{"empty", []string{""}, "ok", 400},
					{"whitespace", []string{" \t "}, "ok", 400},
					{"weak", []string{"W/" + etag}, "ok", 400},
					{"wildcard", []string{"*"}, "ok", 400},
					{"unquoted", []string{cell.Revision}, "ok", 400},
					{"quoted space", []string{`"bad tag"`}, "ok", 400},
					{"list", []string{etag + ", " + etag}, "ok", 400},
					{"duplicate", []string{etag, etag}, "ok", 400},
					{"stale", []string{`"old"`}, "ok", 412},
					{"no scope", []string{etag}, "none", 403},
					{"wrong tenant", []string{etag}, "other", 403},
					{"hidden cell", []string{etag}, "hidden", 404},
					{"no get", []string{etag}, "get", 403},
					{"no update", []string{etag}, "update", 403},
					{"project denied", []string{etag}, "project", 403},
				} {
					t.Run(action+"/"+test.name, func(t *testing.T) {
						authority := jobdb.CellAuthority{Principal: "tester", TenantID: scope, Permissions: map[string]bool{"cells.get": test.authority != "get", "cells.update": test.authority != "update"}}
						if test.authority == "other" {
							authority.TenantID = "other"
						}
						if test.authority == "hidden" {
							authority.Cells = map[string]bool{}
						}
						policy.blocked = test.authority == "project"
						defer func() { policy.blocked = false }()
						req := httptest.NewRequest(http.MethodPost, base+"/"+cell.ID+"/"+action, nil)
						if test.authority != "none" {
							req = req.WithContext(jobdb.WithCellAuthority(req.Context(), authority))
						}
						for _, header := range test.headers {
							req.Header.Add("If-Match", header)
						}
						out := httptest.NewRecorder()
						handler.ServeHTTP(out, req)
						require.Equal(t, test.wantStatus, out.Code, out.Body.String())
						stored, err := backend.GetCell(ctx, key)
						require.NoError(t, err)
						require.Equal(t, cell, stored)
					})
				}
			}
			authorized := celltest.Handler(handler)
			apply := func(action, observed string, marked bool) string {
				req := httptest.NewRequest(http.MethodPost, base+"/"+cell.ID+"/"+action, nil)
				req.Header.Set("If-Match", observed)
				out := httptest.NewRecorder()
				authorized.ServeHTTP(out, req)
				require.Equal(t, 200, out.Code, out.Body.String())
				var fields map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(out.Body.Bytes(), &fields))
				require.JSONEq(t, fmt.Sprintf("%q", scope), string(fields[identity]))
				require.Equal(t, marked, fields["archivedAt"] != nil)
				require.Equal(t, string(fields["revision"]), out.Header().Get("ETag"))
				return out.Header().Get("ETag")
			}
			markedTag := apply("archive", etag, true)
			require.NotEqual(t, etag, markedTag)
			require.Equal(t, markedTag, apply("archive", markedTag, true))
			unmarkedTag := apply("unarchive", markedTag, false)
			require.NotEqual(t, markedTag, unmarkedTag)
			require.Equal(t, unmarkedTag, apply("unarchive", unmarkedTag, false))

			mark := func(cell jobdb.Cell) jobdb.Cell {
				marked, err := backend.ArchiveCell(ctx, jobdb.ArchiveCellRequest{CellKey: jobdb.CellKey{TenantID: scope, CellID: cell.ID}, IfMatch: fmt.Sprintf("%q", cell.Revision)})
				require.NoError(t, err)
				return marked
			}
			unmarked := create("unmarked")
			marked := []jobdb.Cell{mark(create("first-marked")), mark(create("second-marked"))}
			hidden := mark(create("hidden"))
			retired := mark(create("retired"))
			_, err := backend.RetireCell(ctx, jobdb.RetireCellRequest{CellKey: jobdb.CellKey{TenantID: scope, CellID: retired.ID}, IfMatch: fmt.Sprintf("%q", retired.Revision)})
			require.NoError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				caller := jobdb.CellAuthority{Principal: "list", TenantID: scope, Permissions: map[string]bool{"cells.list": true, "cells.get": true, "cells.update": true}, Cells: map[string]bool{cell.ID: true, unmarked.ID: true, marked[0].ID: true, marked[1].ID: true, retired.ID: true}}
				if r.Header.Get("X-Test-Caller") == "other" {
					caller.Principal = "other"
				}
				handler.ServeHTTP(w, r.WithContext(jobdb.WithCellAuthority(r.Context(), caller)))
			}))
			defer server.Close()
			newClient := New
			if family == "projects" {
				newClient = NewProjectRuntime
			}
			client, err := newClient(server.URL, server.Client())
			require.NoError(t, err)
			for _, action := range []struct {
				name  string
				apply func(context.Context, jobdb.ArchiveCellRequest) (jobdb.Cell, error)
			}{{"archive", client.ArchiveCell}, {"unarchive", client.UnarchiveCell}} {
				t.Run(action.name+"/client precondition", func(t *testing.T) {
					_, err := action.apply(ctx, jobdb.ArchiveCellRequest{CellKey: key})
					require.ErrorIs(t, err, jobdb.ErrCellPreconditionRequired)
					_, err = action.apply(ctx, jobdb.ArchiveCellRequest{CellKey: key, IfMatch: " \t "})
					require.ErrorIs(t, err, jobdb.ErrInvalidCell)
				})
			}
			active, err := client.GetCell(ctx, key)
			require.NoError(t, err)
			newlyMarked, err := client.ArchiveCell(ctx, jobdb.ArchiveCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", active.Revision)})
			require.NoError(t, err)
			require.NotNil(t, newlyMarked.ArchivedAt)
			_, err = client.UnarchiveCell(ctx, jobdb.ArchiveCellRequest{CellKey: key, IfMatch: fmt.Sprintf("%q", newlyMarked.Revision)})
			require.NoError(t, err)

			yes, no := true, false
			choices := []*bool{nil, &yes, &no}
			for _, filter := range choices {
				query := jobdb.ListCellsRequest{TenantID: scope, Archived: filter, PageSize: 1}
				first, err := client.ListCells(ctx, query)
				require.NoError(t, err)
				require.NotEmpty(t, first.NextPageToken)
				params := url.Values{"pageToken": {first.NextPageToken}}
				if filter != nil {
					params.Set("archived", fmt.Sprint(*filter))
				}
				request, err := http.NewRequest(http.MethodGet, server.URL+base+"?"+params.Encode(), nil)
				require.NoError(t, err)
				request.Header.Set("X-Test-Caller", "other")
				response, err := server.Client().Do(request)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				require.Equal(t, http.StatusBadRequest, response.StatusCode)
				for _, different := range choices {
					if filter != different {
						_, err := client.ListCells(ctx, jobdb.ListCellsRequest{TenantID: scope, Archived: different, PageToken: first.NextPageToken})
						require.ErrorIs(t, err, jobdb.ErrInvalidCellPageToken)
					}
				}
				ids := []string{}
				for page := first; ; {
					require.Len(t, page.Cells, 1)
					ids = append(ids, page.Cells[0].ID)
					if page.NextPageToken == "" {
						break
					}
					query.PageToken = page.NextPageToken
					page, err = client.ListCells(ctx, query)
					require.NoError(t, err)
					require.LessOrEqual(t, len(ids), 4)
				}
				expected := []string{marked[0].ID, marked[1].ID}
				if filter == nil {
					expected = append(expected, cell.ID, unmarked.ID)
				} else if !*filter {
					expected = []string{cell.ID, unmarked.ID}
				}
				require.ElementsMatch(t, expected, ids)
				require.NotContains(t, ids, hidden.ID)
			}
			for _, filter := range choices {
				page, err := client.ListCells(ctx, jobdb.ListCellsRequest{TenantID: scope, State: "RETIRED", Archived: filter})
				require.NoError(t, err)
				if filter == &yes {
					require.Empty(t, page.Cells)
				} else {
					require.Len(t, page.Cells, 1)
					require.Equal(t, retired.ID, page.Cells[0].ID)
				}
			}
		})
	}
}

func TestCellArchiveUnsupportedHTTP(t *testing.T) {
	backend := struct {
		legacyRuntime
		jobdb.LeaseEventReader
		jobdb.ConfigurationAdmin
	}{}
	handler := celltest.Handler(NewConsumerServer(backend))
	for _, action := range []string{"archive", "unarchive"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants/legacy/cells/id/"+action, nil)
		req.Header.Set("If-Match", `"observed"`)
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, req)
		require.Equal(t, http.StatusNotImplemented, out.Code, out.Body.String())
		require.Contains(t, out.Body.String(), jobdb.ErrCellArchiveUnsupported.Code)
	}
}
