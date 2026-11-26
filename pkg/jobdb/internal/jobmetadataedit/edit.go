package jobmetadataedit

import (
	"bytes"
	"context"
	"encoding/json"
	"math"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type LeaseWriter interface {
	EditJobMetadataWithVerifiedLease(context.Context, jobdb.JobKey, jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, error)
}

func CheckScope(ctx context.Context, job jobdb.JobKey, cellID string) error {
	authority, scoped := jobdb.CellAuthorityFromContext(ctx)
	if !scoped {
		return nil
	}
	if authority.Principal == "" || (authority.TenantID != job.TenantId && authority.TenantID != "*") {
		return jobdb.ErrCellForbidden
	}
	if cellID != "" && authority.Cells != nil && !authority.Cells[cellID] {
		return jobdb.ErrJobNotFound
	}
	return nil
}

func Apply(current jobdb.JobMetadataEntry, edit jobdb.JobMetadataEdit) (jobdb.JobMetadataEntry, bool, error) {
	if current.Version != *edit.ExpectedVersion {
		return jobdb.JobMetadataEntry{}, false, jobdb.ErrConflict
	}
	var value json.RawMessage
	if edit.Set != nil {
		var compact bytes.Buffer
		if err := json.Compact(&compact, edit.Set); err != nil {
			return jobdb.JobMetadataEntry{}, false, err
		}
		value = compact.Bytes()
	}
	if current.Present == !edit.Remove && bytes.Equal(current.Value, value) {
		return Clone(current), false, nil
	}
	if current.Version == math.MaxInt64 {
		return jobdb.JobMetadataEntry{}, false, jobdb.ErrConflict
	}
	return jobdb.JobMetadataEntry{Version: current.Version + 1, Present: !edit.Remove, Value: value}, true, nil
}

func Clone(entry jobdb.JobMetadataEntry) jobdb.JobMetadataEntry {
	entry.Value = bytes.Clone(entry.Value)
	return entry
}
