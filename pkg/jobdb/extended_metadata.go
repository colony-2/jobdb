package jobdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/colony-2/jobdb/pkg/jobdb/clientpayload"
)

var (
	ErrInvalidJobMetadata     = errors.New("invalid job metadata edit")
	ErrJobMetadataUnsupported = errors.New("job metadata edits are unsupported")
)

// MetadataVersion is a nonnegative int64 encoded as a canonical decimal string.
type MetadataVersion int64

func (v MetadataVersion) MarshalJSON() ([]byte, error) {
	if v < 0 {
		return nil, ErrInvalidJobMetadata
	}
	return json.Marshal(strconv.FormatInt(int64(v), 10))
}
func (v *MetadataVersion) UnmarshalJSON(raw []byte) error {
	var s string
	if json.Unmarshal(raw, &s) != nil || s == "" || (len(s) > 1 && s[0] == '0') {
		return ErrInvalidJobMetadata
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return ErrInvalidJobMetadata
		}
	}
	n, e := strconv.ParseInt(s, 10, 64)
	if e != nil {
		return ErrInvalidJobMetadata
	}
	*v = MetadataVersion(n)
	return nil
}

func validJobMetadataKey(key string) bool {
	if len(key) < 1 || len(key) > 512 || !utf8.ValidString(key) {
		return false
	}
	for _, r := range key {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

type JobMetadataEntry struct {
	Version MetadataVersion `json:"version"`
	Present bool            `json:"present"`
	Value   json.RawMessage `json:"value,omitempty"`
}

type JobMetadataEdit struct {
	Key             string           `json:"key"`
	ExpectedVersion *MetadataVersion `json:"expectedVersion"`
	Set             json.RawMessage  `json:"set,omitempty"`
	Remove          bool             `json:"remove,omitempty"`
}

func (edit JobMetadataEdit) Validate() error {
	if !validJobMetadataKey(edit.Key) || edit.ExpectedVersion == nil || *edit.ExpectedVersion < 0 || (edit.Set != nil) == edit.Remove {
		return ErrInvalidJobMetadata
	}
	if edit.Set != nil {
		if err := clientpayload.ValidateJSONWithLimits(edit.Set, 256<<10, 127); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidJobMetadata, err)
		}
	}
	return nil
}

func (edit *JobMetadataEdit) UnmarshalJSON(raw []byte) error {
	if err := clientpayload.ValidateJSONWithLimits(raw, 300<<10, 128); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidJobMetadata, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for name := range fields {
		switch name {
		case "key", "expectedVersion", "set", "remove":
		default:
			return ErrInvalidJobMetadata
		}
	}
	type plain JobMetadataEdit
	var decoded plain
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	if fields["remove"] != nil && !decoded.Remove {
		return ErrInvalidJobMetadata
	}
	*edit = JobMetadataEdit(decoded)
	return edit.Validate()
}

// JobMetadataRuntime is the optional lease-free editing capability.
type JobMetadataRuntime interface {
	EditJobMetadata(context.Context, JobKey, JobMetadataEdit) (JobMetadataEntry, error)
}

// JobMetadataLease edits metadata only on the job to which the lease belongs.
type JobMetadataLease interface {
	EditJobMetadata(context.Context, JobMetadataEdit) (JobMetadataEntry, error)
}
