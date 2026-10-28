package migration

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"time"

	pb "github.com/colony-2/jobdb/pkg/internal/storagepb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Repair only typed JobDB identity fields. Opaque application bytes (including
// user strings resembling hashes) are never rewritten.
func (c *converter) repairChapterIdentities() error {
	if c.opts.Mode != "rewrite" || c.version != 8 || len(c.hashes) == 0 {
		return nil
	}
	var cursor int64
	for {
		var id int64
		var tenant string
		var body []byte
		err := c.conn.QueryRowContext(c.ctx, "SELECT rowid,anthology_id,body FROM jobdb_chapter_chapters WHERE rowid>? ORDER BY rowid LIMIT 1", cursor).Scan(&id, &tenant, &body)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		cursor = id
		var p pb.ChapterRecord
		if err = proto.Unmarshal(body, &p); err != nil {
			return err
		}
		changed := false
		if hash, ok := c.hashes[tenant+"/"+p.GetInputHash()]; ok {
			p.SetInputHash(hash)
			changed = true
		}
		var walk func(protoreflect.Message)
		walk = func(m protoreflect.Message) {
			if m.Descriptor().FullName() == "jobdb.storage.v1.InputReference" {
				f := m.Descriptor().Fields().ByName("hash")
				if hash, ok := c.hashes[tenant+"/"+m.Get(f).String()]; ok {
					m.Set(f, protoreflect.ValueOfString(hash))
					changed = true
				}
			}
			m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
				if f.IsMap() {
					if f.MapValue().Kind() == protoreflect.MessageKind {
						v.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool { walk(v.Message()); return true })
					}
				} else if f.IsList() {
					if f.Kind() == protoreflect.MessageKind {
						for i := 0; i < v.List().Len(); i++ {
							walk(v.List().Get(i).Message())
						}
					}
				} else if f.Kind() == protoreflect.MessageKind {
					walk(v.Message())
				}
				return true
			})
		}
		walk(p.ProtoReflect())
		if changed {
			raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(&p)
			if err != nil {
				return err
			}
			if _, err = c.conn.ExecContext(c.ctx, "UPDATE jobdb_chapter_chapters SET body=? WHERE rowid=?", raw, id); err != nil {
				return err
			}
			c.report.RepairedReferenceChapters++
		}
	}
}
func (c *converter) rememberHash(old, new string) error {
	if c.hashes == nil {
		c.hashes = map[string]string{}
	}
	key := c.tenant + "/" + old
	if existing, ok := c.hashes[key]; ok && existing != new {
		return fmt.Errorf("ambiguous execution identity during migration")
	}
	c.hashes[key] = new
	return nil
}

// Freeze the schema-8 Go JSON identity contract, independently of future APIs.
type legacyIdentityExecutionSnapshot struct {
	Data      json.RawMessage                   `json:"data"`
	Artifacts []legacyIdentityExecutionArtifact `json:"artifacts,omitempty"`
}
type legacyIdentityExecutionArtifact struct {
	Key  *jobdb.ArtifactKey `json:"key,omitempty"`
	Name string             `json:"name"`
	Data []byte             `json:"data"`
}

// legacyIdentityExecutionRecord is the versioned execution history wire contract. Unknown
// fields and kinds are rejected rather than interpreted as a different event.
type legacyIdentityExecutionRecord struct {
	Version           uint32                           `json:"version"`
	Kind              string                           `json:"kind"`
	ID                string                           `json:"id,omitempty"`
	Activation        string                           `json:"activation,omitempty"`
	Versions          *jobdb.ExecutionVersions         `json:"versions,omitempty"`
	At                time.Time                        `json:"at"`
	Descriptor        json.RawMessage                  `json:"descriptor,omitempty"`
	Attempt           int                              `json:"attempt,omitempty"`
	Deadline          time.Time                        `json:"deadline,omitempty"`
	TotalDeadline     time.Time                        `json:"total_deadline,omitempty"`
	Input             *legacyIdentityExecutionSnapshot `json:"input,omitempty"`
	Output            *legacyIdentityExecutionSnapshot `json:"output,omitempty"`
	ErrorKind         string                           `json:"error_kind,omitempty"`
	Error             json.RawMessage                  `json:"error,omitempty"`
	Rule              string                           `json:"rule,omitempty"`
	Disposition       string                           `json:"disposition,omitempty"`
	Retryable         *bool                            `json:"retryable,omitempty"`
	Retry             bool                             `json:"retry,omitempty"`
	WakeAt            time.Time                        `json:"wake_at,omitempty"`
	Source            *jobdb.JobKey                    `json:"source,omitempty"`
	Payload           json.RawMessage                  `json:"payload,omitempty"`
	Dependencies      []jobdb.Dependency               `json:"dependencies,omitempty"`
	Prerequisites     []jobdb.JobPrerequisite          `json:"prerequisites,omitempty"`
	AlternateAt       *time.Time                       `json:"alternate_at,omitempty"`
	ExecutionTaskType string                           `json:"execution_task_type,omitempty"`
}

func legacyExecutionIdentity(raw []byte) (string, error) {
	var r legacyIdentityExecutionRecord
	if err := json.Unmarshal(raw, &r); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(canonical)), nil
}
