// Package migration contains the offline-only decoder for schema-8 chapters.
// Live runtimes never import it or accept the previous representation.
package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/colony-2/jobdb/pkg/internal/executionmeta"
	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	pb "github.com/colony-2/jobdb/pkg/internal/storagepb/v1"
	"github.com/colony-2/jobdb/pkg/jobdb"
	runtimecore "github.com/colony-2/jobdb/pkg/jobdb/runtime/core"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

type Options struct {
	Database                          string
	BlobDir                           string
	Mode                              string // check, rewrite, verify
	MaxChapterApplicationPayloadBytes int64
}
type Report struct {
	PayloadViolationCount     int64                                   `json:"payloadViolationCount"`
	RepairedReferenceChapters int64                                   `json:"repairedReferenceChapters"`
	SourceVersion             int                                     `json:"sourceVersion"`
	TargetVersion             int                                     `json:"targetVersion"`
	Chapters                  int64                                   `json:"chapters"`
	OldBodyBytes              int64                                   `json:"oldBodyBytes"`
	NewBodyBytes              int64                                   `json:"newBodyBytes"`
	LargestBodyBytes          int64                                   `json:"largestBodyBytes"`
	ArtifactReferences        int64                                   `json:"artifactReferences"`
	CreatedArtifacts          int64                                   `json:"createdArtifacts"`
	RepairedWaits             int64                                   `json:"repairedWaits"`
	PayloadViolations         []*jobdb.ChapterApplicationPayloadError `json:"payloadViolations,omitempty"`
}
type converter struct {
	depth       int
	hashes      map[string]string
	ctx         context.Context
	conn        *sql.Conn
	opts        Options
	report      Report
	version     int
	tenant, job string
	ordinal     int64
	created     []string
}

// Run requires offline ownership. SQLite prevents other writes throughout the
// conversion. check/verify use a read-only connection and never mutate storage.
func Run(ctx context.Context, opts Options) (report Report, err error) {
	if opts.Mode != "check" && opts.Mode != "rewrite" && opts.Mode != "verify" {
		return report, fmt.Errorf("mode must be check, rewrite, or verify")
	}
	if opts.MaxChapterApplicationPayloadBytes < 0 {
		return report, fmt.Errorf("payload limit cannot be negative")
	}
	if opts.Database == "" {
		return report, fmt.Errorf("database is required")
	}
	path, err := filepath.Abs(opts.Database)
	if err != nil {
		return report, err
	}
	if _, err = os.Stat(path); err != nil {
		return report, err
	}
	if opts.BlobDir == "" {
		opts.BlobDir = path + ".blobs"
	}
	opts.BlobDir, err = filepath.Abs(opts.BlobDir)
	if err != nil {
		return report, err
	}
	mode := "ro"
	if opts.Mode == "rewrite" {
		mode = "rw"
	}
	dsn := url.URL{Scheme: "file", Path: filepath.ToSlash(path), RawQuery: "mode=" + mode}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		return report, err
	}
	defer conn.Close()
	for _, q := range []string{"PRAGMA foreign_keys=ON", "PRAGMA busy_timeout=1000", "PRAGMA cache_size=-8192"} {
		if _, err = conn.ExecContext(ctx, q); err != nil {
			return report, err
		}
	}
	begin := "BEGIN"
	if opts.Mode == "rewrite" {
		begin = "BEGIN EXCLUSIVE"
	}
	if _, err = conn.ExecContext(ctx, begin); err != nil {
		return report, fmt.Errorf("offline migration ownership: %w", err)
	}
	c := &converter{ctx: ctx, conn: conn, opts: opts}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			// Leave the manifest on uncertainty; the next offline run verifies
			// references before deleting migration-owned orphan blobs.
			if len(c.created) > 0 {
				_ = c.recoverBlobs()
			}
		}
	}()
	if err = conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&c.version); err != nil {
		return report, err
	}
	if opts.Mode == "rewrite" {
		if err = c.recoverBlobs(); err != nil {
			return c.report, err
		}
	}
	c.report.SourceVersion = c.version
	c.report.TargetVersion = 9
	if c.version != 8 && c.version != 9 {
		return c.report, fmt.Errorf("unsupported source schema %d; only schema 8 and converted schema 9 are supported", c.version)
	}
	if opts.Mode == "verify" && c.version != 9 {
		return c.report, fmt.Errorf("database has not been converted")
	}
	var cursor int64
	for {
		var rowid int64
		var body []byte
		err = conn.QueryRowContext(ctx, "SELECT rowid,anthology_id,story_id,ordinal,body FROM jobdb_chapter_chapters WHERE rowid>? ORDER BY rowid LIMIT 1", cursor).Scan(&rowid, &c.tenant, &c.job, &c.ordinal, &body)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return c.report, err
		}
		cursor = rowid
		encoded, err := c.chapter(body)
		if err != nil {
			return c.report, fmt.Errorf("chapter %s/%s/%d: %w", c.tenant, c.job, c.ordinal, err)
		}
		ch, err := runtimecore.DecodeChapter(runtimecore.EncodedChapter{Ordinal: c.ordinal, Payload: encoded})
		if err != nil {
			return c.report, err
		}
		if err = jobdb.ValidateChapterApplicationPayload(ctx, ch, opts.MaxChapterApplicationPayloadBytes); err != nil {
			var size *jobdb.ChapterApplicationPayloadError
			if !errors.As(err, &size) {
				return c.report, err
			}
			c.report.PayloadViolationCount++
			size.JobKey = &jobdb.JobKey{TenantId: c.tenant, JobId: c.job}
			if len(c.report.PayloadViolations) < 100 {
				c.report.PayloadViolations = append(c.report.PayloadViolations, size)
			}
		}
		c.report.Chapters++
		c.report.OldBodyBytes += int64(len(body))
		c.report.NewBodyBytes += int64(len(encoded))
		if int64(len(encoded)) > c.report.LargestBodyBytes {
			c.report.LargestBodyBytes = int64(len(encoded))
		}
		if opts.Mode == "rewrite" && c.version == 8 {
			if _, err = conn.ExecContext(ctx, "UPDATE jobdb_chapter_chapters SET body=? WHERE rowid=?", encoded, rowid); err != nil {
				return c.report, err
			}
		}
	}
	if len(c.report.PayloadViolations) > 0 {
		return c.report, fmt.Errorf("application payload limit exceeded; inspect report and select an explicit higher limit if appropriate")
	}
	if err = c.repairChapterIdentities(); err != nil {
		return c.report, err
	}
	if err = c.waits(); err != nil {
		return c.report, err
	}
	if opts.Mode == "rewrite" && c.version == 8 {
		if _, err = conn.ExecContext(ctx, "PRAGMA user_version=9"); err != nil {
			return c.report, err
		}
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return c.report, err
	}
	committed = true
	if len(c.created) > 0 {
		if err = c.recoverBlobs(); err != nil {
			return c.report, err
		}
	}
	return c.report, nil
}
func (c *converter) chapter(body []byte) ([]byte, error) {
	var chapter pb.ChapterRecord
	if c.version == 8 {
		var wrapper struct {
			Protobuf string `json:"protobuf"`
		}
		if err := json.Unmarshal(body, &wrapper); err != nil {
			return nil, err
		}
		raw, err := base64.StdEncoding.DecodeString(wrapper.Protobuf)
		if err != nil || len(raw) == 0 {
			return nil, fmt.Errorf("invalid legacy protobuf envelope")
		}
		if err = proto.Unmarshal(raw, &chapter); err != nil {
			return nil, err
		}
		if chapter.HasExecution() {
			raw, err := legacyRecordJSON(chapter.GetExecution())
			if err != nil {
				return nil, err
			}
			r, err := c.record(raw, "record")
			if err != nil {
				return nil, err
			}
			p, err := runtimecodec.ExecutionToProto(r)
			if err != nil {
				return nil, err
			}
			e := new(pb.ExecutionChapter)
			e.SetRecord(p)
			chapter.SetExecution(e)
		}
		if meta := chapter.GetMetadata(); meta != nil {
			if value, ok := meta.GetFields()[executionmeta.Key]; ok {
				if !value.HasStringValue() {
					return nil, fmt.Errorf("legacy reconstruction must be a string")
				}
				rs, err := c.records([]byte(value.GetStringValue()), "prefix")
				if err != nil {
					return nil, err
				}
				chapter.SetReconstruction(rs)
				delete(meta.GetFields(), executionmeta.Key)
			}
		}
		chapter.SetFormatVersion(runtimecodec.ChapterFormatVersion)
	} else {
		if err := proto.Unmarshal(body, &chapter); err != nil {
			return nil, err
		}
		// Decode strictly before traversing references; this rejects old containers.
		if _, err := runtimecodec.DecodeChapter(body); err != nil {
			return nil, err
		}
		if chapter.HasExecution() {
			if err := c.verifyRecord(chapter.GetExecution().GetRecord()); err != nil {
				return nil, err
			}
		}
		for _, r := range chapter.GetReconstruction() {
			if err := c.verifyRecord(r); err != nil {
				return nil, err
			}
		}
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(&chapter)
}
func legacyRecordJSON(e *pb.ExecutionChapter) ([]byte, error) {
	raw := e.ProtoReflect().GetUnknown()
	var found []byte
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return nil, fmt.Errorf("invalid legacy execution tag")
		}
		raw = raw[n:]
		if num != 1 || typ != protowire.BytesType || found != nil {
			return nil, fmt.Errorf("unsupported legacy execution field")
		}
		v, n := protowire.ConsumeBytes(raw)
		if n < 0 {
			return nil, fmt.Errorf("invalid legacy execution value")
		}
		found = v
		raw = raw[n:]
	}
	if found == nil {
		return nil, fmt.Errorf("legacy execution JSON missing")
	}
	return found, nil
}
func (c *converter) records(raw []byte, path string) ([]*pb.ExecutionRecord, error) {
	var rs []json.RawMessage
	if err := json.Unmarshal(raw, &rs); err != nil {
		return nil, err
	}
	out := make([]*pb.ExecutionRecord, 0, len(rs))
	for i, raw := range rs {
		r, err := c.record(raw, fmt.Sprintf("%s/%d", path, i))
		if err != nil {
			return nil, err
		}
		p, err := runtimecodec.ExecutionToProto(r)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// Shadow the two snapshots while decoding the old record so the legacy data
// field is accepted only on artifact descriptors. Reject unknown owned fields.
type legacyArtifact struct {
	Key  *jobdb.ArtifactKey `json:"key,omitempty"`
	Name string             `json:"name"`
	Data json.RawMessage    `json:"data"`
}
type legacySnapshot struct {
	Data      json.RawMessage  `json:"data"`
	Artifacts []legacyArtifact `json:"artifacts,omitempty"`
}
type legacyExecution struct {
	jobdb.ExecutionRecord
	Input  *legacySnapshot `json:"input,omitempty"`
	Output *legacySnapshot `json:"output,omitempty"`
}

func (c *converter) record(raw []byte, path string) (jobdb.ExecutionRecord, error) {
	c.depth++
	defer func() { c.depth-- }()
	if c.depth > 64 {
		return jobdb.ExecutionRecord{}, fmt.Errorf("legacy reconstruction depth exceeds 64")
	}
	var old legacyExecution
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&old); err != nil {
		return old.ExecutionRecord, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return old.ExecutionRecord, fmt.Errorf("trailing legacy execution data")
	}
	r := old.ExecutionRecord
	if r.Version != 3 {
		return r, fmt.Errorf("unsupported execution version %d", r.Version)
	}
	for _, entry := range []struct {
		name   string
		old    *legacySnapshot
		target **jobdb.ExecutionSnapshot
	}{{"input", old.Input, &r.Input}, {"output", old.Output, &r.Output}} {
		if entry.old == nil {
			continue
		}
		snapshot := &jobdb.ExecutionSnapshot{Data: entry.old.Data}
		*entry.target = snapshot
		for i, oldArtifact := range entry.old.Artifacts {
			a := jobdb.ExecutionArtifact{Key: oldArtifact.Key, Name: oldArtifact.Name}
			var content []byte
			present := len(oldArtifact.Data) > 0
			if present {
				if err := json.Unmarshal(oldArtifact.Data, &content); err != nil {
					return r, err
				}
			}
			if err := c.artifact(&a, content, present, fmt.Sprintf("%s/%s/%d", path, entry.name, i)); err != nil {
				return r, err
			}
			snapshot.Artifacts = append(snapshot.Artifacts, a)
		}
	}

	if r.Kind == "restart" && len(r.Descriptor) > 0 {
		var rs []json.RawMessage
		if err := json.Unmarshal(r.Descriptor, &rs); err != nil {
			return r, err
		}
		converted := make([]jobdb.ExecutionRecord, 0, len(rs))
		for i, raw := range rs {
			v, err := c.record(raw, fmt.Sprintf("%s/retained/%d", path, i))
			if err != nil {
				return r, err
			}
			converted = append(converted, v)
		}
		var err error
		r.Descriptor, err = json.Marshal(converted)
		if err != nil {
			return r, err
		}
	}
	r.Version = jobdb.ExecutionPatternVersion
	if r.Versions != nil {
		r.Versions.ExecutionPatternVersion = r.Version
	}
	if r.Kind == "wait" {
		hash, err := runtimecodec.ExecutionIdentity(r)
		if err != nil {
			return r, err
		}
		if err = c.rememberHash(fmt.Sprintf("%x", sha256.Sum256(raw)), hash); err != nil {
			return r, err
		}
		oldHash, err := legacyExecutionIdentity(raw)
		if err != nil {
			return r, err
		}
		if err = c.rememberHash(oldHash, hash); err != nil {
			return r, err
		}
	}

	return r, nil
}
func (c *converter) artifact(a *jobdb.ExecutionArtifact, content []byte, present bool, path string) error {
	c.report.ArtifactReferences++
	if a.Key != nil {
		size, hash, err := c.verifyArtifact(*a.Key)
		if err != nil {
			return fmt.Errorf("artifact %q: %w", a.Name, err)
		}
		if a.Key.SizeBytes >= 0 && a.Key.SizeBytes != size {
			return fmt.Errorf("artifact %q key size mismatch", a.Name)
		}
		if present && (int64(len(content)) != size || fmt.Sprintf("%x", sha256.Sum256(content)) != hash) {
			return fmt.Errorf("artifact %q inline copy differs from stored bytes", a.Name)
		}
		a.Key.SizeBytes = size
		return nil
	}
	if !present {
		return fmt.Errorf("artifact %q has neither key nor recoverable bytes", a.Name)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(content))
	name := fmt.Sprintf("migration-%x", sha256.Sum256([]byte(path+"/"+a.Name)))
	key := jobdb.ArtifactKey{JobId: c.job, TaskOrdinal: c.ordinal, Name: name, SizeBytes: int64(len(content))}
	if size, got, err := c.verifyArtifact(key); err == nil {
		if size != key.SizeBytes || got != hash {
			return fmt.Errorf("migration artifact collision")
		}
		a.Key = &key
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	a.Key = &key
	c.report.CreatedArtifacts++
	if c.opts.Mode != "rewrite" {
		return nil
	}
	var position int
	if err := c.conn.QueryRowContext(c.ctx, "SELECT coalesce(max(position)+1,0) FROM jobdb_chapter_artifacts WHERE anthology_id=? AND story_id=? AND ordinal=?", c.tenant, c.job, c.ordinal).Scan(&position); err != nil {
		return err
	}
	var inline []byte
	var blob any
	id := fmt.Sprintf("migration-%x", sha256.Sum256([]byte(c.tenant+"/"+c.job+"/"+fmt.Sprint(c.ordinal)+"/"+name+"/"+hash)))
	if len(content) <= 256 {
		inline = content
		if inline == nil {
			inline = []byte{}
		}
	} else {
		if err := os.MkdirAll(c.opts.BlobDir, 0700); err != nil {
			return err
		}
		target := filepath.Join(c.opts.BlobDir, id)
		if _, err := os.Lstat(target); err == nil {
			return fmt.Errorf("migration blob already exists without a matching artifact row: %s", id)
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := c.journalBlob(id); err != nil {
			return err
		}
		f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return fmt.Errorf("stage blob %s: %w", id, err)
		}
		c.created = append(c.created, target)
		_, err = f.Write(content)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		dir, err := os.Open(c.opts.BlobDir)
		if err != nil {
			return err
		}
		err = dir.Sync()
		_ = dir.Close()
		if err != nil {
			return err
		}
		blob = id
	}
	_, err := c.conn.ExecContext(c.ctx, `INSERT INTO jobdb_chapter_artifacts(anthology_id,story_id,ordinal,position,id,name,content_type,size_bytes,sha256,inline_data,blob_path) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, c.tenant, c.job, c.ordinal, position, id, name, "application/octet-stream", key.SizeBytes, hash, inline, blob)
	return err
}
func (c *converter) verifyArtifact(key jobdb.ArtifactKey) (int64, string, error) {
	tenant, job := c.tenant, key.JobId
	for depth := 0; depth < 128; depth++ {
		var size int64
		var hash string
		var inline []byte
		var blob sql.NullString
		err := c.conn.QueryRowContext(c.ctx, `SELECT size_bytes,sha256,inline_data,blob_path FROM jobdb_chapter_artifacts WHERE anthology_id=? AND story_id=? AND ordinal=? AND name=?`, tenant, job, key.TaskOrdinal, key.Name).Scan(&size, &hash, &inline, &blob)
		if errors.Is(err, sql.ErrNoRows) {
			var bt, bj sql.NullString
			var fork sql.NullInt64
			e := c.conn.QueryRowContext(c.ctx, "SELECT base_anthology_id,base_story_id,base_fork_ordinal FROM jobdb_chapter_stories WHERE anthology_id=? AND story_id=?", tenant, job).Scan(&bt, &bj, &fork)
			if e != nil || !bt.Valid || !bj.Valid || !fork.Valid || key.TaskOrdinal > fork.Int64 {
				return 0, "", sql.ErrNoRows
			}
			tenant, job = bt.String, bj.String
			continue
		}
		if err != nil {
			return 0, "", err
		}
		var reader io.Reader = bytes.NewReader(inline)
		var f *os.File
		if blob.Valid {
			if !filepath.IsLocal(blob.String) {
				return 0, "", fmt.Errorf("unsupported blob path %q", blob.String)
			}
			f, err = os.Open(filepath.Join(c.opts.BlobDir, blob.String))
			if err != nil {
				return 0, "", err
			}
			reader = f
		} else if inline == nil && size != 0 {
			return 0, "", fmt.Errorf("artifact has no backing content")
		}
		h := sha256.New()
		n, err := io.Copy(h, reader)
		if f != nil {
			_ = f.Close()
		}
		if err != nil {
			return 0, "", err
		}
		digest := fmt.Sprintf("%x", h.Sum(nil))
		if n != size || digest != hash {
			return 0, "", fmt.Errorf("stored artifact integrity mismatch")
		}
		return size, hash, nil
	}
	return 0, "", fmt.Errorf("story link cycle or excessive depth")
}
func (c *converter) verifyRecord(r *pb.ExecutionRecord) error {
	for _, s := range []*pb.ExecutionSnapshot{r.GetInput(), r.GetOutput()} {
		if s == nil {
			continue
		}
		for _, a := range s.GetArtifacts() {
			c.report.ArtifactReferences++
			size, _, err := c.verifyArtifact(jobdb.ArtifactKey{JobId: a.GetJobId(), TaskOrdinal: a.GetOrdinal(), Name: a.GetStoredName()})
			if err != nil {
				return err
			}
			if size != a.GetSizeBytes() {
				return fmt.Errorf("artifact key size mismatch")
			}
		}
	}
	for _, r := range r.GetRetained() {
		if err := c.verifyRecord(r); err != nil {
			return err
		}
	}
	return nil
}
func (c *converter) waits() error {
	var cursor int64
	for {
		var rowid int64
		var tenant, job string
		var raw []byte
		err := c.conn.QueryRowContext(c.ctx, "SELECT rowid,tenant_id,job_id,payload FROM jobdb_jobs WHERE rowid>? ORDER BY rowid LIMIT 1", cursor).Scan(&rowid, &tenant, &job, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		cursor = rowid
		payload, err := runtimecodec.DecodeSchedulerPayload(raw)
		if err != nil {
			return err
		}
		if payload.TaskWait == nil {
			continue
		}
		body, owner, err := c.chapterAt(tenant, job, payload.TaskWait.InputStep)
		if err != nil {
			return err
		}
		oldtenant, oldjob, oldord := c.tenant, c.job, c.ordinal
		c.tenant, c.job, c.ordinal = tenant, owner, payload.TaskWait.InputStep
		version := c.version
		if c.opts.Mode == "rewrite" {
			c.version = 9
		}
		references, created := c.report.ArtifactReferences, c.report.CreatedArtifacts
		converted, err := c.chapter(body)
		c.report.ArtifactReferences, c.report.CreatedArtifacts = references, created
		c.version = version
		c.tenant, c.job, c.ordinal = oldtenant, oldjob, oldord
		if err != nil {
			return err
		}
		env, err := runtimecodec.DecodeChapter(converted)
		if err != nil {
			return err
		}
		if env.ChapterType != "Execution" {
			continue
		}
		rec, err := jobdb.DecodeExecutionRecord(env.Payload)
		if err != nil {
			return err
		}
		if rec.Kind != "wait" {
			return fmt.Errorf("task wait points to %s", rec.Kind)
		}
		hash, err := runtimecodec.ExecutionIdentity(rec)
		if err != nil {
			return err
		}
		if hash == payload.TaskWait.InputHash {
			continue
		}
		if c.version == 9 {
			return fmt.Errorf("stale wait hash for %s/%s", tenant, job)
		}
		c.report.RepairedWaits++
		payload.TaskWait.InputHash = hash
		if c.opts.Mode == "rewrite" {
			raw, err = runtimecodec.EncodeSchedulerPayload(payload)
			if err != nil {
				return err
			}
			if _, err = c.conn.ExecContext(c.ctx, "UPDATE jobdb_jobs SET payload=? WHERE rowid=?", raw, rowid); err != nil {
				return err
			}
		}
	}
}
func (c *converter) chapterAt(tenant, job string, ordinal int64) ([]byte, string, error) {
	for depth := 0; depth < 128; depth++ {
		var body []byte
		err := c.conn.QueryRowContext(c.ctx, "SELECT body FROM jobdb_chapter_chapters WHERE anthology_id=? AND story_id=? AND ordinal=?", tenant, job, ordinal).Scan(&body)
		if err == nil {
			return body, job, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, "", err
		}
		var bt, bj sql.NullString
		var fork sql.NullInt64
		err = c.conn.QueryRowContext(c.ctx, "SELECT base_anthology_id,base_story_id,base_fork_ordinal FROM jobdb_chapter_stories WHERE anthology_id=? AND story_id=?", tenant, job).Scan(&bt, &bj, &fork)
		if err != nil || !bt.Valid || !bj.Valid || ordinal > fork.Int64 {
			return nil, "", fmt.Errorf("missing chapter for wait")
		}
		tenant, job = bt.String, bj.String
	}
	return nil, "", fmt.Errorf("story link cycle")
}
