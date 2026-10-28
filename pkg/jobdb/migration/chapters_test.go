package migration_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	pb "github.com/colony-2/jobdb/pkg/internal/storagepb/v1"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/migration"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func legacyFixture(t *testing.T, inlineOnly, waiting bool) (migration.Options, []byte) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "job.db")
	rt, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{DBPath: path})
	require.NoError(t, err)
	_, err = rt.SubmitJob(ctx, jobdb.SubmitJobRequest{Job: jobdb.SubmitJob{TenantId: "tenant", JobID: "job", JobType: "test", Data: jobdb.NewTaskDataOrPanic("input")}})
	require.NoError(t, err)
	require.NoError(t, rt.Close(ctx))
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	defer db.Close()
	var start []byte
	require.NoError(t, db.QueryRow("SELECT body FROM jobdb_chapter_chapters WHERE ordinal=0").Scan(&start))
	var p pb.ChapterRecord
	require.NoError(t, proto.Unmarshal(start, &p))
	p.ClearFormatVersion()
	raw, err := proto.Marshal(&p)
	require.NoError(t, err)
	wrapped, _ := json.Marshal(map[string]string{"protobuf": base64.StdEncoding.EncodeToString(raw)})
	_, err = db.Exec("UPDATE jobdb_chapter_chapters SET body=? WHERE ordinal=0", wrapped)
	require.NoError(t, err)
	data := []byte(strings.Repeat("artifact contents", 100))
	a := map[string]any{"name": "logical-name", "data": data}
	if !inlineOnly {
		a["key"] = jobdb.ArtifactKey{JobId: "job", TaskOrdinal: 1, Name: "stored-name", SizeBytes: int64(len(data))}
	}
	rec := map[string]any{"version": 3, "kind": "result", "id": "task", "output": map[string]any{"data": map[string]any{"number": json.Number("9007199254740993"), "data": "application data stays", "artifacts": "user field stays"}, "artifacts": []any{a}}}
	if waiting {
		rec["kind"] = "wait"
		rec["descriptor"] = "external"
		rec["input"] = rec["output"]
		delete(rec, "output")
	}
	oldJSON, err := json.Marshal(rec)
	require.NoError(t, err)
	e := new(pb.ExecutionChapter)
	e.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), oldJSON))
	p = newChapter(e)
	raw, err = proto.Marshal(&p)
	require.NoError(t, err)
	wrapped, _ = json.Marshal(map[string]string{"protobuf": base64.StdEncoding.EncodeToString(raw)})
	_, err = db.Exec("INSERT INTO jobdb_chapter_chapters(anthology_id,story_id,ordinal,body,created_at_ns) VALUES('tenant','job',1,?,1)", wrapped)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE jobdb_chapter_stories SET latest_ordinal=1,chapter_count=2")
	require.NoError(t, err)
	if !inlineOnly {
		require.NoError(t, os.WriteFile(path+".blobs/blob", data, 0600))
		_, err = db.Exec(`INSERT INTO jobdb_chapter_artifacts(anthology_id,story_id,ordinal,position,id,name,content_type,size_bytes,sha256,blob_path) VALUES('tenant','job',1,0,'blob','stored-name','application/octet-stream',?,?,'blob')`, len(data), fmt.Sprintf("%x", sha256.Sum256(data)))
		require.NoError(t, err)
	}
	if waiting {
		payload, err := runtimecodec.EncodeSchedulerPayload(runtimecodec.SchedulerPayload{TaskWait: &runtimecodec.TaskWait{InputStep: 1, OutputStep: 2, Next: "test", InputHash: fmt.Sprintf("%x", sha256.Sum256(oldJSON))}})
		require.NoError(t, err)
		_, err = db.Exec("UPDATE jobdb_jobs SET payload=?,route_task_type='external'", payload)
		require.NoError(t, err)
	}
	_, err = db.Exec("PRAGMA user_version=8")
	require.NoError(t, err)
	return migration.Options{Database: path, BlobDir: path + ".blobs", Mode: "check"}, wrapped
}
func newChapter(e *pb.ExecutionChapter) pb.ChapterRecord {
	p := new(pb.ChapterRecord)
	p.SetOrdinal(1)
	p.SetTaskType("__execution__")
	p.SetExecution(e)
	return *p
}
func TestOfflineChapterConversion(t *testing.T) {
	for _, inlineOnly := range []bool{false, true} {
		t.Run(fmt.Sprint(inlineOnly), func(t *testing.T) {
			opts, old := legacyFixture(t, inlineOnly, false)
			ctx := context.Background()
			_, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{DBPath: opts.Database})
			require.ErrorContains(t, err, "jobdb-migrate")
			report, err := migration.Run(ctx, opts)
			require.NoError(t, err)
			require.EqualValues(t, 2, report.Chapters)
			require.Less(t, report.NewBodyBytes, report.OldBodyBytes)
			db, err := sql.Open("sqlite", opts.Database)
			require.NoError(t, err)
			defer db.Close()
			var body []byte
			require.NoError(t, db.QueryRow("SELECT body FROM jobdb_chapter_chapters WHERE ordinal=1").Scan(&body))
			require.Equal(t, old, body)
			opts.Mode = "rewrite"
			report, err = migration.Run(ctx, opts)
			require.NoError(t, err)
			if inlineOnly {
				require.EqualValues(t, 1, report.CreatedArtifacts)
			}
			opts.Mode = "verify"
			_, err = migration.Run(ctx, opts)
			require.NoError(t, err)
			opts.Mode = "rewrite"
			_, err = migration.Run(ctx, opts)
			require.NoError(t, err)
			rt, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{DBPath: opts.Database})
			require.NoError(t, err)
			defer rt.Close(ctx)
			ch, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, Ordinal: 1})
			require.NoError(t, err)
			record, err := jobdb.DecodeExecutionRecord(ch.Body.(jobdb.ExecutionChapter).Record)
			require.NoError(t, err)
			require.Contains(t, string(record.Output.Data), "9007199254740993")
			require.Contains(t, string(record.Output.Data), "application data stays")
			key := record.Output.Artifacts[0].Key
			require.NotNil(t, key)
			reader, err := rt.OpenArtifact(ctx, jobdb.ArtifactRef{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: key.JobId}, Ordinal: key.TaskOrdinal, Name: key.Name})
			require.NoError(t, err)
			require.EqualValues(t, 1700, reader.Size())
		})
	}
}
func TestMigrationRepairsWaitGeneration(t *testing.T) {
	opts, _ := legacyFixture(t, false, true)
	opts.Mode = "rewrite"
	ctx := context.Background()
	report, err := migration.Run(ctx, opts)
	require.NoError(t, err)
	require.EqualValues(t, 1, report.RepairedWaits)
	opts.Mode = "verify"
	_, err = migration.Run(ctx, opts)
	require.NoError(t, err)
	rt, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{DBPath: opts.Database})
	require.NoError(t, err)
	defer rt.Close(ctx)
	ch, err := rt.GetChapter(ctx, jobdb.ChapterRef{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, Ordinal: 1})
	require.NoError(t, err)
	r, err := jobdb.DecodeExecutionRecord(ch.Body.(jobdb.ExecutionChapter).Record)
	require.NoError(t, err)
	hash, err := runtimecodec.ExecutionIdentity(r)
	require.NoError(t, err)
	req := jobdb.CompleteTaskIfWaitingRequest{JobKey: jobdb.JobKey{TenantId: "tenant", JobId: "job"}, Route: jobdb.Route{JobType: "test", TaskType: "external"}, ResumeJobType: "test", InputOrdinal: 1, OutputOrdinal: 2, InputHash: "stale", Data: jobdb.NewTaskDataOrPanic("answer")}
	require.ErrorIs(t, rt.CompleteTaskIfWaiting(ctx, req), jobdb.ErrConflict)
	req.InputHash = hash
	require.NoError(t, rt.CompleteTaskIfWaiting(ctx, req))
}
func TestMigrationRollbackAndCorruption(t *testing.T) {
	for _, mode := range []string{"limit", "blob"} {
		t.Run(mode, func(t *testing.T) {
			opts, old := legacyFixture(t, false, false)
			opts.Mode = "rewrite"
			if mode == "limit" {
				opts.MaxChapterApplicationPayloadBytes = 1
			} else {
				require.NoError(t, os.WriteFile(opts.BlobDir+"/blob", []byte("corrupted"), 0600))
			}
			_, err := migration.Run(context.Background(), opts)
			require.Error(t, err)
			db, err := sql.Open("sqlite", opts.Database)
			require.NoError(t, err)
			defer db.Close()
			var version int
			require.NoError(t, db.QueryRow("PRAGMA user_version").Scan(&version))
			require.Equal(t, 8, version)
			var body []byte
			require.NoError(t, db.QueryRow("SELECT body FROM jobdb_chapter_chapters WHERE ordinal=1").Scan(&body))
			require.Equal(t, old, body)
		})
	}
}

func TestMigrationEmptyArtifactWithoutBackingBytes(t *testing.T) {
	opts, _ := legacyFixture(t, false, false)
	db, err := sql.Open("sqlite", opts.Database)
	require.NoError(t, err)
	_, err = db.Exec("UPDATE jobdb_chapter_artifacts SET size_bytes=0,sha256=?,blob_path=NULL,inline_data=NULL", fmt.Sprintf("%x", sha256.Sum256(nil)))
	require.NoError(t, err)
	var wrapper struct {
		Protobuf string `json:"protobuf"`
	}
	var body []byte
	require.NoError(t, db.QueryRow("SELECT body FROM jobdb_chapter_chapters WHERE ordinal=1").Scan(&body))
	require.NoError(t, json.Unmarshal(body, &wrapper))
	raw, err := base64.StdEncoding.DecodeString(wrapper.Protobuf)
	require.NoError(t, err)
	var p pb.ChapterRecord
	require.NoError(t, proto.Unmarshal(raw, &p))
	unknown := p.GetExecution().ProtoReflect().GetUnknown()
	_, _, n := protowire.ConsumeTag(unknown)
	old, _ := protowire.ConsumeBytes(unknown[n:])
	var r map[string]any
	require.NoError(t, json.Unmarshal(old, &r))
	a := r["output"].(map[string]any)["artifacts"].([]any)[0].(map[string]any)
	a["data"] = nil
	a["key"].(map[string]any)["sizeBytes"] = 0
	old, err = json.Marshal(r)
	require.NoError(t, err)
	p.GetExecution().ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), old))
	raw, err = proto.Marshal(&p)
	require.NoError(t, err)
	body, _ = json.Marshal(map[string]string{"protobuf": base64.StdEncoding.EncodeToString(raw)})
	_, err = db.Exec("UPDATE jobdb_chapter_chapters SET body=? WHERE ordinal=1", body)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	opts.Mode = "rewrite"
	_, err = migration.Run(context.Background(), opts)
	require.NoError(t, err)
	opts.Mode = "verify"
	_, err = migration.Run(context.Background(), opts)
	require.NoError(t, err)
}

// editLegacyChapter preserves the old wire container while changing known fields.
func editLegacyChapter(t *testing.T, opts migration.Options, edit func(*pb.ChapterRecord, map[string]any)) {
	t.Helper()
	db, err := sql.Open("sqlite", opts.Database)
	require.NoError(t, err)
	defer db.Close()
	var body []byte
	require.NoError(t, db.QueryRow("SELECT body FROM jobdb_chapter_chapters WHERE ordinal=1").Scan(&body))
	var wrapper struct {
		Protobuf string `json:"protobuf"`
	}
	require.NoError(t, json.Unmarshal(body, &wrapper))
	raw, err := base64.StdEncoding.DecodeString(wrapper.Protobuf)
	require.NoError(t, err)
	var p pb.ChapterRecord
	require.NoError(t, proto.Unmarshal(raw, &p))
	unknown := p.GetExecution().ProtoReflect().GetUnknown()
	_, _, n := protowire.ConsumeTag(unknown)
	old, _ := protowire.ConsumeBytes(unknown[n:])
	var r map[string]any
	dec := json.NewDecoder(strings.NewReader(string(old)))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&r))
	before, err := json.Marshal(r)
	require.NoError(t, err)
	original := old
	edit(&p, r)
	old, err = json.Marshal(r)
	require.NoError(t, err)
	if string(before) == string(old) {
		old = original
	}
	p.GetExecution().ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), old))
	raw, err = proto.Marshal(&p)
	require.NoError(t, err)
	body, err = json.Marshal(map[string]string{"protobuf": base64.StdEncoding.EncodeToString(raw)})
	require.NoError(t, err)
	_, err = db.Exec("UPDATE jobdb_chapter_chapters SET body=? WHERE ordinal=1", body)
	require.NoError(t, err)
}
func TestMigrationRejectsUnknownOwnedFields(t *testing.T) {
	for _, location := range []string{"record", "snapshot", "artifact"} {
		t.Run(location, func(t *testing.T) {
			opts, _ := legacyFixture(t, false, false)
			editLegacyChapter(t, opts, func(_ *pb.ChapterRecord, r map[string]any) {
				target := r
				if location != "record" {
					target = r["output"].(map[string]any)
				}
				if location == "artifact" {
					target = target["artifacts"].([]any)[0].(map[string]any)
				}
				target["future_semantics"] = true
			})
			opts.Mode = "rewrite"
			_, err := migration.Run(context.Background(), opts)
			require.ErrorContains(t, err, "unknown field")
		})
	}
}
func TestMigrationBlobRollbackAndCrashRecovery(t *testing.T) {
	t.Run("rollback", func(t *testing.T) {
		opts, _ := legacyFixture(t, true, false)
		opts.Mode = "rewrite"
		opts.MaxChapterApplicationPayloadBytes = 1
		_, err := migration.Run(context.Background(), opts)
		require.Error(t, err)
		files, err := os.ReadDir(opts.BlobDir)
		require.NoError(t, err)
		require.Empty(t, files)
		opts.MaxChapterApplicationPayloadBytes = 0
		_, err = migration.Run(context.Background(), opts)
		require.NoError(t, err)
	})
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("crash_committed_%v", committed), func(t *testing.T) {
			opts, _ := legacyFixture(t, false, false)
			const name = "migration-interrupted"
			if committed {
				require.NoError(t, os.Rename(filepath.Join(opts.BlobDir, "blob"), filepath.Join(opts.BlobDir, name)))
				db, err := sql.Open("sqlite", opts.Database)
				require.NoError(t, err)
				_, err = db.Exec("UPDATE jobdb_chapter_artifacts SET blob_path=?", name)
				require.NoError(t, err)
				require.NoError(t, db.Close())
			} else {
				require.NoError(t, os.WriteFile(filepath.Join(opts.BlobDir, name), []byte("uncommitted"), 0600))
			}
			// Seed the durable journal at the point immediately before/after COMMIT.
			manifest := filepath.Join(opts.BlobDir, fmt.Sprintf(".jobdb-chapter-migration-%x.json", sha256.Sum256([]byte(opts.Database))))
			require.NoError(t, os.WriteFile(manifest, []byte(`["migration-interrupted"]`), 0600))
			opts.Mode = "rewrite"
			_, err := migration.Run(context.Background(), opts)
			require.NoError(t, err)
			_, err = os.Stat(manifest)
			require.True(t, os.IsNotExist(err))
			_, err = os.Stat(filepath.Join(opts.BlobDir, name))
			if committed {
				require.NoError(t, err)
			} else {
				require.True(t, os.IsNotExist(err))
			}
			opts.Mode = "verify"
			_, err = migration.Run(context.Background(), opts)
			require.NoError(t, err)
		})
	}
}
func TestMigrationRepairsTypedReferencesAndPreservesApplicationData(t *testing.T) {
	opts, _ := legacyFixture(t, false, true)
	db, err := sql.Open("sqlite", opts.Database)
	require.NoError(t, err)
	defer db.Close()
	var raw []byte
	require.NoError(t, db.QueryRow("SELECT payload FROM jobdb_jobs").Scan(&raw))
	scheduler, err := runtimecodec.DecodeSchedulerPayload(raw)
	require.NoError(t, err)
	oldHash := scheduler.TaskWait.InputHash
	editLegacyChapter(t, opts, func(p *pb.ChapterRecord, _ map[string]any) {
		p.SetInputHash(oldHash)
		ref := new(pb.InputReference)
		ref.SetHash(oldHash)
		p.SetInputRef(ref)
	})
	opts.Mode = "rewrite"
	report, err := migration.Run(context.Background(), opts)
	require.NoError(t, err)
	require.EqualValues(t, 1, report.RepairedReferenceChapters)
	require.NoError(t, db.QueryRow("SELECT payload FROM jobdb_jobs").Scan(&raw))
	scheduler, err = runtimecodec.DecodeSchedulerPayload(raw)
	require.NoError(t, err)
	require.NotEqual(t, oldHash, scheduler.TaskWait.InputHash)
	require.NoError(t, db.QueryRow("SELECT body FROM jobdb_chapter_chapters WHERE ordinal=1").Scan(&raw))
	var p pb.ChapterRecord
	require.NoError(t, proto.Unmarshal(raw, &p))
	require.Equal(t, scheduler.TaskWait.InputHash, p.GetInputHash())
	require.Equal(t, scheduler.TaskWait.InputHash, p.GetInputRef().GetHash())
	require.Contains(t, string(p.GetExecution().GetRecord().GetInput().GetData()), "application data stays")
}

func TestMigrationNestedReconstruction(t *testing.T) {
	opts, _ := legacyFixture(t, false, false)
	editLegacyChapter(t, opts, func(p *pb.ChapterRecord, r map[string]any) {
		retained, err := json.Marshal(r)
		require.NoError(t, err)
		for key := range r {
			delete(r, key)
		}
		r["version"] = 3
		r["kind"] = "restart"
		r["id"] = "restart"
		r["source"] = jobdb.JobKey{TenantId: "tenant", JobId: "source"}
		r["descriptor"] = []json.RawMessage{retained}
		meta := new(pb.Metadata)
		value := new(pb.MetadataValue)
		value.SetStringValue("[" + string(retained) + "]")
		meta.SetFields(map[string]*pb.MetadataValue{"__execution_prefix_v3": value})
		p.SetMetadata(meta)
	})
	opts.Mode = "rewrite"
	_, err := migration.Run(context.Background(), opts)
	require.NoError(t, err)
	opts.Mode = "verify"
	_, err = migration.Run(context.Background(), opts)
	require.NoError(t, err)
	db, err := sql.Open("sqlite", opts.Database)
	require.NoError(t, err)
	defer db.Close()
	var raw []byte
	require.NoError(t, db.QueryRow("SELECT body FROM jobdb_chapter_chapters WHERE ordinal=1").Scan(&raw))
	var p pb.ChapterRecord
	require.NoError(t, proto.Unmarshal(raw, &p))
	require.Len(t, p.GetReconstruction(), 1)
	require.NotContains(t, p.GetMetadata().GetFields(), "__execution_prefix_v3")
	require.Len(t, p.GetExecution().GetRecord().GetRetained(), 1)
	for _, record := range []*pb.ExecutionRecord{p.GetReconstruction()[0], p.GetExecution().GetRecord().GetRetained()[0]} {
		require.EqualValues(t, 4, record.GetVersion())
		require.Len(t, record.GetOutput().GetArtifacts(), 1)
		require.Equal(t, "stored-name", record.GetOutput().GetArtifacts()[0].GetStoredName())
		require.Contains(t, string(record.GetOutput().GetData()), "9007199254740993")
	}
}
