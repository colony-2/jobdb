-- Produced by the real JobDB 1dd75e9 runtime on 2026-11-20.
-- Schedule-free format 11, append chain/event, delayed job, input and artifact.
BEGIN TRANSACTION;
CREATE TABLE jobdb_cell_catalog(tenant_id TEXT PRIMARY KEY,cursor_key BLOB NOT NULL);
INSERT INTO "jobdb_cell_catalog" VALUES('tenant',X'BEF1BBA21DC4D3AF8CE128A810D629152256A69DC9D2C73113220D13F568B6BA');
CREATE TABLE jobdb_cell_names(tenant_id TEXT NOT NULL,name TEXT NOT NULL,cell_id TEXT NOT NULL,PRIMARY KEY(tenant_id,name),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
INSERT INTO "jobdb_cell_names" VALUES('tenant','work','3MO5Qij47uwc1H73lEc2XbGn3eq');
CREATE TABLE jobdb_cell_requests(tenant_id TEXT NOT NULL,principal TEXT NOT NULL,request_id TEXT NOT NULL,descriptor TEXT NOT NULL,cell_id TEXT NOT NULL,name TEXT NOT NULL,repo TEXT NOT NULL,description TEXT NOT NULL,metadata BLOB NOT NULL,revision TEXT NOT NULL,created_at_ns INTEGER NOT NULL,PRIMARY KEY(tenant_id,principal,request_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
CREATE TABLE jobdb_cells(tenant_id TEXT NOT NULL,cell_id TEXT NOT NULL,name TEXT NOT NULL,repo TEXT NOT NULL,description TEXT NOT NULL,metadata BLOB NOT NULL,state TEXT NOT NULL CHECK(state IN ('ACTIVE','RETIRED')),revision TEXT NOT NULL,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,retired_at_ns INTEGER,PRIMARY KEY(tenant_id,cell_id),UNIQUE(tenant_id,name),UNIQUE(tenant_id,repo));
INSERT INTO "jobdb_cells" VALUES('tenant','3MO5Qij47uwc1H73lEc2XbGn3eq','work','example.test/work','',X'7B7D','ACTIVE','3MO5QcTUlwBxA6grHxlf3eYmrO4',1795208758445098212,1795208758445098212,NULL);
CREATE TABLE jobdb_chapter_artifacts (
	anthology_id TEXT NOT NULL,
	story_id TEXT NOT NULL,
	ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
	position INTEGER NOT NULL CHECK (position >= 0),
	id TEXT NOT NULL,
	name TEXT NOT NULL,
	content_type TEXT NOT NULL,
	size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
	sha256 TEXT NOT NULL,
	inline_data BLOB,
	blob_path TEXT,
	PRIMARY KEY (anthology_id, story_id, ordinal, position),
	FOREIGN KEY (anthology_id, story_id, ordinal)
		REFERENCES jobdb_chapter_chapters(anthology_id, story_id, ordinal)
		ON DELETE CASCADE,
	CHECK (
		(inline_data IS NOT NULL AND blob_path IS NULL)
		OR (inline_data IS NULL AND blob_path IS NOT NULL)
		OR (inline_data IS NULL AND blob_path IS NULL)
	)
);
INSERT INTO "jobdb_chapter_artifacts" VALUES('tenant','saved',0,0,'b8e38091-2040-4809-bfed-700fff0b3f0f','saved.bin','application/octet-stream',23,'1db5634b09f5f61487f761abad47382a0c4542b5eab2c8c597783972c1245578',X'736176656420757073747265616D206172746966616374',NULL);
CREATE TABLE jobdb_chapter_chapters (
	anthology_id TEXT NOT NULL,
	story_id TEXT NOT NULL,
	ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
	body BLOB NOT NULL,
	created_at_ns INTEGER NOT NULL,
	PRIMARY KEY (anthology_id, story_id, ordinal),
	FOREIGN KEY (anthology_id, story_id)
		REFERENCES jobdb_chapter_stories(anthology_id, story_id)
		ON DELETE CASCADE
);
INSERT INTO "jobdb_chapter_chapters" VALUES('tenant','saved',0,X'08001204776F726B1A2E6233613539383032653037613A34343239372D334D4F3551647268736B3641516A4D6541687467335437624B3358220C08B6EC82D80610FECFB3D4013A4064383963306161313466383432653164373237626436613634366661613961626362313935323537656566346365353431646365663661363238616366313566420050015800680082010D0A0B11000000000000F03F200192014035616433383330346235333563323938376462643234363537633161313162383834393834666636303064396633383964656230643465363334666565373932A00102F201110A0F0A0D22736176656420696E70757422',1795208758445684212);
INSERT INTO "jobdb_chapter_chapters" VALUES('tenant','Xt9QGUa7Xy2uAuAAh6ynh9EdfAf',0,X'0800120562617463681A00220C08B6EC82D8061095D4B7D6013A403433353936316464373531336333303234633262373238356464626636366165383463656535356263666430393031323062343935663134396636616334343350005800680082010D0A0B11000000000000F03F2001920100A00102AA01B7067B224170704D65746164617461223A7B7D2C22417070656E6447656E65726174696F6E223A312C22417070656E644B6579223A2273617665642D636861696E222C22417661696C61626C654174223A6E756C6C2C2243656C6C223A7B2263656C6C223A22776F726B222C2263656C6C4964223A22334D4F3551696A3437757763314837336C4563325862476E336571222C2263656C6C5265706F223A226578616D706C652E746573742F776F726B227D2C22436C69656E745061796C6F6164223A6E756C6C2C22436C69656E745061796C6F61645265766973696F6E223A302C22437265617465644174223A22323032362D31312D32305432313A30353A35382E3434393730323432315A222C22446570656E64656E63696573223A6E756C6C2C22457870697265734174223A6E756C6C2C22476F616C4944223A22222C22496E697469616C5061796C6F6164446967657374223A2235616433383330346235333563323938376462643234363537633161313162383834393834666636303064396633383964656230643465363334666565373932222C224A6F624B6579223A7B226A6F624964223A22587439514755613758793275417541416836796E68394564664166222C2274656E616E744964223A2274656E616E74227D2C224A6F6254797065223A226261746368222C22506172656E744A6F624944223A22222C2252756E506F6C696379223A7B22496E766F636174696F6E54696D656F7574223A6E756C6C2C225265747279223A7B224261636B6F6666436F656666696369656E74223A312C22496E697469616C496E74657276616C223A302C224D6178696D756D417474656D707473223A312C224D6178696D756D496E74657276616C223A302C224E6F6E526574727961626C654572726F725479706573223A6E756C6C7D2C22546F74616C54696D656F7574223A6E756C6C7D2C22536368656D6148617368223A22222C2253656C6563746564476F616C4944223A22222C225368617265476F616C223A66616C73652C2257616974466F724A6F62494473223A6E756C6C2C22576F726B65724944223A226A6F6264622D617070656E642D334D4F3551694D436553613754536E67546969574E687442754548227DF201280A100A0E22617070656E6420696E70757422120E0A0C7B226E756D626572223A317D1A046E756C6C',1795208758449873671);
CREATE TABLE jobdb_chapter_stories (
	anthology_id TEXT NOT NULL,
	story_id TEXT NOT NULL,
	created_at_ns INTEGER NOT NULL,
	updated_at_ns INTEGER NOT NULL,
	finalized INTEGER NOT NULL DEFAULT 0 CHECK (finalized IN (0, 1)),
	deleted INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0, 1)),
	deleted_at_ns INTEGER,
	base_anthology_id TEXT,
	base_story_id TEXT,
	base_fork_ordinal INTEGER,
	chapter_count INTEGER NOT NULL DEFAULT 0 CHECK (chapter_count >= 0),
	latest_ordinal INTEGER NOT NULL DEFAULT -1 CHECK (latest_ordinal >= -1),
	CHECK (
		(base_anthology_id IS NULL AND base_story_id IS NULL AND base_fork_ordinal IS NULL)
		OR (base_anthology_id IS NOT NULL AND base_story_id IS NOT NULL AND base_fork_ordinal >= 0)
	),
	PRIMARY KEY (anthology_id, story_id)
);
INSERT INTO "jobdb_chapter_stories" VALUES('tenant','saved',1795208758445684212,1795208758445684212,0,0,NULL,NULL,NULL,NULL,1,0);
INSERT INTO "jobdb_chapter_stories" VALUES('tenant','Xt9QGUa7Xy2uAuAAh6ynh9EdfAf',1795208758449873671,1795208758449873671,0,0,NULL,NULL,NULL,NULL,1,0);
CREATE TABLE jobdb_configuration (tenant_id TEXT PRIMARY KEY, snapshot BLOB NOT NULL);
CREATE TABLE jobdb_dependency_targets(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,target_kind TEXT NOT NULL,target_id TEXT NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id,target_kind,target_id));
CREATE TABLE jobdb_dependency_waits(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id));
CREATE TABLE jobdb_goal_candidates(tenant_id TEXT NOT NULL,goal_id TEXT NOT NULL,job_id TEXT NOT NULL,added_at_ns INTEGER NOT NULL,PRIMARY KEY(tenant_id,goal_id,job_id));
CREATE TABLE jobdb_goal_preparations(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,request_id TEXT,goal_id TEXT NOT NULL,share_goal INTEGER NOT NULL,descriptor BLOB NOT NULL,requested_cell TEXT NOT NULL,requested_repo TEXT NOT NULL,cell_id TEXT NOT NULL,cell_name TEXT NOT NULL,cell_repo TEXT NOT NULL,parent_job_id TEXT NOT NULL,prior_job_id TEXT NOT NULL,dependencies BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id),UNIQUE(tenant_id,request_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
INSERT INTO "jobdb_goal_preparations" VALUES('tenant','saved',NULL,'3MO5QdJ4zrC1lKhdf35unQltUgZ',0,'{"Data":"39e23a730764d2e56306aeeb12205d1176f40661a18ff83e3c65ce2f4a66c7df","Job":{"AvailableAt":"2030-01-02T03:04:05.123456789Z","Cell":"","CellRepo":"","ClientPayloadUpdate":null,"Data":null,"Dependencies":null,"JobID":"","JobType":"work","Metadata":null,"Prerequisites":null,"RunPolicy":{"InvocationTimeout":null,"Retry":{"BackoffCoefficient":0,"InitialInterval":0,"MaximumAttempts":0,"MaximumInterval":0,"NonRetryableErrorTypes":null},"TotalTimeout":null},"Schema":null,"TenantId":"tenant"},"Kind":"submit","Parent":""}','work','example.test/work','3MO5Qij47uwc1H73lEc2XbGn3eq','work','example.test/work','','',X'6E756C6C');
INSERT INTO "jobdb_goal_preparations" VALUES('tenant','Xt9QGUa7Xy2uAuAAh6ynh9EdfAf',NULL,'3MO5QjBRX8JPsfmFsPtZq4aw2cR',0,'{"kind":"start-or-append","id":"Xt9QGUa7Xy2uAuAAh6ynh9EdfAf"}','work','example.test/work','3MO5Qij47uwc1H73lEc2XbGn3eq','work','example.test/work','','',X'6E756C6C');
CREATE TABLE jobdb_goals(tenant_id TEXT NOT NULL,goal_id TEXT NOT NULL,state TEXT NOT NULL DEFAULT '',created_at_ns INTEGER NOT NULL DEFAULT 0,updated_at_ns INTEGER NOT NULL DEFAULT 0,cell_id TEXT NOT NULL,cell_name TEXT NOT NULL,cell_repo TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(tenant_id,goal_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
INSERT INTO "jobdb_goals" VALUES('tenant','3MO5QdJ4zrC1lKhdf35unQltUgZ','OPEN',1795208758447303046,1795208758447303046,'3MO5Qij47uwc1H73lEc2XbGn3eq','work','example.test/work',X'7B22637265617465644174223A22323032362D31312D32305432313A30353A35382E3434373330333034365A222C22676F616C4B6579223A7B2274656E616E744964223A2274656E616E74222C22676F616C4964223A22334D4F3551644A347A7243316C4B6864663335756E516C7455675A227D2C227265766973696F6E223A2231222C227374617465223A224F50454E222C22757064617465644174223A22323032362D31312D32305432313A30353A35382E3434373330333034365A227D');
INSERT INTO "jobdb_goals" VALUES('tenant','3MO5QjBRX8JPsfmFsPtZq4aw2cR','OPEN',1795208758451278587,1795208758451278587,'3MO5Qij47uwc1H73lEc2XbGn3eq','work','example.test/work',X'7B22637265617465644174223A22323032362D31312D32305432313A30353A35382E3435313237383538375A222C22676F616C4B6579223A7B2274656E616E744964223A2274656E616E74222C22676F616C4964223A22334D4F35516A425258384A5073666D467350745A71346177326352227D2C227265766973696F6E223A2231222C227374617465223A224F50454E222C22757064617465644174223A22323032362D31312D32305432313A30353A35382E3435313237383538375A227D');
CREATE TABLE jobdb_jobs (
	tenant_id TEXT NOT NULL,
	job_id TEXT NOT NULL,
 goal_id TEXT NOT NULL,
 cell_id TEXT NOT NULL,
 cell_name TEXT NOT NULL,
 cell_repo TEXT NOT NULL,
 dependency_wait_id TEXT NOT NULL DEFAULT '',
	job_type TEXT NOT NULL,
	route_job_type TEXT NOT NULL,
 route_task_type TEXT NOT NULL DEFAULT '',
	payload BLOB NOT NULL DEFAULT x'',
 client_payload BLOB,
 client_payload_revision INTEGER NOT NULL DEFAULT 0,
 initial_payload_digest TEXT NOT NULL,
	metadata BLOB,
	parent_job_id TEXT,
	wait_for BLOB NOT NULL DEFAULT x'',
	available_at_ns INTEGER NOT NULL,
	wait_deadline_ns INTEGER,
	created_at_ns INTEGER NOT NULL,
	updated_at_ns INTEGER NOT NULL,
	archived_at_ns INTEGER,
	hidden_at_ns INTEGER,
	cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
	completion_status TEXT,
	completion_detail TEXT,
	consecutive_expirations INTEGER NOT NULL DEFAULT 0,
	lease_id TEXT,
	lease_worker_id TEXT,
	lease_expires_at_ns INTEGER,
	alternate_job_type TEXT,
 alternate_task_type TEXT,
	alternate_at_ns INTEGER, append_key TEXT, append_gen INTEGER,
	PRIMARY KEY (tenant_id, job_id),
 FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id)
);
INSERT INTO "jobdb_jobs" VALUES('tenant','saved','3MO5QdJ4zrC1lKhdf35unQltUgZ','3MO5Qij47uwc1H73lEc2XbGn3eq','work','example.test/work','','work','work','',X'0A0D0A0B11000000000000F03F2001',NULL,0,'5ad38304b535c2987dbd24657c1a11b884984ff600d9f389deb0d4e634fee792',NULL,NULL,X'',1893553445123456789,NULL,1795208758447303046,1795208758447303046,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "jobdb_jobs" VALUES('tenant','Xt9QGUa7Xy2uAuAAh6ynh9EdfAf','3MO5QjBRX8JPsfmFsPtZq4aw2cR','3MO5Qij47uwc1H73lEc2XbGn3eq','work','example.test/work','','batch','batch','',X'0A0D0A0B11000000000000F03F2001',X'6E756C6C',1,'74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b',X'7B22617070223A7B7D7D',NULL,X'',1795208758449702421,NULL,1795208758449702421,1795208758451278587,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL,'saved-chain',1);
CREATE TABLE jobdb_lease_events (
 event_id INTEGER PRIMARY KEY AUTOINCREMENT,
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('lease_acquired','lease_renewed')),
 occurred_at_ns INTEGER NOT NULL,
 cell_id TEXT NOT NULL,cell_name TEXT NOT NULL,cell_repo TEXT NOT NULL,
 goal_id TEXT NOT NULL,job_type TEXT NOT NULL,
 route_job_type TEXT NOT NULL,route_task_type TEXT NOT NULL,
 lease_id TEXT NOT NULL,worker_id TEXT NOT NULL,lease_expires_at_ns INTEGER NOT NULL,
 previous_lease_expires_at_ns INTEGER,acquisition_source TEXT,previous_lease_id TEXT,
 CHECK ((event_type='lease_acquired' AND acquisition_source IS NOT NULL AND acquisition_source IN ('poll','targeted','task_completion','append') AND previous_lease_expires_at_ns IS NULL)
 OR (event_type='lease_renewed' AND previous_lease_expires_at_ns IS NOT NULL AND acquisition_source IS NULL AND previous_lease_id IS NULL))
);
CREATE TABLE jobdb_schemas (
	tenant_id TEXT NOT NULL,
	schema_hash TEXT NOT NULL,
	schema_json BLOB NOT NULL,
	state TEXT NOT NULL,
	created_at_ns INTEGER NOT NULL,
	archived_at_ns INTEGER,
	PRIMARY KEY (tenant_id, schema_hash)
);
CREATE TABLE jobdb_submission_receipts (
 tenant_id TEXT NOT NULL, job_id TEXT NOT NULL,
 descriptor BLOB NOT NULL, requested_cell TEXT NOT NULL,
 goal_id TEXT NOT NULL, cell_id TEXT NOT NULL, cell_name TEXT NOT NULL, cell_repo TEXT NOT NULL,
 PRIMARY KEY(tenant_id,job_id)
);
CREATE INDEX jobdb_jobs_poll_idx
	ON jobdb_jobs (archived_at_ns, tenant_id, route_job_type, route_task_type, available_at_ns, created_at_ns);
CREATE INDEX jobdb_jobs_list_idx
	ON jobdb_jobs (tenant_id, created_at_ns DESC, job_id DESC);
CREATE INDEX jobdb_schemas_list_idx
	ON jobdb_schemas (tenant_id, state, created_at_ns DESC, schema_hash ASC);
CREATE INDEX jobdb_jobs_cell_idx ON jobdb_jobs(tenant_id,cell_id,job_id);
CREATE INDEX jobdb_jobs_goal_idx ON jobdb_jobs(tenant_id,goal_id,job_id);
CREATE INDEX jobdb_goal_candidates_job_idx ON jobdb_goal_candidates(tenant_id,job_id,goal_id);
CREATE INDEX jobdb_dependency_targets_lookup ON jobdb_dependency_targets(tenant_id,target_kind,target_id);
CREATE INDEX jobdb_goals_cell_idx ON jobdb_goals(tenant_id,cell_id,state);
CREATE INDEX jobdb_preparations_cell_idx ON jobdb_goal_preparations(tenant_id,cell_id);
CREATE INDEX jobdb_goals_state_idx ON jobdb_goals(tenant_id,state,goal_id);
CREATE INDEX jobdb_goals_created_idx ON jobdb_goals(tenant_id,created_at_ns,goal_id);
CREATE INDEX jobdb_goals_updated_idx ON jobdb_goals(tenant_id,updated_at_ns,goal_id);
CREATE INDEX jobdb_jobs_type_goal_idx ON jobdb_jobs(tenant_id,job_type,goal_id,job_id);
CREATE INDEX jobdb_dependency_targets_discovery_idx ON jobdb_dependency_targets(tenant_id,target_kind,target_id,job_id,wait_id);
CREATE INDEX jobdb_dependency_waits_state_idx ON jobdb_dependency_waits(tenant_id,job_id,state,wait_id);
CREATE UNIQUE INDEX jobdb_jobs_append_idx ON jobdb_jobs(tenant_id,cell_id,job_type,append_key,append_gen) WHERE append_key IS NOT NULL;
CREATE UNIQUE INDEX jobdb_lease_events_acquired_unique ON jobdb_lease_events(tenant_id,job_id,lease_id) WHERE event_type='lease_acquired';
CREATE INDEX jobdb_lease_events_time_idx ON jobdb_lease_events(tenant_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_job_id_idx ON jobdb_lease_events(tenant_id,job_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_cell_id_idx ON jobdb_lease_events(tenant_id,cell_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_goal_id_idx ON jobdb_lease_events(tenant_id,goal_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_worker_id_idx ON jobdb_lease_events(tenant_id,worker_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_lease_id_idx ON jobdb_lease_events(tenant_id,lease_id,occurred_at_ns DESC,event_id DESC);
CREATE INDEX jobdb_lease_events_event_type_idx ON jobdb_lease_events(tenant_id,event_type,occurred_at_ns DESC,event_id DESC);
CREATE INDEX idx_jobdb_chapter_stories_anthology_story
ON jobdb_chapter_stories(anthology_id, story_id);
CREATE INDEX idx_jobdb_chapter_chapters_story_ordinal
ON jobdb_chapter_chapters(anthology_id, story_id, ordinal);
DELETE FROM "sqlite_sequence";
INSERT INTO "sqlite_sequence" VALUES('jobdb_lease_events',0);
COMMIT;
PRAGMA user_version=11;
