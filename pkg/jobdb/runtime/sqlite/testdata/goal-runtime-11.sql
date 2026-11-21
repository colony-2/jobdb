-- Generated with the saved JobDB source 5732636605cb6f9e7e30fc4f2c5fd230d43ce9f7.
-- Real public submission, ordinary failure, shared restart, selected recovery,
-- hidden member, legacy failure, receipt, pending dependency, chapters/artifact and configuration.
CREATE TABLE jobdb_cell_catalog(tenant_id TEXT PRIMARY KEY,cursor_key BLOB NOT NULL);
INSERT INTO "jobdb_cell_catalog" VALUES('tenant',X'B698E26B53E3223E84B48DDBE86B9A4AF1221C03DB91C6B76D49117CC0CF6FF9');
CREATE TABLE jobdb_cell_names(tenant_id TEXT NOT NULL,name TEXT NOT NULL,cell_id TEXT NOT NULL,PRIMARY KEY(tenant_id,name),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
INSERT INTO "jobdb_cell_names" VALUES('tenant','work','3M1SwdN9tk3BDSc8wQjP5VXNqT2');
CREATE TABLE jobdb_cell_requests(tenant_id TEXT NOT NULL,principal TEXT NOT NULL,request_id TEXT NOT NULL,descriptor TEXT NOT NULL,cell_id TEXT NOT NULL,name TEXT NOT NULL,repo TEXT NOT NULL,description TEXT NOT NULL,metadata BLOB NOT NULL,revision TEXT NOT NULL,created_at_ns INTEGER NOT NULL,PRIMARY KEY(tenant_id,principal,request_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
CREATE TABLE jobdb_cells(tenant_id TEXT NOT NULL,cell_id TEXT NOT NULL,name TEXT NOT NULL,repo TEXT NOT NULL,description TEXT NOT NULL,metadata BLOB NOT NULL,state TEXT NOT NULL CHECK(state IN ('ACTIVE','RETIRED')),revision TEXT NOT NULL,created_at_ns INTEGER NOT NULL,updated_at_ns INTEGER NOT NULL,retired_at_ns INTEGER,PRIMARY KEY(tenant_id,cell_id),UNIQUE(tenant_id,name),UNIQUE(tenant_id,repo));
INSERT INTO "jobdb_cells" VALUES('tenant','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','',X'7B7D','ACTIVE','3M1Swc37Tf95YMmPinIxynSCHwM',1794516819045408340,1794516819045408340,NULL);
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
INSERT INTO "jobdb_chapter_artifacts" VALUES('tenant','source',0,0,'4539c663-05c3-4224-a447-4a1f01222ed2','saved.bin','application/octet-stream',16,'b534ae76caafad772fdd58cc88785e30165230aed453256823d77fa4387d31d6',X'6C656761637920617274696661637400',NULL);
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
INSERT INTO "jobdb_chapter_chapters" VALUES('tenant','source',0,X'08001204776F726B1A2E3733623235363939643530343A38313739332D334D31537758574D4E693051496E59666476474B456B6373564144220B08D3CED8D70610A480E6163A4038363963396532643531636232346139363830663365363831373739323931643137333437626633666566333036326432346366613039336365323831323064420050015800680082010D0A0B11000000000000F03F200192014035616433383330346235333563323938376462643234363537633161313162383834393834666636303064396633383964656230643465363334666565373932A00102F2010B0A090A0722696E70757422',1794516819048202798);
INSERT INTO "jobdb_chapter_chapters" VALUES('tenant','source',1,X'08011204776F726B1A00220B08D3CED8D70610DBA2BF173A00500058006800920100A00102FA01180A1612140A106F7264696E617279206661696C7572651200',1794516819049455881);
INSERT INTO "jobdb_chapter_chapters" VALUES('tenant','selected',0,X'08001204776F726B1A2E3733623235363939643530343A38313739332D334D31537758574D4E693051496E59666476474B456B6373564144220B08D3CED8D70610AA9DAC183A4037343233346539386166653734393866623564616631663336616332643738616363333339343634663935303730336238633031393839326639383262393062420050015800680082010D0A0B11000000000000F03F200192014035616433383330346235333563323938376462643234363537633161313162383834393834666636303064396633383964656230643465363334666565373932A00102F201080A060A046E756C6C',1794516819051364256);
INSERT INTO "jobdb_chapter_chapters" VALUES('tenant','consumer',0,X'08001204776F726B1A2E3733623235363939643530343A38313739332D334D31537758574D4E693051496E59666476474B456B6373564144220B08D3CED8D70610FFFFEE193A4037343233346539386166653734393866623564616631663336616332643738616363333339343634663935303730336238633031393839326639383262393062420050015800680082010D0A0B11000000000000F03F200192014035616433383330346235333563323938376462643234363537633161313162383834393834666636303064396633383964656230643465363334666565373932A00102F201080A060A046E756C6C',1794516819054304215);
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
INSERT INTO "jobdb_chapter_stories" VALUES('tenant','source',1794516819048202798,1794516819049455881,0,0,NULL,NULL,NULL,NULL,2,1);
INSERT INTO "jobdb_chapter_stories" VALUES('tenant','restart',1794516819050110965,1794516819050110965,0,0,NULL,'tenant','source',0,1,0);
INSERT INTO "jobdb_chapter_stories" VALUES('tenant','selected',1794516819051364256,1794516819051364256,0,0,NULL,NULL,NULL,NULL,1,0);
INSERT INTO "jobdb_chapter_stories" VALUES('tenant','consumer',1794516819054304215,1794516819054304215,0,0,NULL,NULL,NULL,NULL,1,0);
CREATE TABLE jobdb_configuration (tenant_id TEXT PRIMARY KEY, snapshot BLOB NOT NULL);
INSERT INTO "jobdb_configuration" VALUES('tenant',X'7B227265766973696F6E223A226366672D334D31537762513336565177556336633751306D37723652476F46222C2276616C756573223A7B2272657461696E6564223A7B226578616374223A393030373139393235343734303939337D7D7D');
CREATE TABLE jobdb_dependency_targets(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,target_kind TEXT NOT NULL,target_id TEXT NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id,target_kind,target_id));
CREATE TABLE jobdb_dependency_waits(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,wait_id TEXT NOT NULL,state TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id,wait_id));
INSERT INTO "jobdb_dependency_waits" VALUES('tenant','consumer','submission','FAILED',X'7B226A6F624B6579223A7B2274656E616E744964223A2274656E616E74222C226A6F624964223A22636F6E73756D6572227D2C22776169744964223A227375626D697373696F6E222C227374617465223A224641494C4544222C22646570656E64656E63696573223A5B7B22746172676574223A7B22676F616C4964223A22334D3153776167724C6B30725A68653269524E626D627254496B32227D2C22636F6E646974696F6E223A2273756363657373222C227374617465223A224641494C4544222C22626C6F636B6564526561736F6E223A2277616974696E675F666F725F657865637574696F6E222C226661696C757265223A7B22636F6465223A22676F616C5F6661696C6564222C22676F616C4964223A22334D3153776167724C6B30725A68653269524E626D627254496B32222C226D657373616765223A2220206F726967696E616C206661696C7572655C6EE4BF9DE5AD982020222C227465726D696E616C4A6F62496473223A5B2272657374617274222C22736F75726365225D2C226E6F6E5465726D696E616C4A6F62496473223A5B2273656C6563746564225D2C226661696C65644174223A22323032362D31312D31325432303A35333A33392E3035353337343633315A227D7D5D2C226661696C6564446570656E64656E6379496E646578223A302C226661696C757265223A7B22636F6465223A22676F616C5F6661696C6564222C22676F616C4964223A22334D3153776167724C6B30725A68653269524E626D627254496B32222C226D657373616765223A2220206F726967696E616C206661696C7572655C6EE4BF9DE5AD982020222C227465726D696E616C4A6F62496473223A5B2272657374617274222C22736F75726365225D2C226E6F6E5465726D696E616C4A6F62496473223A5B2273656C6563746564225D2C226661696C65644174223A22323032362D31312D31325432303A35333A33392E3035353337343633315A227D7D');
CREATE TABLE jobdb_goal_candidates(tenant_id TEXT NOT NULL,goal_id TEXT NOT NULL,job_id TEXT NOT NULL,added_at_ns INTEGER NOT NULL,PRIMARY KEY(tenant_id,goal_id,job_id));
CREATE TABLE jobdb_goal_preparations(tenant_id TEXT NOT NULL,job_id TEXT NOT NULL,request_id TEXT,goal_id TEXT NOT NULL,share_goal INTEGER NOT NULL,descriptor BLOB NOT NULL,requested_cell TEXT NOT NULL,requested_repo TEXT NOT NULL,cell_id TEXT NOT NULL,cell_name TEXT NOT NULL,cell_repo TEXT NOT NULL,parent_job_id TEXT NOT NULL,prior_job_id TEXT NOT NULL,dependencies BLOB NOT NULL,PRIMARY KEY(tenant_id,job_id),UNIQUE(tenant_id,request_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
INSERT INTO "jobdb_goal_preparations" VALUES('tenant','source',NULL,'3M1SwagrLk0rZhe2iRNbmbrTIk2',0,'{"Data":"8bf86a9b32722aa71862417b0120d516b4d206373f89ab5eae18f9c6d954f59a","Job":{"AvailableAt":null,"Cell":"","CellRepo":"","ClientPayloadUpdate":null,"Data":null,"Dependencies":null,"JobID":"","JobType":"work","Metadata":null,"Prerequisites":null,"RunPolicy":{"InvocationTimeout":null,"Retry":{"BackoffCoefficient":0,"InitialInterval":0,"MaximumAttempts":0,"MaximumInterval":0,"NonRetryableErrorTypes":null},"TotalTimeout":null},"Schema":null,"TenantId":"tenant"},"Kind":"submit","Parent":""}','work','example.test/work','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','','',X'6E756C6C');
INSERT INTO "jobdb_goal_preparations" VALUES('tenant','restart','recover','3M1SwagrLk0rZhe2iRNbmbrTIk2',1,'{"Input":"","Job":{"Cell":"","CellRepo":"","ClientPayloadUpdate":null,"Dependencies":null,"ExecutionRestart":null,"ExtraTaskInput":null,"ExtraTaskOutput":null,"JobID":"","LastStepToKeep":0,"Prerequisites":null,"PriorJobKey":{"jobId":"source","tenantId":"tenant"},"Schema":null,"ShareGoal":true},"Kind":"restart","Output":"","Parent":""}','work','example.test/work','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','','source',X'6E756C6C');
INSERT INTO "jobdb_goal_preparations" VALUES('tenant','selected',NULL,'3M1SwagrLk0rZhe2iRNbmbrTIk2',1,'{"Data":"c6eb66b427007ced9189b98f4b33542babb208e24dae73bdbe8d3c0d4a5c92d1","Job":{"AvailableAt":null,"Cell":"","CellRepo":"","ClientPayloadUpdate":null,"Data":null,"Dependencies":null,"GoalID":"3M1SwagrLk0rZhe2iRNbmbrTIk2","JobID":"","JobType":"work","Metadata":{},"Prerequisites":null,"RunPolicy":{"InvocationTimeout":null,"Retry":{"BackoffCoefficient":1,"InitialInterval":0,"MaximumAttempts":1,"MaximumInterval":0,"NonRetryableErrorTypes":null},"TotalTimeout":null},"Schema":null,"TenantId":"tenant"},"Kind":"submit","Parent":""}','work','example.test/work','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','','',X'6E756C6C');
INSERT INTO "jobdb_goal_preparations" VALUES('tenant','consumer',NULL,'3M1SwaanCpG16cYaThxSmaCvkI1',0,'{"Data":"c6eb66b427007ced9189b98f4b33542babb208e24dae73bdbe8d3c0d4a5c92d1","Job":{"AvailableAt":null,"Cell":"","CellRepo":"","ClientPayloadUpdate":null,"Data":null,"Dependencies":[{"condition":"success","target":{"goalId":"3M1SwagrLk0rZhe2iRNbmbrTIk2"}}],"JobID":"","JobType":"work","Metadata":null,"Prerequisites":null,"RunPolicy":{"InvocationTimeout":null,"Retry":{"BackoffCoefficient":0,"InitialInterval":0,"MaximumAttempts":0,"MaximumInterval":0,"NonRetryableErrorTypes":null},"TotalTimeout":null},"Schema":null,"TenantId":"tenant"},"Kind":"submit","Parent":""}','work','example.test/work','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','','',X'5B7B22746172676574223A7B22676F616C4964223A22334D3153776167724C6B30725A68653269524E626D627254496B32227D2C22636F6E646974696F6E223A2273756363657373227D5D');
CREATE TABLE jobdb_goals(tenant_id TEXT NOT NULL,goal_id TEXT NOT NULL,state TEXT NOT NULL DEFAULT '',created_at_ns INTEGER NOT NULL DEFAULT 0,updated_at_ns INTEGER NOT NULL DEFAULT 0,cell_id TEXT NOT NULL,cell_name TEXT NOT NULL,cell_repo TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(tenant_id,goal_id),FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id));
INSERT INTO "jobdb_goals" VALUES('tenant','3M1SwagrLk0rZhe2iRNbmbrTIk2','FAILED',1794516819048398048,1794516819055374631,'3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work',X'7B22636C6F7365644174223A22323032362D31312D31325432303A35333A33392E3035353337343633315A222C22637265617465644174223A22323032362D31312D31325432303A35333A33392E3034383339383034385A222C226661696C757265223A7B22636F6465223A22676F616C5F6661696C6564222C22676F616C4964223A22334D3153776167724C6B30725A68653269524E626D627254496B32222C226D657373616765223A2220206F726967696E616C206661696C7572655C6EE4BF9DE5AD982020222C227465726D696E616C4A6F62496473223A5B2272657374617274222C22736F75726365225D2C226E6F6E5465726D696E616C4A6F62496473223A5B2273656C6563746564225D2C226661696C65644174223A22323032362D31312D31325432303A35333A33392E3035353337343633315A227D2C22676F616C4B6579223A7B2274656E616E744964223A2274656E616E74222C22676F616C4964223A22334D3153776167724C6B30725A68653269524E626D627254496B32227D2C227265766973696F6E223A2232222C227374617465223A224641494C4544222C22757064617465644174223A22323032362D31312D31325432303A35333A33392E3035353337343633315A227D');
INSERT INTO "jobdb_goals" VALUES('tenant','3M1SwaanCpG16cYaThxSmaCvkI1','OPEN',1794516819054394173,1794516819054394173,'3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work',X'7B22637265617465644174223A22323032362D31312D31325432303A35333A33392E3035343339343137335A222C22676F616C4B6579223A7B2274656E616E744964223A2274656E616E74222C22676F616C4964223A22334D31537761616E4370473136635961546878536D6143766B4931227D2C227265766973696F6E223A2231222C227374617465223A224F50454E222C22757064617465644174223A22323032362D31312D31325432303A35333A33392E3035343339343137335A227D');
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
	alternate_at_ns INTEGER,
	PRIMARY KEY (tenant_id, job_id),
 FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id)
);
INSERT INTO "jobdb_jobs" VALUES('tenant','source','3M1SwagrLk0rZhe2iRNbmbrTIk2','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','','work','work','',X'0A0D0A0B11000000000000F03F2001',NULL,0,'5ad38304b535c2987dbd24657c1a11b884984ff600d9f389deb0d4e634fee792',NULL,NULL,X'',1794516819048398048,NULL,1794516819048398048,1794516819049545131,1794516819049545131,NULL,0,'failed_app',NULL,0,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "jobdb_jobs" VALUES('tenant','restart','3M1SwagrLk0rZhe2iRNbmbrTIk2','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','','work','work','',X'0A0D0A0B11000000000000F03F2001',NULL,0,'5ad38304b535c2987dbd24657c1a11b884984ff600d9f389deb0d4e634fee792',NULL,NULL,X'',1794516819050157590,NULL,1794516819050157590,1794516819050482673,1794516819050482673,NULL,1,'cancelled','',0,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "jobdb_jobs" VALUES('tenant','selected','3M1SwagrLk0rZhe2iRNbmbrTIk2','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','','work','work','',X'0A0D0A0B11000000000000F03F2001',NULL,0,'5ad38304b535c2987dbd24657c1a11b884984ff600d9f389deb0d4e634fee792',X'7B22617070223A7B7D7D',NULL,X'',1794516819051754006,NULL,1794516819051754006,1794516819055674923,1794516819055674923,1794516819055674923,1,'cancelled','job deleted',0,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT INTO "jobdb_jobs" VALUES('tenant','consumer','3M1SwaanCpG16cYaThxSmaCvkI1','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','submission','work','work','',X'0A0D0A0B11000000000000F03F2001',NULL,0,'5ad38304b535c2987dbd24657c1a11b884984ff600d9f389deb0d4e634fee792',NULL,NULL,X'',1794516819054394173,NULL,1794516819054394173,1794516819054394173,NULL,NULL,0,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL);
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
 CHECK ((event_type='lease_acquired' AND acquisition_source IS NOT NULL AND acquisition_source IN ('poll','targeted','task_completion') AND previous_lease_expires_at_ns IS NULL)
 OR (event_type='lease_renewed' AND previous_lease_expires_at_ns IS NOT NULL AND acquisition_source IS NULL AND previous_lease_id IS NULL))
);
INSERT INTO "jobdb_lease_events" VALUES(1,'tenant','source','lease_acquired',1794516819049132340,'3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work','3M1SwagrLk0rZhe2iRNbmbrTIk2','work','work','','3M1SwZa2St8f5Lx5uIXVPfz62kf','worker',1794516879049132340,NULL,'targeted',NULL);
CREATE TABLE jobdb_schedules (
 pause_reason TEXT NOT NULL DEFAULT '',
	tenant_id TEXT NOT NULL,
	schedule_id TEXT NOT NULL,
	state TEXT NOT NULL,
	generation INTEGER NOT NULL,
	spec_hash TEXT NOT NULL,
	trigger_json BLOB NOT NULL,
	target_json BLOB NOT NULL,
 cell_id TEXT NOT NULL,
 cell_name TEXT NOT NULL,
 cell_repo TEXT NOT NULL,
	target_job_type TEXT NOT NULL,
	overlap_policy TEXT NOT NULL,
	failure_policy_json BLOB NOT NULL,
	next_fire_at_ns INTEGER,
	next_job_id TEXT,
	created_at_ns INTEGER NOT NULL,
	updated_at_ns INTEGER NOT NULL,
 FOREIGN KEY(tenant_id,cell_id) REFERENCES jobdb_cells(tenant_id,cell_id),
	PRIMARY KEY (tenant_id, schedule_id)
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
INSERT INTO "jobdb_submission_receipts" VALUES('tenant','selected','{"Data":"c6eb66b427007ced9189b98f4b33542babb208e24dae73bdbe8d3c0d4a5c92d1","Job":{"AvailableAt":null,"Cell":"","CellRepo":"","ClientPayloadUpdate":null,"Data":null,"Dependencies":null,"GoalID":"3M1SwagrLk0rZhe2iRNbmbrTIk2","JobID":"","JobType":"work","Metadata":{},"Prerequisites":null,"RunPolicy":{"InvocationTimeout":null,"Retry":{"BackoffCoefficient":1,"InitialInterval":0,"MaximumAttempts":1,"MaximumInterval":0,"NonRetryableErrorTypes":null},"TotalTimeout":null},"Schema":null,"TenantId":"tenant"},"Kind":"submit","Parent":""}','work','3M1SwagrLk0rZhe2iRNbmbrTIk2','3M1SwdN9tk3BDSc8wQjP5VXNqT2','work','example.test/work');
CREATE INDEX jobdb_jobs_poll_idx
	ON jobdb_jobs (archived_at_ns, tenant_id, route_job_type, route_task_type, available_at_ns, created_at_ns);
CREATE INDEX jobdb_jobs_list_idx
	ON jobdb_jobs (tenant_id, created_at_ns DESC, job_id DESC);
CREATE INDEX jobdb_schedules_list_idx
	ON jobdb_schedules (tenant_id, state, updated_at_ns DESC, schedule_id DESC);
CREATE INDEX jobdb_schemas_list_idx
	ON jobdb_schemas (tenant_id, state, created_at_ns DESC, schema_hash ASC);
CREATE INDEX jobdb_jobs_cell_idx ON jobdb_jobs(tenant_id,cell_id,job_id);
CREATE INDEX jobdb_jobs_goal_idx ON jobdb_jobs(tenant_id,goal_id,job_id);
CREATE INDEX jobdb_goal_candidates_job_idx ON jobdb_goal_candidates(tenant_id,job_id,goal_id);
CREATE INDEX jobdb_dependency_targets_lookup ON jobdb_dependency_targets(tenant_id,target_kind,target_id);
CREATE INDEX jobdb_goals_cell_idx ON jobdb_goals(tenant_id,cell_id,state);
CREATE INDEX jobdb_preparations_cell_idx ON jobdb_goal_preparations(tenant_id,cell_id);
CREATE INDEX jobdb_schedules_cell_idx ON jobdb_schedules(tenant_id,cell_id,state);
CREATE INDEX jobdb_goals_state_idx ON jobdb_goals(tenant_id,state,goal_id);
CREATE INDEX jobdb_goals_created_idx ON jobdb_goals(tenant_id,created_at_ns,goal_id);
CREATE INDEX jobdb_goals_updated_idx ON jobdb_goals(tenant_id,updated_at_ns,goal_id);
CREATE INDEX jobdb_jobs_type_goal_idx ON jobdb_jobs(tenant_id,job_type,goal_id,job_id);
CREATE INDEX jobdb_dependency_targets_discovery_idx ON jobdb_dependency_targets(tenant_id,target_kind,target_id,job_id,wait_id);
CREATE INDEX jobdb_dependency_waits_state_idx ON jobdb_dependency_waits(tenant_id,job_id,state,wait_id);
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
INSERT INTO "sqlite_sequence" VALUES('jobdb_lease_events',1);
PRAGMA user_version=11;
