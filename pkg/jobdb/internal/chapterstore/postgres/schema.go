package postgres

const schemaSQL = `
CREATE TABLE IF NOT EXISTS jobdb_chapter_stories (
	anthology_id text NOT NULL,
	story_id text NOT NULL,
	created_at_ns bigint NOT NULL,
	updated_at_ns bigint NOT NULL,
	finalized boolean NOT NULL DEFAULT false,
	deleted boolean NOT NULL DEFAULT false,
	deleted_at_ns bigint,
	base_anthology_id text,
	base_story_id text,
	base_fork_ordinal bigint,
	chapter_count bigint NOT NULL DEFAULT 0 CHECK (chapter_count >= 0),
	latest_ordinal bigint NOT NULL DEFAULT -1 CHECK (latest_ordinal >= -1),
	CHECK (
		(base_anthology_id IS NULL AND base_story_id IS NULL AND base_fork_ordinal IS NULL)
		OR (base_anthology_id IS NOT NULL AND base_story_id IS NOT NULL AND base_fork_ordinal >= 0)
	),
	PRIMARY KEY (anthology_id, story_id)
);

CREATE TABLE IF NOT EXISTS jobdb_chapter_chapters (
	anthology_id text NOT NULL,
	story_id text NOT NULL,
	ordinal bigint NOT NULL CHECK (ordinal >= 0),
	body bytea NOT NULL,
	created_at_ns bigint NOT NULL,
	PRIMARY KEY (anthology_id, story_id, ordinal),
	FOREIGN KEY (anthology_id, story_id)
		REFERENCES jobdb_chapter_stories(anthology_id, story_id)
		ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS jobdb_chapter_artifacts (
	anthology_id text NOT NULL,
	story_id text NOT NULL,
	ordinal bigint NOT NULL CHECK (ordinal >= 0),
	position integer NOT NULL CHECK (position >= 0),
	id text NOT NULL,
	name text NOT NULL,
	content_type text NOT NULL,
	size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
	sha256 text NOT NULL,
	inline_data bytea,
	blob_path text,
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

CREATE INDEX IF NOT EXISTS idx_jobdb_chapter_stories_anthology_story
ON jobdb_chapter_stories(anthology_id, story_id);

CREATE INDEX IF NOT EXISTS idx_jobdb_chapter_chapters_story_ordinal
ON jobdb_chapter_chapters(anthology_id, story_id, ordinal);
`
