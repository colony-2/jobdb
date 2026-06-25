package sqlite

const schemaSQL = `
CREATE TABLE IF NOT EXISTS jobdb_chapter_stories (
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

CREATE TABLE IF NOT EXISTS jobdb_chapter_chapters (
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

CREATE TABLE IF NOT EXISTS jobdb_chapter_artifacts (
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

CREATE INDEX IF NOT EXISTS idx_jobdb_chapter_stories_anthology_story
ON jobdb_chapter_stories(anthology_id, story_id);

CREATE INDEX IF NOT EXISTS idx_jobdb_chapter_chapters_story_ordinal
ON jobdb_chapter_chapters(anthology_id, story_id, ordinal);
`
