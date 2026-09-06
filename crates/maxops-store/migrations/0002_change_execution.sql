ALTER TABLE changes ADD COLUMN creator TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE changes ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;
ALTER TABLE changes ADD COLUMN jobs_json TEXT NOT NULL DEFAULT '{}';

CREATE INDEX changes_creator_created ON changes(creator, created_at DESC);
CREATE INDEX changes_host_created ON changes(host, created_at DESC);
