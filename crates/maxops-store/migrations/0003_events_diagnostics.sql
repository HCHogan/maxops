CREATE TABLE fleet_event_episodes (
    source TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    host TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    active INTEGER NOT NULL CHECK (active IN (0, 1)),
    last_received_at TEXT NOT NULL,
    PRIMARY KEY (source, fingerprint, host)
);

CREATE TABLE fleet_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT UNIQUE NOT NULL,
    source TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    episode_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN (
        'alert_firing', 'alert_resolved', 'diagnostic_collected',
        'remediation_started', 'remediation_finished'
    )),
    host TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    related_job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    related_change_id TEXT REFERENCES changes(id) ON DELETE SET NULL,
    payload_json TEXT NOT NULL
);

CREATE INDEX fleet_events_host_sequence ON fleet_events(host, sequence);
CREATE INDEX fleet_events_episode_sequence ON fleet_events(episode_id, sequence);

CREATE TABLE event_deliveries (
    subscription_id TEXT NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    event_sequence INTEGER NOT NULL REFERENCES fleet_events(sequence) ON DELETE CASCADE,
    stage TEXT NOT NULL CHECK (stage IN ('queued', 'accepted', 'confirmed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    retry_at TEXT,
    response_status INTEGER,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (subscription_id, event_sequence)
);

CREATE TABLE remediations (
    id TEXT PRIMARY KEY NOT NULL,
    event_id TEXT NOT NULL REFERENCES fleet_events(id) ON DELETE RESTRICT,
    episode_id TEXT NOT NULL,
    host TEXT NOT NULL,
    principal TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    revision INTEGER NOT NULL CHECK (revision > 0),
    state TEXT NOT NULL CHECK (state IN (
        'active', 'succeeded', 'failed', 'no_action', 'superseded'
    )),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    related_job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    related_change_id TEXT REFERENCES changes(id) ON DELETE SET NULL,
    summary TEXT
);

CREATE INDEX remediations_episode_started
    ON remediations(episode_id, started_at DESC);
CREATE UNIQUE INDEX remediations_one_active_episode
    ON remediations(episode_id) WHERE state = 'active';
CREATE UNIQUE INDEX remediations_one_active_host
    ON remediations(host) WHERE state = 'active';
