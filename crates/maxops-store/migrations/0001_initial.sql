PRAGMA foreign_keys = ON;

CREATE TABLE jobs (
    id TEXT PRIMARY KEY NOT NULL,
    principal TEXT NOT NULL,
    host TEXT NOT NULL,
    operation TEXT NOT NULL,
    spec_version INTEGER NOT NULL CHECK (spec_version > 0),
    spec_json TEXT NOT NULL,
    spec_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'queued', 'dispatching', 'running', 'reconciling', 'succeeded',
        'failed', 'cancelled', 'timed_out', 'outcome_unknown'
    )),
    revision INTEGER NOT NULL CHECK (revision > 0),
    policy_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    deadline TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    result_json TEXT
);

CREATE INDEX jobs_principal_created ON jobs(principal, created_at DESC);
CREATE INDEX jobs_host_state ON jobs(host, state);

CREATE TABLE idempotency (
    principal TEXT NOT NULL,
    key TEXT NOT NULL,
    spec_hash TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (principal, key)
);

CREATE TABLE job_attempts (
    operation_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    executor TEXT NOT NULL,
    boot_id TEXT,
    systemd_unit TEXT,
    invocation_id TEXT,
    dispatch_state TEXT NOT NULL,
    exit_code INTEGER,
    signal INTEGER,
    completion_hash TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE job_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE INDEX job_events_job_sequence ON job_events(job_id, sequence);

CREATE TABLE resources (
    resource_kind TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    owner_job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    revision INTEGER NOT NULL,
    acquired_at TEXT NOT NULL,
    PRIMARY KEY (resource_kind, resource_key)
);

CREATE TABLE workspaces (
    id TEXT PRIMARY KEY NOT NULL,
    repository_id TEXT NOT NULL,
    executor TEXT NOT NULL,
    base_commit TEXT NOT NULL,
    revision INTEGER NOT NULL,
    tree_hash TEXT,
    commit_hash TEXT,
    state TEXT NOT NULL,
    creator TEXT NOT NULL,
    created_at TEXT NOT NULL,
    retain_until TEXT
);

CREATE TABLE artifacts (
    digest TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    executor TEXT NOT NULL,
    source_commit TEXT,
    lock_hash TEXT,
    drv_path TEXT,
    out_path TEXT,
    access_scope TEXT NOT NULL,
    created_at TEXT NOT NULL,
    retain_until TEXT
);

CREATE TABLE changes (
    id TEXT PRIMARY KEY NOT NULL,
    workspace_id TEXT REFERENCES workspaces(id) ON DELETE RESTRICT,
    workspace_revision INTEGER,
    host TEXT NOT NULL,
    profile TEXT NOT NULL,
    source_json TEXT NOT NULL,
    artifact_json TEXT,
    baseline_json TEXT NOT NULL,
    intent_json TEXT NOT NULL,
    state TEXT NOT NULL,
    recovery_state TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE observations (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    resource_kind TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    value_json TEXT NOT NULL,
    evidence TEXT NOT NULL,
    related_change_id TEXT REFERENCES changes(id) ON DELETE SET NULL
);

CREATE INDEX observations_resource_time
    ON observations(resource_kind, resource_key, sequence DESC);

CREATE TABLE subscriptions (
    id TEXT PRIMARY KEY NOT NULL,
    principal TEXT NOT NULL,
    filter_json TEXT NOT NULL,
    target_json TEXT NOT NULL,
    credential_ref TEXT,
    cursor INTEGER NOT NULL DEFAULT 0,
    retry_at TEXT,
    acknowledged_at TEXT,
    created_at TEXT NOT NULL
);
