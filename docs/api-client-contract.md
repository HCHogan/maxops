# Public API and client contract

Status: implemented locally, 2026-09-07. Written before implementation; local
validation does not imply a production deployment.

## Ownership

maxops is a general fleet control plane. HTTP, CLI, MCP and bot clients share
one registry, identity model and durable job implementation. maxops owns domain
execution, revision checks, recovery and evidence. Clients own presentation and
their own task lifecycle. No operation accepts a bot task ID as authorization.

Max loads fixed skill bundles: loading the maxops skill exposes the complete
currently permitted bundle in one model request. Catalog search must not choose
which tools Max activates. Input schemas come from the registry; response schemas
and implementation metadata need not occupy model context.

## Compatibility and discovery

Keep protocol 2 and existing operation shapes compatible. Extend the authenticated
catalog with explicit `view=summary|tools|full`, category/name/query filters,
bounded limits and a revision-bound cursor. The no-query legacy endpoint retains
its full response. `tools` includes all execution metadata and input schemas, but
omits response schemas. A catalog revision identifies the selected credential's
catalog content; it is not a cached authorization grant.

`resources.list` discovers permitted hosts, services, execution profiles,
repositories and deployments with filters and bounded pagination. Never include
tokens, credential values, commands, environment values or ungranted resources.
Execution-profile discovery comes from the target's declared policy; unreachable
targets remain explicitly unavailable. Resource discovery does not authorize a call.

## Errors and result views

Retain the human-readable `error` field and add a stable machine code and explicit
retry advice. Distinguish authentication/scope rejection, invalid input, unsupported
operation, idempotency conflict, revision conflict, changed baseline, busy,
unavailable storage/target and unknown outcome. Safe public errors cross client
boundaries without forwarding arbitrary upstream bodies or credentials.

Existing full reads remain available. Add explicit compact views for job/change
status and cursor pagination for their lists. Separate a job's status from its
potentially large result; results are bounded and can be addressed by a scoped
job reference and JSON pointer. Log reads retain byte cursors and explicit
complete/truncated markers; clients may request decoded text without receiving
the same output again as base64. Do not silently truncate JSON or schemas.

## Durable observation

`jobs.wait` is a bounded long poll on a job revision. It returns on terminal
outcome, revision change or observation timeout. Waiting has a separate capacity
limit and does not occupy ordinary execution admission. Timeout/disconnect never
cancels the job. Reconnect with the last observed revision.

`jobs.events` exposes the existing durable, job-scoped event sequence with a
cursor. Authorization is the same as job status. Reads can be replayed after
disconnect or Hub restart. `outcome_unknown` ends automatic waiting but remains
reconcilable through later `jobs.status` observation; it is never permission to
resubmit. An unknown deploy.run may reconcile child evidence to a stopped result,
but observation never resumes omitted stages.

CLI and MCP use this observation contract. Job submissions keep returning a
durable handle; clients may offer bounded automatic waiting. Neither a waiting
client nor the transport owns the remote job's lifetime.

## Deployment workflow

Keep `deploy.prepare/build/activate/verify/rollback` as independently callable
operations. Add `deploy.run(change_id, expected_revision, until=built|verified)`
with a stable submission key. It creates a durable Hub-owned workflow job for
one frozen change. The workflow owns only its declared stage progression, not
arbitrary scripts or a user-defined workflow DSL.

Persist ownership and stage identities before dispatch. A resumed workflow reuses
the same child jobs and observes their results. Client disconnect and Hub restart
do not require the model to resubmit stages. Prevent competing workflow/primitive
stage advancement from replacing the workflow's jobs. Do not absorb a concurrent
revision, prepare a new baseline, or retry a write with a new key.

Build, activation, verification and recovery keep the existing source/runtime
checks and target-owned rollback policy. `until=built` finishes after producing
the artifact. `until=verified` requires the declared acceptance checks. Changed
baselines, superseded changes, cancellation and unknown outcomes remain distinct.
Cancellation stops future stages and coordinates an active child without assuming
that an accepted service action or activation was reversed.

## Client behavior

Max's credential stays in its host adapter. A logical invocation determines the
submission key; recovery reuses it. Max routes every job submission through its
existing durable task runtime and waits programmatically, without predicting
latency, model polling or blocking another frontend request. Terminal results and required
decisions return to the frontend for interpretation.

The Max-side design is `docs/adr/010-skill-tool-bundles.md` in HCHogan/max.
Inventory, grants and deployment profiles remain in the consuming Nix repository.

## Acceptance

- Catalog views are bounded, deterministic, revisioned and credential-filtered;
  old full-catalog clients still work. Input schemas remain registry-derived.
- Resource discovery does not reveal out-of-scope resources or secret material.
- Safe error codes survive HTTP/CLI/MCP; write retry advice never permits blind replay.
- Status/result/log projections and list cursors preserve explicit missing and
  truncated evidence, including binary logs and split UTF-8 chunks.
- Waiters release capacity on disconnect and timeout, cannot starve execution,
  and resume with durable state/events after restart.
- deploy.run survives restart, duplicate submission and interrupted stage
  transitions; external changes or competing callers cannot be overwritten.
- Rust format/clippy/nextest, affected module evaluations and HTTP integration
  tests pass. Production deployment is separate from local validation.

## Broad observation and incident queries

Agent and Hub `readAllUnits` is opt-in, independent of `manageableUnits`.
`units.list` returns bounded pages with `unit_scope`, `state` and literal `prefix`
filters. `all_loaded` coverage excludes unobserved installed unit files; an empty
allowlist result is not whole-host health. `resources.list(kind=units)` discovers
loaded names under broad policy and preserves explicit readable/manageable flags.
`kind=execution_profiles` requires a nonempty `host`; the tools schema encodes it.

`events.recent` defaults to the last hour, newest first, with 20 entries (maximum
50), optional exact host/unit filters and `before_sequence` pagination.
`events.list` retains oldest-first replay. `view=summary` removes event payloads
and includes a bounded summary, unit and event ID. `events.get` exposes scoped
JSON fragments with the same pointer/offset bounds as `jobs.result`.

Fixed error codes distinguish `host_not_permitted`, `capability_not_permitted`,
`unit_not_readable`, `unit_not_manageable`, `logs_not_permitted` and
`execution_profile_host_required`. These are non-retryable without correcting
the input or policy; no arbitrary upstream error text needs to be reflected.

## Model-facing observations and submission identity

`POST /v1/execute?view=summary` now projects `alerts.active`, `fleet.overview`
and `host.metrics`. Omitting `view`, or selecting `view=full`, retains the legacy
observations unless the caller explicitly requests pagination or aggregation.

- `alerts.active` accepts an optional exact `host` from `resources.list(kind=hosts)`
  and `limit`/`cursor`. Summary defaults to 20 alerts per page; limits are 1..200
  **before grouping**, so one alertname cannot create an unbounded peer list.
  Host authorization and filtering precede pagination. `total` counts matching
  alerts, while each group's `count` counts peers on that page. Continue with
  `next_cursor`; changed projected data invalidates the cursor.
- Summary groups contain `alertname`, `count` and `peers`. Each peer retains its
  fingerprint and inherits identical host, unit, severity, startsAt and summary
  fields from its group; differing fields stay on the peer. Summary text is
  limited to 200 Unicode characters. Where every peer has the same lossless
  text template, `summary_template` uses `{host}` and `{peer}` substitutions
  instead of repeating host/peer-specific summaries. Peer labels are retained;
  generatorURL, receivers, updatedAt and status are omitted. Full reads without
  pagination preserve every original alert object and its ordering.
- Fleet summary retains each host's availability assessment, agent/exporter
  states, failed-unit count and observation coverage. Its pressure fields are
  load1, memory_used_fraction and worst_filesystem (lowest available fraction),
  with explicit partial/unknown coverage. Filesystem pairs match their complete
  visible labels; stale or ambiguous samples never become healthy zeros.
  The full view retains the existing detailed pressure series.
- `host.metrics` accepts `aggregation=none|stats`, default `none`. `stats`, and
  summary view regardless of aggregation, return min/max/mean over fresh,
  unambiguous samples plus series_count, available_count and counts by source
  state. Missing aggregates are null. Fetch the full view with `aggregation=none`
  to inspect CPU/device labels and source timestamps.

The tools catalog recursively removes `$schema` and schema `title` metadata,
while retaining descriptions, constraints, definitions and references. Full
catalogs retain that metadata. Every parameter property, including nested command
and workspace-edit properties, has a description enforced by a registry test.
Unit mutations encode the exact service-only syntax and length constraints;
clients must validate the advertised schema **before** admitting durable work.
The tools view additionally refines service-mutation unit inputs to an enum of
services in the principal's currently permitted manageable_units (and omits these
operations when the list is empty). This lets enum-aware clients, including Max's current catalog validator, reject non-services before admission even without
pattern support. Host/unit pairing remains enforced at execution.
The CLI also validates typed unit actions before HTTP submission. A canonical
non-service observation unit sent directly to a mutation endpoint returns
`unit_kind_not_manageable`, with retry advice `never`, before a job is created.

`jobs.status`, `jobs.wait`, `jobs.logs` and `jobs.result` accept exactly one of:

```json
{"idempotency_key":"incident-123-diagnostic"}
```

```json
{"job_id":"00000000-0000-0000-0000-000000000000"}
```

The key is the original submission's `Idempotency-Key`, **in the params body**
for reads, not an HTTP header. It is 1..128 printable ASCII characters without
spaces and is scoped to the authenticated principal. The receipt becomes readable
when submission commits; an unknown/uncommitted key returns `not_found`. Lookups
survive Hub restart, still enforce current host grants, and never resubmit a job.
The returned UUID and result shapes are the same as UUID reads. Executor-facing
requests continue using UUIDs only. A consumer can therefore retain its key at
admission and read the corresponding remote job without first learning its UUID.

The consumer's invalid-request hints, deadline-bounded observation loop and
on-demand skill split are tracked separately in HCHogan/max#20. This contract
provides that consumer's idempotency-key lookup prerequisite.
