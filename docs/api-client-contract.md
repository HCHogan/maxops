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
