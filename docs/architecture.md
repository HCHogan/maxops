# Architecture

## Ownership

`maxops-proto` owns operation names, request types, capabilities and wire data.
Its small registry macro enumerates operations; it is not a code generation
framework. Schemars supplies parameter schemas for CLI generation and discovery;
utoipa supplies the REST OpenAPI document from those same Rust types.

`maxops-agent` reads Linux/systemd observations. Its observation token cannot
reach the separate management endpoint. An execution token may forward typed
requests over a local Unix socket; the agent itself never receives root or a
polkit grant. Journal reads still use separate arguments, no shell and a bounded
reader.

`maxops-executor` is the privileged local coordinator. It durably accepts a
stable job ID before starting a transient systemd unit. The job runner executes
one structured argv or an explicit-interpreter script as the configured profile
user and writes bounded output plus an atomically renamed completion record.

`maxops-hub` authenticates clients, applies host and capability grants, stores
its view of jobs, and queries agents or existing monitoring services.
`maxopsctl` is a thin HTTP client. `maxops-mcp` is a thin stdio-to-HTTP adapter:
it obtains its tool schemas from the Hub's credential-filtered registry and
uses the same identity and durable operations. Inventory, execution profiles,
credentials and principals belong to the consuming Nix repo.

## Authentication and limits

Every agent has observation and optional execution tokens; every client has a
different token. Hub startup rejects
duplicate client identities, duplicate client tokens, reused client/agent
credentials, unknown hosts and unknown capabilities. Alert ingress requires a
separate token. Tokens are not accepted in query strings or request parameters.
They are compared in constant time for equal-length inputs and are never logged.

Observation uses exact systemd unit names with a restricted ASCII syntax and
canonical hexadecimal escapes. Hub and Agent independently enforce an explicit
list or opt-in `read_all_units` policy. Typed mutations still require exact
`.service` names in independent manageable lists. New capabilities
are opt-in; there is no `*` grant. Logs need `logs:read` and agent `allowLogs`.
The service uses a dynamic unprivileged user with no capabilities or privilege
escalation. Journal membership remains a broader process-level read privilege
than the API allowlist, and is disabled by default.

Limits: 128 KiB ordinary requests, 2 MiB management requests, 256 KiB incoming
alert payloads, 1 MiB workspace edits, 2 MiB upstream JSON, 1 MiB journal output,
1–200 journal entries and a 1–86400 second window.
The journal subprocess has a ten-second timeout and is killed on cancellation;
HTTP connects have a two-second timeout and requests twelve seconds. Requests
use a shared client with redirects and environment/system proxies disabled.
At most 16 hub handlers and eight agent observations/log reads run concurrently;
fleet queries visit at most eight agents per batch. Health checks are exempt.

Cold journal scans can take several seconds even for a small result. The HTTP
budget leaves room for the journal deadline, and consumers must allow more than
twelve seconds for a complete hub request. This does not narrow journal history,
skip service-manager entries, or relax the byte, line or concurrency limits.

## Observations

Jiff supplies UTC observation timestamps. Timeouts and other duration budgets
use Tokio/std monotonic timers. Agent collection failure or clock skew yields
unavailable data, never a healthy zero. Fleet aggregates retain per-host errors.
No attempt is made to distinguish a power failure from a network partition using
only one observation point. A `site` value is metadata, not evidence of cause.

Prometheus source times come from `timestamp(up{job="node"})`, not the instant
query evaluation timestamp. Samples older than 90 seconds or more than 30
seconds ahead are stale. The same tolerance applies to agent observations.

Unit status uses systemd's loaded-unit list. An allowlisted unit that has not
been loaded is explicitly unknown. `units.list` pages this snapshot with state/prefix filters and explicit coverage.
`units.status` separately reads Service D-Bus properties for PID, memory,
restart count and last main-process exit code/status. Unsupported memory
accounting remains null, not zero. Non-service units return common loaded state without requesting Service-specific
properties. Agent responses negotiate gzip; the shared client bounds the decoded body,
so transport compression does not bypass the response-size limit. This is not a list of every installed unit. Old agent snapshots default
to allowlist coverage; Hub policy can narrow but cannot claim broader observation
than the Agent reported.

`host.metrics` uses fixed host-scoped expressions, including separate raw
source timestamp queries for the counters underlying five-minute rates.
Only selected dimension labels are exposed. Duplicate visible series remain
ambiguous even when hidden labels differ. A source returning more than 4096
points or malformed/oversized data is unavailable, never a healthy zero.
Fleet overview combines exporter and agent observations without inferring a
power failure or a network partition. Both unreachable means only unreachable.

Facts and `deploy.status` distinguish `/run/current-system` from the resolved
persistent profile. `profile_generation` comes from a `system-N-link` name and
does not claim to identify a different running closure. `activated_at` remains
null: symlink ctime is not evidence of a successful system activation.

## Notifications

Alert ingress accepts the Alertmanager v4 envelope and forwards it to one generic
webhook. It uses a separate optional sink token and never forwards the ingress
Authorization header. There is no chat-specific behavior and no LLM in delivery.

The original synchronous receiver still returns success only after its sink
returns 2xx. A timeout, connection failure or non-2xx yields 503 so Alertmanager
can retry. In parallel, alerts whose `instance` is an exact inventory host are
written to SQLite before forwarding. An Alertmanager fingerprint plus source
and host identifies an active episode; repeated firing notifications reuse it,
resolution closes it, and a later firing creates a new episode.

Each generic event subscription has a durable cursor. Delivery intent is saved
before HTTP and a receiver outage leaves it queued for retry. HTTP 202 without a
body means accepted, while another successful response means confirmed; an
explicit `queued`, `accepted` or `confirmed` response is retained. Advancing a
cursor and its acknowledgement is one SQLite transaction. Delivery is at least
once, so receivers deduplicate by immutable event ID. Keep a basic direct
Alertmanager receiver independent of the hub.

## Diagnostics and remediation coordination

`diagnostics.collect` is a Hub-owned durable job. It records the current agent
snapshot, bounded unit logs and only named argv probes declared for that host.
Probe commands run through the existing hardened executor profile under stable
child job IDs, so restart recovery queries the same target jobs. Evidence is
marked as fact, hypothesis or missing; rule results carry stable IDs and cite
their evidence. The bundle and a `diagnostic_collected` event retain the parent
episode when collection starts from an event. Each probe must succeed with
complete, untruncated logs to count as collected evidence. The bundle reports
`collection_status` and `missing_evidence`; compact job summaries preserve this
assessment independently of the inline result size. Completing collection does
not assert that the target is healthy. Profiles expose only declared execution
metadata (including PATH), never credentials or arbitrary environment values.

`remediations.begin` atomically claims one attempt for an episode and host.
SQLite permits one active remediation per episode and per host, then applies the
configured attempt count and cooldown. `remediations.finish` uses revision CAS,
can link the verified job or change, and emits a result event. This is
coordination for arbitrary clients; maxops does not embed an LLM or depend on a
bot database.

`/readyz` checks the Hub's durable store while treating individual agents as
independent components. `self.status` and authenticated `/metrics` expose fixed
queue, terminal-duration, reconciliation, delivery, remediation, database,
filesystem and agent/executor heartbeat fields. Metric labels are limited to
configured host names and contain no job IDs, command text or credentials.

## Durable command jobs

The Hub and target executor keep separate records. Submission requires an
idempotency key scoped to the authenticated principal and operation. The Hub
assigns the job ID and the target accepts that same ID transactionally. If an
HTTP acknowledgement is lost, the Hub may resubmit only the identical immutable
specification under the same ID; the target returns its existing job instead of
launching another transient unit.

Transient unit names derive only from validated UUID job IDs. `Type=exec`,
`ExitType=cgroup`, `KillMode=control-group` and `RuntimeMaxSec` make start,
process-tree lifetime, cancellation and timeout systemd-owned. Result and output
files live in a per-job `StateDirectory`. The executor reconciles the result
file, unit state and its database after restart. Missing evidence moves through
`reconciling` to `outcome_unknown`; a later completion record can still resolve
that state.

Profiles cap timeout, output bytes, tasks and optionally memory. They select an
existing account, allowed working roots, an explicit script interpreter and a
non-secret base environment. Diagnostic profiles apply filesystem, device,
kernel and capability hardening. A root profile must be declared privileged.
Credential references resolve through a Nix-declared runtime path map and a
per-profile allowlist. The target passes only requested names through systemd's
`LoadCredential`; API clients cannot choose source paths or read credential
contents through job metadata.

## Durable systemd service jobs

`units.start`, `units.stop`, `units.restart` and `units.reload` share the durable
Hub/executor job protocol. They require a management credential, the
`units:manage` capability and an exact unit present in the Hub, Agent and
Executor `manageable_units` inventories. The Agent forwards only the typed
operation to the executor's Unix socket. The executor calls systemd over D-Bus;
it does not construct a shell command or accept an arbitrary unit path.

Before a call, the executor reads `ActiveState`, `SubState` and `InvocationID`.
An optional expected InvocationID is a compare-before-write guard: a mismatch
fails the job as `stale_baseline` before systemd receives the action. This catches
observable manual restarts between diagnosis and execution without treating
maxops as the only service writer.

The executor serializes its own service jobs with a durable host-level systemd
manager lock. This queue cannot block `systemctl`, another deployment tool or a
human. Each job moves to `running` before making the D-Bus call, then to
`reconciling` after systemd returns its manager job path. An executor restart
never blindly repeats a `running` action. It observes the unit instead; when the
target effect is visible it records that external change cannot be excluded,
and otherwise returns `outcome_unknown`. A `reconciling` job continues to watch
the accepted systemd job and records the before/after state. Unsupported reload
is a failed action and never falls back to restart. The lock remains held for a
short property-settle interval before the final observation and next maxops
service action.

Cancellation is definitive only while an action is still queued or dispatching.
Once the systemd call may have begun, a cancellation request is retained but
does not apply an inverse service action. Deadlines likewise prevent an action
that has not started; an already accepted action proceeds through reconciliation
rather than being reported as a safe timeout.

## Configuration workspaces

Repositories, their URLs, default branch, publishable refs, check commands and
commit identity are Nix-owned executor configuration. Clients name a repository
and a configured check; they cannot supply a Git URL, arbitrary check command or
publish destination. Hub principals need an explicit repository grant in
addition to `workspace:read`, `workspace:write` or `workspace:publish`.

The executor keeps a private bare mirror and one directory per immutable
workspace revision. A workspace ID is the durable create-job ID, and the SQLite
record is the commit point for revision changes. `read`, `apply`, `diff` and
`commit` require the caller's expected revision. Apply creates the next revision
copy, atomically replaces regular UTF-8 files, updates a private Git index, then
advances SQLite by compare-and-swap. An interrupted unpublished directory may be
discarded on retry; a committed revision is never edited in place. Absolute
paths, traversal, `.git`, symbolic-link targets and special files are rejected.

Checks are normal durable command jobs, but their argv and sandbox profile come
from repository configuration and their cwd is the exact requested revision.
The check account can read workspace trees through a dedicated group. It cannot
read the executor's bare mirror or any future publish credential. A later apply
therefore does not change the tree observed by an already running check.

Publish accepts only a configured branch and a committed workspace revision. It
fetches immediately before pushing, compares the observed remote commit with the
caller's expected remote head, verifies ancestry, and uses a normal non-force
push. A human or another tool advancing the branch yields `baseline_changed` and
the external commit remains untouched. The internal fetch/publish locks
coordinate maxops jobs only. If a push may have succeeded but its result cannot
be confirmed, the job is `outcome_unknown`; recovery re-observes the remote and
does not blindly repeat or overwrite it.

## Nix deployment changes

Deployment profiles are declared independently on the Hub and executors. The
Hub routes a named profile to one repository builder and one target; the
executors own the exact flake attribute, activation program, profile links,
verification argv and rollback policy. API callers can select a granted profile
but cannot replace any command or path in it.

`deploy.prepare` creates a durable change whose ID is also the prepare job ID.
It observes the configured remote ref and the target's running closure,
persistent profile, generation and boot ID, then freezes those facts with an
immutable workspace revision and source commit. Prepare evaluates the exact Nix
derivation and records a digest of `flake.lock`. Build realizes that derivation
and records the output path. Every stage has a separate durable job and change
revision, so a caller must re-read the record before proceeding.

Immediately before activation, the Hub re-observes both the remote ref and the
target runtime. Any difference from the plan makes the change `stale`; maxops
does not reset the branch, profile or host. The target also compares the live
runtime under its host mutation lock before invoking the configured activation
program. Service actions, activation, verification and rollback share that lock;
Nix evaluation and builds do not. The lock coordinates maxops jobs only.

Verification checks both profile links and runs the target-owned acceptance
argv. Automatic rollback restores the recorded baseline only while the failed
change still owns both the running and persistent profile links. If a person or
another tool activates a different output during acceptance, the change becomes
`superseded` and recovery leaves that output running. Restoring a baseline can
create a new Nix profile generation, so recovery proves closure identity rather
than requiring the old generation number to reappear.

System and Home Manager deployments are separate profile kinds and closures.
Their paths, flake attributes and activation programs must be declared
separately; a system activation does not imply a home activation.

## Library choices

- `reqwest`: shared async HTTP client, rustls, explicit deadlines and bounded
  response reads. Reuse clients rather than create one per request.
- `color-eyre`: application diagnostics. Public HTTP errors stay small and do
  not expose upstream bodies or exception chains.
- `jiff`: UTC timestamps and future time parsing; elapsed timeouts stay monotonic.
- `utoipa`: OpenAPI derived from typed requests. The initial polymorphic response
  body remains JSON, so the response schema is intentionally broad.
- `itertools`: useful when it materially simplifies collection operations; no
  direct dependency yet. It is different from the `iter_tools` crate.
- `command-run`: convenient synchronous command logging and status checking;
  not used for the agent's asynchronous bounded journal subprocess.
- `cargo-nextest`: correctness tests; Criterion: local microbenchmarks. Benchmark
  figures are not fleet throughput measurements.

References: [reqwest](https://docs.rs/reqwest/latest/reqwest/),
[utoipa](https://docs.rs/utoipa/latest/utoipa/),
[command-run](https://docs.rs/command-run/latest/command_run/),
[Jiff](https://docs.rs/jiff/latest/jiff/),
[systemd D-Bus](https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.systemd1.html).

## Mutation boundary

Version 0.3 includes preauthorized command, service and Nix deployment execution with
observation credentials kept separate. It does not require per-command human
confirmation within the configured scope. Immutable job specifications,
execution-time checks, idempotency, durable records and reconciliation of
unknown outcomes remain required.

maxops is one of several fleet writers. People and other tools may push commits,
rebuild hosts or change services directly. Live observations and remote refs are
distinct from maxops's own operation history; internal locks do not exclude those
writers. Plans must revalidate their baselines, and recovery must stop when a
later external deployment has superseded the operation.

The [implementation plan](implementation-plan.md) records the completed event,
diagnostic and client stages. Reboot and data recovery have separate
implementation and verification requirements. Rolling procedures and the
read-only fallback are in [upgrade.md](upgrade.md).

## Compact client contract and fixed deployment workflow

The public contract is [api-client-contract.md](api-client-contract.md). The Hub
projects the single proto registry into credential-scoped summary/tools/full
catalogs with revision-bound pagination. Input-only discovery avoids sending
response schemas to model clients. `resources.list` discovers permitted policy
names; it does not confer authorization. Job/change list filters apply in SQL
before page limits, and cursors preserve ordering and scope.

`jobs.wait` uses separate bounded waiter admission and committed revision
notifications, with a timed fallback for a reopened store. `jobs.events` replays
existing durable events. `jobs.result` reads bounded JSON fragments; compact
status and decoded logs avoid duplicating large results. Public machine errors
cross CLI/MCP boundaries through an allowlist, without arbitrary upstream bodies.

`deploy.run` is one fixed Hub-owned job over an existing frozen change. Migration
004 adds workflow ownership; job/idempotency insertion and change/stage linkage
share a transaction. Deterministic child IDs survive interruption and restart.
Independent primitives cannot replace an active workflow's children. Cancellation
coordinates the current child and never implies reversal. Unknown workflows can
reconcile evidence through jobs.status, but observation never starts new stages.
The executor retains source/runtime baseline checks and rollback policy.
