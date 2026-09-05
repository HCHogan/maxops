# Initial architecture

## Ownership

`maxops-proto` owns operation names, request types, capabilities and wire data.
Its small registry macro enumerates operations; it is not a code generation
framework. Schemars supplies parameter schemas for CLI generation and discovery;
utoipa supplies the REST OpenAPI document from those same Rust types.

`maxops-agent` reads Linux/systemd observations. It does not expose a shell,
start/stop/restart endpoints or a polkit grant. Its only subprocess is
`journalctl`, executed with separate arguments, no shell and a bounded reader.

`maxops-hub` authenticates clients, applies host and capability grants, and
queries agents or existing monitoring services. `maxopsctl` is a thin HTTP
client. Inventory, credentials and principals belong to the consuming Nix repo.

## Authentication and limits

Every agent has a token; every client has a different token. Hub startup rejects
duplicate client identities, duplicate client tokens, reused client/agent
credentials, unknown hosts and unknown capabilities. Alert ingress requires a
separate token. Tokens are not accepted in query strings or request parameters.
They are compared in constant time for equal-length inputs and are never logged.

Service names are canonical `.service` names with a deliberately restricted
ASCII syntax. Both the hub and agent validate service scope. New capabilities
are opt-in; there is no `*` grant. Logs need `logs:read` and agent `allowLogs`.
The service uses a dynamic unprivileged user with no capabilities or privilege
escalation. Journal membership remains a broader process-level read privilege
than the API allowlist, and is disabled by default.

Limits: 4 KiB query requests, 256 KiB incoming alert payloads, 2 MiB upstream
JSON, 1 MiB journal output, 1–200 journal entries and a 1–86400 second window.
The journal subprocess has a five-second timeout and is killed on cancellation;
HTTP connects have a two-second timeout and requests eight seconds. Requests
use a shared client with redirects and environment/system proxies disabled.
At most 16 hub handlers and eight agent observations/log reads run concurrently;
fleet queries visit at most eight agents per batch. Health checks are exempt.

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
been loaded is explicitly unknown. `units.list` uses this bounded snapshot.
`units.status` separately reads Service D-Bus properties for PID, memory,
restart count and last main-process exit code/status. Unsupported memory
accounting remains null, not zero. This is not a list of every installed unit.

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

The hub returns success only after the sink returns 2xx. A timeout, connection
failure or non-2xx yields 503 so Alertmanager can retry. A crash after delivery
but before acknowledgement can cause duplicates. There is no durable queue or
exactly-once promise; downstream receivers must tolerate duplicate alerts.
Keep a basic direct Alertmanager receiver independent of the hub. A future
queue/sink abstraction should be justified by delivery needs, not added before
the first real receiver exists.

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

## Before mutations

Do not add mutation routes by reusing query authorization alone. They need
authenticated confirmation outside the LLM, immutable intent parameters,
expiration, one-time consumption, per-agent operation IDs, durable execution
records and reconciliation of unknown outcomes. Polkit must restrict the actual
unit and verb and be tested from the unprivileged agent account. Reboot and
deployment remain separate decisions.
