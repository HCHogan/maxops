# maxops

A small fleet control plane with authenticated observations, durable operations,
versioned configuration workspaces and guarded Nix deployment. Nix owns policy
and inventory; Prometheus owns metrics. No host or user from a private fleet is
built in.

Version 0.3 extends the initial single-host pilot with durable execution,
configuration deployment, event-driven diagnostics and generic clients.
Fleet inventory and deployment evidence belong to the consuming Nix repository.

The [public client contract](docs/api-client-contract.md) defines discovery,
compact results, waiting and fixed deployment workflows.

The [implementation plan](docs/implementation-plan.md) records the completed
delivery stages. Every API is usable by people and arbitrary automation clients.
The design supports concurrent manual and external changes to repositories and
hosts.

## Implemented

- Seven Rust crates: `maxops-proto`, `maxops-store`, `maxops-executor`,
  `maxops-agent`, `maxops-hub`, `maxopsctl`, `maxops-mcp`.
- A shared operation registry generates request decoding, capability names,
  operation kind, idempotency requirement, parameter/response JSON Schemas and
  CLI subcommands. Utoipa derives OpenAPI from the same request types.
- Compact, credential-scoped discovery offers summary/tools/full views,
  revision-bound pages and `resources.list`. Job/change lists are scoped before
  pagination. Safe machine errors retain explicit retry advice.
- SQLx/SQLite stores durable jobs, idempotency keys, revisioned transitions and
  events with WAL, synchronous writes, migrations and consistent backups.
- `exec.run` submits a bounded asynchronous job to a server-defined execution
  profile. `jobs.list/status/logs/cancel` survive client disconnects and daemon
  restarts. `jobs.wait` observes revisions with separate waiter capacity;
  `jobs.events/result` provide bounded replay and evidence reads. The Hub retries a lost acknowledgement with the same stable job ID.
- The Linux executor launches each command as a transient systemd service, uses
  cgroup-wide cancellation and timeouts, stores bounded binary output, and
  reconciles persistent result records after restart. Diagnostic profiles run as
  an ordinary account with systemd hardening; root profiles require an explicit
  privileged setting.
- `units.start/stop/restart/reload` use systemd's typed D-Bus API for an exact
  Nix-declared unit. The executor records the before/after state and
  InvocationID, serializes its own service changes per host, rejects a stale
  expected InvocationID, and reconciles an accepted action after restart.
- `workspace.create/status/read/apply/diff/commit/check/publish` operate on
  Nix-declared repositories in private immutable revision directories. Every
  read or mutation uses revision compare-and-swap; checks run against a frozen
  revision, and publish re-observes the remote ref before a normal fast-forward
  push. Human checkouts are never used or cleaned.
- `deploy.prepare/build/activate/verify/rollback` coordinate a durable change
  across builder and target executors. Plans freeze the workspace revision,
  remote source head, Nix derivation, lock digest and observed runtime baseline.
  Activation and recovery use exact closure/profile ownership checks; a later
  human push or rebuild makes the old operation stale or superseded instead of
  being overwritten.
- `deploy.run` durably drives an existing frozen change to built or verified.
  Workflow ownership and child identities commit atomically; restart reuses
  children, and cancellation stops future stages.
- Deployment and service mutations share a durable per-host lock for maxops
  jobs. Builds remain independent, and the lock never claims to exclude a human
  or another fleet tool.
- Explicit per-client host and capability grants. Request bodies cannot supply
  an identity. Both hub and agent enforce readable service allowlists.
- Agent: systemd D-Bus status, kernel, uptime, current `/run/current-system`
  target, persistent profile/generation, detailed service properties, and
  optionally bounded journal queries.
- `units.list`, `deploy.status`, and host-scoped `host.metrics`; fleet overview
  includes load/filesystem pressure and cautious combined availability states.
- Hub: partial fleet results, optional Prometheus exporter observations and
  host-scoped Alertmanager queries. Unreachable or stale observations remain
  unknown; they are not labelled as a host failure.
- Optional authenticated Alertmanager v4 forwarding to one generic webhook.
  The hub also records host-scoped alerts as durable events with replay cursors
  and episode identity, while preserving the synchronous receiver's response.
- Ordered generic event subscriptions persist delivery intent before HTTP,
  retry after receiver outages and preserve `queued`, `accepted` and
  `confirmed` acknowledgement stages. Receivers deduplicate by event ID.
- `diagnostics.collect` creates a durable evidence bundle from current agent
  facts, bounded logs and fixed Nix-configured probes. Rule conclusions cite
  evidence and missing inputs remain explicit.
- `remediations.begin/finish` associate repairs with an event episode, serialize
  active work per host, and enforce configured attempt and cooldown budgets.
  Max, another bot, a script and a person all use the same API.
- Public readiness plus authenticated status and Prometheus metrics expose
  bounded queue, outcome, duration, recovery, storage and heartbeat state.
- `maxops-mcp` exposes the Hub's principal-scoped operation catalog over MCP
  stdio. It forwards the same bearer identity and HTTP requests as the CLI;
  there is no second authorization or job implementation.
- A Python standard-library HTTP example demonstrates catalog discovery,
  generic operation calls and bounded revision waiting without Max or any bot SDK.
- Native NixOS modules with unprivileged services and systemd credentials.
- Devenv, nextest, Criterion, HTTP integration tests and a NixOS VM test.

Not implemented: QQ impersonation/delegation, reboot, arbitrary PromQL, or
trustworthy activation timestamps. Persistent profile generation is distinct
from the running closure; filesystem ctime is never called deployment time.

## Develop

The development environment is `devenv.nix` + `devenv.yaml` + `devenv.lock`.
The separate `flake.nix` exposes packages and NixOS modules to consumers.
Both locks initially pin nixpkgs to
`34268251cf5547d39063f2c5ea9a196246f7f3a6`, copied from nix-config's root
nixpkgs input. The development URL is pinned to the same revision. Update both
deliberately; `scripts/check-pins.py` detects divergence.

```sh
devenv shell
check                                  # fmt, clippy, nextest, doctests
cargo build --workspace --locked
python3 scripts/smoke.py                # real hub + CLI, synthetic loopback agent
python3 examples/http-client.py --help  # dependency-free generic HTTP client
bench                                  # Criterion, results in target/criterion
python3 scripts/check-pins.py
```

From outside the environment: `just check`, `just test`, `just build`,
`just bench`. `direnv allow` is optional. The CLI works on Linux and macOS;
the real agent requires Linux with systemd.

```sh
nix flake check --all-systems --no-build # evaluation only
nix build                              # build and nextest for this platform
nix build .#checks.x86_64-linux.agent-vm # actual VM test; needs a Linux KVM builder
```

## Query

Create separate random tokens for each principal and agent; store them in files
outside Git and the Nix store. Tokens are 32–512 printable ASCII characters,
with optional trailing newline. The CLI reads a file, never a token argument.

```sh
export MAXOPS_URL=http://127.0.0.1:9721
export MAXOPS_TOKEN_FILE=/run/secrets/maxops-reader
maxopsctl operations
maxopsctl fleet.overview
maxopsctl units.failed
maxopsctl host.facts --host example
maxopsctl host.metrics --host example
maxopsctl deploy.status
maxopsctl units.list --host example
maxopsctl units.status --host example --unit nginx.service
maxopsctl units.logs --host example --unit nginx.service --lines 50 --since-seconds 3600
CURRENT_INVOCATION_ID=$(maxopsctl units.status --host example --unit nginx.service | jq -r .unit.details.invocation_id)
maxopsctl units.restart --host example --unit nginx.service \
  --expected-invocation-id "$CURRENT_INVOCATION_ID" \
  --idempotency-key incident-123-restart --wait
maxopsctl alerts.active
maxopsctl events.list
maxopsctl self.status
maxopsctl diagnostics.collect --params-file ./diagnostic.json \
  --idempotency-key incident-123-diagnostic --wait
maxopsctl remediations.begin --params-file ./remediation.json \
  --idempotency-key incident-123-claim --wait
maxopsctl exec.run --params-file ./job.json --idempotency-key incident-123 --wait --follow
maxopsctl workspace.create --repository nix-config --idempotency-key change-123 --wait
maxopsctl workspace.read --repository nix-config --workspace-id "$WORKSPACE" \
  --expected-revision 1 --path nixos/hosts/example/default.nix
maxopsctl workspace.apply --params-file ./workspace-edit.json
maxopsctl workspace.diff --repository nix-config --workspace-id "$WORKSPACE" \
  --expected-revision 2
maxopsctl workspace.check --repository nix-config --workspace-id "$WORKSPACE" \
  --expected-revision 2 --check flake-check --idempotency-key change-123-check --wait
maxopsctl workspace.commit --repository nix-config --workspace-id "$WORKSPACE" \
  --expected-revision 2 --message 'fix: update example host'
maxopsctl workspace.publish --params-file ./workspace-publish.json \
  --idempotency-key change-123-publish --wait
CHANGE=$(maxopsctl deploy.prepare --repository nix-config --workspace-id "$WORKSPACE" \
  --expected-revision 4 --target-host example --profile example-system \
  --idempotency-key change-123-prepare | jq -r .job_id)
maxopsctl changes.status --change-id "$CHANGE"
REVISION=$(maxopsctl changes.status --change-id "$CHANGE" | jq -r .revision)
maxopsctl deploy.build --change-id "$CHANGE" --expected-revision "$REVISION" \
  --idempotency-key change-123-build --wait
# Re-read the change revision before each later activate/verify/rollback stage.
maxopsctl changes.history --host example
maxopsctl jobs.list
maxopsctl jobs.status --job-id 00000000-0000-0000-0000-000000000000
maxopsctl jobs.logs --job-id 00000000-0000-0000-0000-000000000000
maxopsctl schema                        # local catalog; needs no credentials
```

Nested command input uses a JSON object through `--params-file` or
`--params-stdin`. For example:

```json
{
  "host": "example",
  "profile": "diagnostic",
  "command": { "argv": ["/run/current-system/sw/bin/systemctl", "is-active", "nginx.service"] },
  "timeout_seconds": 30
}
```

`--wait` polls until the job is terminal. `--follow` also streams decoded binary
stdout and stderr. The CLI uses distinct exit codes for failed, unknown,
cancelled and timed-out jobs.

The ordinary HTTP example reads the same environment variables and accepts
either inline parameters or `@path`:

```sh
python3 examples/http-client.py operations
python3 examples/http-client.py call diagnostics.collect @diagnostic.json \
  --idempotency-key incident-123-diagnostic --wait
python3 examples/http-client.py wait 00000000-0000-0000-0000-000000000000
```

For an MCP client, launch `maxops-mcp` as a stdio server with `MAXOPS_URL` and
`MAXOPS_TOKEN_FILE` in its environment. `tools/list` fetches `/v1/operations`
for that credential every time, so an observation token cannot discover or call
management tools. Job tools add a required `_maxops_idempotency_key` input and
translate it to the existing `Idempotency-Key` HTTP header. Tool results include
both text and structured JSON. The adapter follows the
[MCP 2025-06-18 lifecycle](https://modelcontextprotocol.io/specification/2025-06-18/basic/lifecycle),
[stdio transport](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports)
and [tools protocol](https://modelcontextprotocol.io/specification/2025-06-18/server/tools).

Observation accepts exact canonical systemd unit names, including `.service`,
`.timer`, `.target`, `.socket` and `.scope`. Patterns, paths and shell expressions
are rejected. Set native `readAllUnits = true` on both Agent and Hub host entries
for broad observation; defaults remain explicit allowlists. Unloaded units are
`unknown`/`not-loaded`, not automatically healthy. `manageableUnits` stays a
separate exact `.service` list. `units.list` is paged and carries `unit_scope`;
all-loaded coverage is not a list of every installed unit file.

Agent responses negotiate gzip with the shared HTTP client so broad systemd
snapshots remain practical over slower fleet links. The response limit applies
to decoded bytes; clients without gzip support continue receiving plain JSON.

Use `events.recent` for bounded, newest-first incident history with host/unit and
time filters. `events.list` remains an oldest-first durable replay API. Summary
views omit event payloads; `events.get` reads their evidence in bounded JSON
fragments. Discovery requires `host` for `kind=execution_profiles`; profiles
include user, privilege, interpreter, working roots and declared PATH, without
credential values or arbitrary environment variables. Commands run in a non-login
shell and do not inherit an interactive user's PATH. Use `kind=diagnostic_probes`
to list authorized named probes and their argv/profile before collecting evidence.

`diagnostics.collect` reports `collection_status` (`complete`, `partial`, `failed`)
and `missing_evidence`. A failed probe, unavailable logs or truncated probe output
is missing evidence even if the collection job itself completed. `jobs.status`
and `jobs.wait` retain `evidence_status` and `missing_evidence` even when the bundle
is too large to inline; read it through `jobs.result` with `/diagnostic` and page
as needed. `complete` describes evidence collection, not host health. Similarly,
`exec.run` success describes process exit, not achievement of a caller's goal.
Remote `job_id` parameters are UUIDs; a consumer's task number is not a job ID.

HTTP endpoints:

| Endpoint | Authentication | Purpose |
| --- | --- | --- |
| `GET /healthz` | None | Process liveness and version; no readiness guarantee |
| `GET /readyz` | None | Hub/storage readiness; an individual unavailable agent does not fail the Hub |
| `GET /metrics` | Client token with `self:read` | Bounded Prometheus metrics for the permitted scope |
| `GET /v1/operations` | Client token | Allowed operation catalog |
| `GET /v1/openapi.json` | Client token | OpenAPI for the query API |
| `POST /v1/execute` | Client token | `{"op":"host.facts","params":{"host":"example"}}` |
| `POST /v1/alerts` | Separate ingress token | Persist host events and forward the Alertmanager v4 envelope |
| Agent `GET /v1/snapshot` | Agent token | Collect current permitted host observations |
| Agent `POST /v1/unit` | Agent token | Detailed properties for one allowlisted service |
| Agent `POST /v1/logs` | Agent token | Bounded log query with explicit host and unit |
| Agent `POST /v1/manage` | Dedicated execution token | Forward a typed request over the local executor socket |

The executor listens only on a mode `0660` Unix socket shared with the agent.
Observation, execution and client credentials must all be distinct.

Hub and agent also accept `--config /path/to/config.json` when run outside NixOS.
The Nix modules generate these configurations. `scripts/smoke.py` contains a
minimal standalone example with ephemeral test credentials.

## NixOS integration

```nix
# Consuming flake inputs:
inputs.maxops.url = "github:HCHogan/maxops";
inputs.maxops.inputs.nixpkgs.follows = "nixpkgs";

# Managed agent host modules:
imports = [
  inputs.maxops.nixosModules.agent
  inputs.maxops.nixosModules.executor
];
services.maxops-executor = {
  enable = true;
  manageableUnits = [ "nginx.service" ];
  credentialSources.github-token = "/run/secrets/github-token";
  profiles.diagnostic.allowedCredentials = [ "github-token" ];
  profiles.activation = {
    user = "root";
    privileged = true;
  };
  repositories.nix-config = {
    url = "ssh://git@github.com/example/nix-config.git";
    publishRefs = [ "refs/heads/main" ];
    checks.flake-check = [ "${pkgs.nix}/bin/nix" "flake" "check" "--no-build" ];
  };
  deploymentProfiles.example-system = {
    repository = "nix-config";
    targetHost = "example";
    flakeAttribute = "nixosConfigurations.example.config.system.build.toplevel";
    activateProfile = "activation";
    verifyCommands = [ [ "${pkgs.systemd}/bin/systemctl" "is-system-running" "--wait" ] ];
  };
};
services.maxops-agent = {
  enable = true;
  hostName = "example";
  listenAddress = "100.64.0.10";
  tokenFile = "/run/secrets/maxops-agent";
  readableUnits = [ "nginx.service" ];
  manageableUnits = [ "nginx.service" ];
  allowLogs = false;
  execution = {
    enable = true;
    tokenFile = "/run/secrets/maxops-agent-execution";
  };
};

# Hub host module:
imports = [ inputs.maxops.nixosModules.hub ];
services.maxops-hub = {
  enable = true;
  listenAddress = "100.64.0.20";
  hosts = [{
    name = "example";
    agentUrl = "http://100.64.0.10:9720";
    tokenFile = "/run/secrets/example-agent";
    executionTokenFile = "/run/secrets/example-agent-execution";
    readableUnits = [ "nginx.service" ];
    manageableUnits = [ "nginx.service" ];
    diagnosticProfile = "diagnostic";
    diagnosticProbes.service-check = [
      "${pkgs.systemd}/bin/systemctl"
      "is-failed"
      "nginx.service"
    ];
  }];
  clients = [{
    name = "operator";
    tokenFile = "/run/secrets/maxops-operator";
    hosts = [ "example" ];
    capabilities = [ "fleet:read" "host:read" "units:read" "events:read" "self:read" ];
  }];
  repositories = [{ name = "nix-config"; executorHost = "example"; }];
  deployments = [{
    name = "example-system";
    repository = "nix-config";
    builderHost = "example";
    targetHost = "example";
    flakeAttribute = "nixosConfigurations.example.config.system.build.toplevel";
  }];
  prometheusUrl = "http://127.0.0.1:9009";
  alertmanagerUrl = "http://127.0.0.1:9093";
  eventSinks = [{
    id = "automation";
    url = "http://127.0.0.1:8080/events";
    tokenFile = "/run/secrets/maxops-event-sink";
    hosts = [ "example" ];
  }];
};
```

Add a separate client with `access = "manage"` and the `units:manage`, `exec:run`,
`jobs:read`, `jobs:cancel`, `workspace:read`, `workspace:write`,
`workspace:publish`, `deploy:manage`, `changes:read`, `diagnostics:collect` and
`remediations:manage`
capabilities needed by that client. Grant its exact `repositories` and
`deployments` as well.
Observation clients remain read-only. A unit must appear in the Hub, Agent and
Executor `manageableUnits` lists; these are exact names and are separate from
the broader readable inventory. Execution profiles and their users, timeout,
output, process, memory, working-directory and environment limits are declared under
`services.maxops-executor.profiles`.
Credential references are server-side names: the API cannot supply a filesystem
path, and a profile can request only names in its `allowedCredentials` list.

These are separate configuration fragments, not one combined module. Generate
the host list and grants in the consuming repository. No `home-manager` module
or system rebuild is needed just to install `maxopsctl` in a user's home.

## Boundaries and delivery semantics

Read [docs/architecture.md](docs/architecture.md) before enabling logs or
notifications, and [docs/upgrade.md](docs/upgrade.md) before a rolling upgrade.
In particular:

- Use a protected network such as Tailscale, or a correctly configured TLS
  proxy. An explicit listen address is not a substitute for network ACLs.
- Enabling logs gives the agent process the `systemd-journal` group. Its API
  limits output, but a compromised process could read other journal files.
- The legacy notification receiver is synchronous. Durable event subscriptions
  are at least once and ordered per subscription. Preserve an independent
  Alertmanager receiver so a stopped hub cannot silence alerts.
- `alerts.active` includes only alerts whose `labels.instance` exactly matches
  an allowed inventory host. Fleet-wide and unlabelled alerts are omitted.
- Prometheus must expose node-exporter series as `up{job="node",instance="<host>"}`.
  Source errors, stale samples and ambiguous duplicate instances are explicit.
- `host.metrics` requires `metrics:read`. Fixed expressions read CPU idle rates,
  load, memory, filesystem sizes/availability and network byte rates, always
  with an exact host selector. CPU busy fraction is one minus idle rate per CPU.
  Source times are queried separately; stale, future, missing, duplicate and
  non-finite observations are not healthy zeroes. Arbitrary PromQL is disabled.
- Management scope is set by Nix inventory, client capabilities, manageable
  units and executor profiles. maxops does not assume it is the fleet's only
  writer: service operations re-observe the unit immediately before acting;
  later Git and deployment stages re-observe remote and runtime baselines before
  each side effect. Internal job locks cannot exclude a human or another tool.
