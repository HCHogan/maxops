# maxops

A small fleet control plane with authenticated observations and opt-in durable
command and systemd service jobs. Nix owns deployment and inventory; Prometheus
owns metrics. No host or user from a private fleet is built in.

Version 0.2 extends the initial single-host pilot with fleet observations.
Fleet inventory and deployment evidence belong to the consuming Nix repository.

The [implementation plan](docs/implementation-plan.md) covers the remaining
configuration, deployment, event and client stages. Every API is usable
by people and arbitrary automation clients. The design supports concurrent manual
and external changes to repositories and hosts.

## Implemented

- Six Rust crates: `maxops-proto`, `maxops-store`, `maxops-executor`,
  `maxops-agent`, `maxops-hub`, `maxopsctl`.
- A shared operation registry generates request decoding, capability names,
  operation kind, idempotency requirement, parameter/response JSON Schemas and
  CLI subcommands. Utoipa derives OpenAPI from the same request types.
- SQLx/SQLite stores durable jobs, idempotency keys, revisioned transitions and
  events with WAL, synchronous writes, migrations and consistent backups.
- `exec.run` submits a bounded asynchronous job to a server-defined execution
  profile. `jobs.list/status/logs/cancel` survive client disconnects and daemon
  restarts. The Hub retries a lost acknowledgement with the same stable job ID.
- The Linux executor launches each command as a transient systemd service, uses
  cgroup-wide cancellation and timeouts, stores bounded binary output, and
  reconciles persistent result records after restart. Diagnostic profiles run as
  an ordinary account with systemd hardening; root profiles require an explicit
  privileged setting.
- `units.start/stop/restart/reload` use systemd's typed D-Bus API for an exact
  Nix-declared unit. The executor records the before/after state and
  InvocationID, serializes its own service changes per host, rejects a stale
  expected InvocationID, and reconciles an accepted action after restart.
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
  The hub acknowledges only after the destination returns success.
- Native NixOS modules with unprivileged services and systemd credentials.
- Devenv, nextest, Criterion, HTTP integration tests and a NixOS VM test.

Not implemented: configuration workspaces, deployment, MCP,
QQ impersonation/delegation, reboot, hub-side durable notification storage,
arbitrary PromQL, or trustworthy activation timestamps. Persistent profile
generation is distinct from the running closure; filesystem ctime is never
called deployment time.

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
maxopsctl exec.run --params-file ./job.json --idempotency-key incident-123 --wait --follow
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

Service names must be explicit canonical `.service` names. Patterns, paths and
shell expressions are rejected. Status covers the intersection of hub and agent
allowlists; unloaded units are `unknown`/`not-loaded`, not automatically healthy.

HTTP endpoints:

| Endpoint | Authentication | Purpose |
| --- | --- | --- |
| `GET /healthz` | None | Process liveness and version; no readiness guarantee |
| `GET /v1/operations` | Client token | Allowed operation catalog |
| `GET /v1/openapi.json` | Client token | OpenAPI for the query API |
| `POST /v1/execute` | Client token | `{"op":"host.facts","params":{"host":"example"}}` |
| `POST /v1/alerts` | Separate ingress token | Forward an Alertmanager v4 webhook |
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
  }];
  clients = [{
    name = "operator";
    tokenFile = "/run/secrets/maxops-operator";
    hosts = [ "example" ];
    capabilities = [ "fleet:read" "host:read" "units:read" ];
  }];
  prometheusUrl = "http://127.0.0.1:9009";
  alertmanagerUrl = "http://127.0.0.1:9093";
};
```

Add a separate client with `access = "manage"` and the `units:manage`, `exec:run`,
`jobs:read` and `jobs:cancel` capabilities for the corresponding job APIs.
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
notifications. In particular:

- Use a protected network such as Tailscale, or a correctly configured TLS
  proxy. An explicit listen address is not a substitute for network ACLs.
- Enabling logs gives the agent process the `systemd-journal` group. Its API
  limits output, but a compromised process could read other journal files.
- Notifications are synchronous and may be delivered more than once. Preserve
  an independent Alertmanager receiver so a stopped hub cannot silence alerts.
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
