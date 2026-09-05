# maxops

A small, read-only fleet control plane. Nix owns deployment and inventory;
Prometheus owns metrics; maxops provides authenticated observations and optional
Alertmanager webhook forwarding. No host or user from a private fleet is built in.

This is an initial implementation, not a deployed production service.

## Implemented

- Four Rust crates: `maxops-proto`, `maxops-agent`, `maxops-hub`, `maxopsctl`.
- A shared operation registry generates request decoding, capability names,
  parameter JSON Schemas and CLI subcommands. Utoipa derives OpenAPI from the
  same request types.
- Explicit per-client host and capability grants. Request bodies cannot supply
  an identity. Both hub and agent enforce readable service allowlists.
- Agent: systemd D-Bus status, kernel, uptime, current `/run/current-system`
  target, and optionally bounded journal queries.
- Hub: partial fleet results, optional Prometheus exporter observations and
  host-scoped Alertmanager queries. Unreachable or stale observations remain
  unknown; they are not labelled as a host failure.
- Optional authenticated Alertmanager v4 forwarding to one generic webhook.
  The hub acknowledges only after the destination returns success.
- Native NixOS modules with unprivileged services and systemd credentials.
- Devenv, nextest, Criterion, HTTP integration tests and a NixOS VM test.

Not implemented: MCP, QQ impersonation/delegation, service changes, reboot,
deployment, durable notification storage, custom PromQL, rich resource metrics,
Nix generation numbers or trustworthy activation timestamps. A closure path is
reported as a closure path; filesystem ctime is not called deployment time.

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
maxopsctl units.status --host example --unit nginx.service
maxopsctl units.logs --host example --unit nginx.service --lines 50 --since-seconds 3600
maxopsctl alerts.active
maxopsctl schema                        # local catalog; needs no credentials
```

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
| Agent `POST /v1/logs` | Agent token | Bounded log query with explicit host and unit |

Hub and agent also accept `--config /path/to/config.json` when run outside NixOS.
The Nix modules generate these configurations. `scripts/smoke.py` contains a
minimal standalone example with ephemeral test credentials.

## NixOS integration

```nix
# Consuming flake inputs:
inputs.maxops.url = "github:HCHogan/maxops";
inputs.maxops.inputs.nixpkgs.follows = "nixpkgs";

# Agent host module:
imports = [ inputs.maxops.nixosModules.agent ];
services.maxops-agent = {
  enable = true;
  hostName = "example";
  listenAddress = "100.64.0.10";
  tokenFile = "/run/secrets/maxops-agent";
  readableUnits = [ "nginx.service" ];
  allowLogs = false;
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
    readableUnits = [ "nginx.service" ];
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
