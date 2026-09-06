# Validation

Validated locally on aarch64-darwin using the devenv toolchain (Rust 1.95.0).

| Check | Result |
| --- | --- |
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| `cargo nextest run --workspace --locked` | 62 passed, none skipped |
| `cargo test --workspace --doc --locked` | Passed; no doctest examples yet |
| `cargo build --workspace --locked` | All workspace targets built |
| `python3 scripts/smoke.py` | Real Hub, CLI, HTTP example and MCP adapter used one scoped catalog and identity |
| Criterion protocol benchmarks | All four benchmarks executed successfully |
| `scripts/check-pins.py <nix-config>/flake.lock` | All three complete nixpkgs lock records match |
| `nix flake check --all-systems --no-build` | Packages and both Linux VM test derivations evaluated |
| `nix build .#checks.x86_64-linux.agent-vm --no-link -L` on b650 | Passed under KVM; the package sandbox ran all 62 tests before the 128-second VM script |

The Linux build initially failed when reqwest's platform verifier found no CA
store inside the Nix sandbox. The package now supplies nixpkgs' CA bundle through
`SSL_CERT_FILE` during checks; TLS verification remains enabled. The complete
Linux package build and tests passed after this change.

The first live Prometheus acceptance check exposed a label mismatch: `up` carries
`__name__`, while `timestamp(up)` drops it. Matching now ignores only that label
and preserves all other labels. A regression test reproduces the real response
shape and includes another series with the same instance but a different job;
it failed before the fix with `unknown` instead of `up`.

The journal check also found that ANSI-coloured daemon messages were represented
as byte arrays by journald. Hub and agent tracing now disable ANSI formatting so
their own service messages remain plain text.

The HTTP tests cover identity/capability/host scope, service filtering, stale and
oversized agent responses, agent identity mismatch, notification failure and
acknowledgement, active-alert scope, durable event replay, diagnostics and
remediation budgets, mixed read-only/execution inventories, Prometheus scrape
freshness, and OpenAPI operation names. The CLI tests check generated parameters
and retry the transient projection conflict that can occur while waiting for a
newly dispatched job.

Criterion covered log decoding, bearer authentication, event serialization and
canonical job-spec hashing. This verifies the harness and provides a preliminary
local baseline, not a fleet performance result. Run `just bench` for the normal
sampling configuration.

## Single-host NixOS pilot

Revision `c484672` passed all 15 nextest tests on macOS and in the Linux Nix
build sandbox. The consuming Nix configuration was then deployed and all six
operations passed acceptance against real systemd, journal, Prometheus and
Alertmanager data. The packaged CLI, credential separation, host/unit scope,
log limits and the live daemon processes' unprivileged state were also checked.

The consumer keeps its inventory, credentials and repeatable acceptance script
in its own repository. No real notification destination is configured.

## Durable command execution

On 2026-09-06, P1 passed the local Rust gate with 40 nextest tests and the full
workspace passed `nix flake check --all-systems --no-build`. The nixpkgs records
in this repository, the consumer lock and devenv lock were identical at
`34268251cf5547d39063f2c5ea9a196246f7f3a6`.

The `checks.x86_64-linux.agent-vm` derivation then ran to completion under KVM
on b650 from an isolated `/tmp/maxops-build` checkout. This build did not alter
the host's NixOS configuration or its running services. The Nix package build
ran all 40 nextest tests again before the VM started.

The VM exercised the real Hub, Agent, Executor, job runner and systemd manager.
It verified:

- observer credentials cannot submit jobs and the agent account cannot restart
  a system service;
- two submissions with one idempotency key return one job;
- a running command survives simultaneous Hub, Agent and Executor restarts and
  retains the exact `firstsecond` output;
- job units run as `maxops-runner` with cgroup kill semantics,
  `NoNewPrivileges=yes` and `ProtectSystem=strict`;
- cancellation stops the transient unit and records `cancelled` with revision
  checking;
- systemd runtime expiry becomes `timed_out`;
- binary output is returned as base64 and reports truncation at the configured
  bound;
- a declared credential is injected through systemd credentials, while an
  undeclared credential produces a durable failed job without exposing a
  credential value.

## Durable systemd service execution

On 2026-09-06, P2 passed the devenv gate with all 44 nextest tests and
`nix flake check --all-systems --no-build`. `scripts/check-pins.py` confirmed
the repository, devenv and nix-config nixpkgs lock records still match at
`34268251cf5547d39063f2c5ea9a196246f7f3a6`.

The final `checks.x86_64-linux.agent-vm` derivation ran to completion under KVM
on b650 from the isolated `/tmp/maxops-build` checkout. The package build ran
the same 44 tests before starting the VM. It did not change b650's NixOS
configuration or any service in its running system.

The VM used real systemd D-Bus calls and verified:

- observation credentials and a merely readable failed unit cannot submit a
  service mutation;
- restart records exact before/after InvocationIDs and completes after the
  executor is restarted while the systemd manager job is still in progress;
- stop and start reach their requested states;
- a successful reload reaches its service handler, a nonzero `ExecReload`
  records `failed` from systemd's `ReloadResult`, and a unit without reload
  support fails without being restarted;
- a manual `systemctl restart` changes the InvocationID and causes a subsequent
  request using the old expected ID to fail as `stale_baseline`, leaving the
  externally changed service running;
- two independent management principals can submit overlapping restarts, while
  the executor's durable host-level manager lock makes the second job observe
  the first job's resulting InvocationID before it acts; and
- each principal can read only its own job record.

The lock serializes maxops jobs only. The external `systemctl restart` fixture
deliberately bypasses it, demonstrating that manual and other fleet writers
remain valid inputs to the next baseline check.

The macOS store cannot represent ncurses' case-distinct terminfo directories
faithfully on its case-insensitive volume. The VM fixture therefore uses the
classic scripted initrd so the closure avoids that damaged local ncurses path;
the actual VM execution and package build ran on Linux.

## Configuration workspaces

On 2026-09-06, P3 passed the devenv gate with all 48 nextest tests and
`nix flake check --all-systems --no-build`. The final
`checks.x86_64-linux.agent-vm` derivation ran under KVM on b650 from the isolated
`/tmp/maxops-build` checkout; it did not activate a b650 configuration or alter
its running services.

The VM used a real bare Git remote, Hub, Agent, Executor and transient check
unit. It verified:

- observation credentials and a management principal without a repository
  grant cannot create a workspace;
- create, status, bounded read, file apply, diff and commit operate on revisioned
  private directories, while traversal and symbolic-link escape return 422;
- a repeated apply against the old revision returns HTTP 409;
- a check already running on revision 2 continues to see revision 2 while an
  apply creates revision 3, and the check process has no credentials directory;
- a human checkout with an uncommitted edit remains unchanged throughout;
- an external clone can advance the remote branch, causing a publish with the
  old expected head to fail as `baseline_changed` without overwriting it; and
- a new workspace based on the re-observed external commit can commit and
  publish by fast-forward, after which the remote ref is checked against the
  exact intended commit.

The VM gate exposed and fixed three integration defects before passing: custom
workspace jobs skipped the protocol's `dispatching` state, bare
`checkout-index` lacked an explicit work tree, and revision directories did not
inherit the check account's dedicated group. CLI calls and polling assertions
also have explicit test timeouts so a nonterminal regression fails the gate
instead of hanging it.

## Guarded Nix deployment

On 2026-09-06, P4 passed the devenv gate with all 50 nextest tests, strict
clippy, and full flake evaluation. The final
`checks.x86_64-linux.agent-vm` derivation ran to completion under KVM on b650
from `/tmp/maxops-build-p4`. The package build repeated the 50 tests before the
VM started; it did not activate b650 or change its running configuration.

The VM built real Nix derivations from immutable workspace revisions and
verified:

- prepare freezes the source commit, remote head, workspace tree, `flake.lock`
  digest, derivation path and directly observed target runtime;
- build realizes the exact prepared derivation and records the resulting store
  path before activation is allowed;
- a slow systemd restart and deployment activation serialize through the same
  target-local maxops lock, while the earlier Nix build remains independent;
- activation changes both the persistent profile and fixture runtime link, and
  target-owned acceptance commands must pass before the explicit verify stage
  records success;
- a later external Git push makes a built plan stale without changing runtime;
- a manual profile switch/rebuild makes a plan stale before activation and is
  left intact;
- failed target acceptance restores the exact baseline closure and reports
  `rolled_back`; and
- a manual activation during maxops acceptance takes ownership, makes the old
  change `superseded`, and prevents its recovery path from rolling back the
  external generation.

The gate exposed and fixed lost immutable workspace revisions after publish,
truncated deployment reports, a missing fixture lock file, target commands that
depended on ambient paths, and rollback checks that incorrectly required a
restored closure to reuse an old Nix profile generation number. The durable
host lock coordinates maxops service and deployment mutations only; direct Git,
`systemctl`, profile and activation operations remain valid external writes.

## Events, diagnostics and remediation coordination

On 2026-09-07, P5 added durable alert episodes, ordered event cursors, staged
webhook delivery, diagnostic jobs and serialized remediation claims with attempt
budgets and cooldowns. Readiness, self-status and metrics expose storage,
delivery and job health without command strings, token material or unbounded
identity labels.

The final KVM VM verified the complete alert-to-remediation path: an Alertmanager
event was persisted and replayed, a diagnostic collected snapshot, unit-log and
explicit probe evidence, one remediation claim restarted a managed service, the
confirmed result emitted delivery events, and a second claim exhausted the
configured episode budget. Authentication, host scope, agent account privilege,
service inventory and webhook acknowledgement boundaries remained enforced.

The first Linux Nix attempt misleadingly reported three missing-table failures.
That flake build had started before the new migration was added to Git, so Nix's
Git source snapshot omitted `0003_events_diagnostics.sql`. Rebuilding from the
complete tracked source included all three migrations and passed. This is why a
successful local workspace test does not prove that an untracked migration is
present in a Git flake build.

## Generic clients and upgrade path

On 2026-09-07, P6 added the stdio MCP adapter and a dependency-free Python HTTP
example. Both fetch the server's principal-scoped operation catalog and use the
same bearer identity and `/v1/execute` protocol as `maxopsctl`; MCP job tools
require an explicit idempotency key. The real smoke test starts Hub and Agent,
then confirms CLI, HTTP and MCP expose the same scoped catalog and return the
same host identity.

The Hub test suite also verifies that an inventory may mix observation-only and
execution-enabled hosts: reads continue on the former, while a mutation fails
closed when that host has no execution channel. The upgrade guide requires a
fresh remote and live-state observation before each change, documents a
consistent SQLite backup, and preserves a read-only fallback by removing the
execution credential rather than assuming maxops owns all fleet writes.

The first full VM run under TCG exposed a fast dispatch/projection race:
`maxopsctl --wait` could terminate on a transient HTTP 409 from `jobs.status`.
The client now retries only that conflict and still surfaces other HTTP errors.
The 62-test workspace gate and strict clippy passed after the fix; the tracked
source archive then passed the Linux package tests and complete KVM VM script
on b650. The VM independently exercised external Git commits, a manual
service restart and an external profile activation, proving stale plans stop and
superseded deployments do not roll back another writer's running generation.
