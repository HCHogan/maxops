# Validation

Validated locally on aarch64-darwin using the devenv toolchain (Rust 1.95.0).

| Check | Result |
| --- | --- |
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| `cargo nextest run --workspace --locked` | 48 passed, none skipped |
| `cargo test --workspace --doc --locked` | Passed; no doctest examples yet |
| `cargo build --workspace --locked` | All workspace targets built |
| `python3 scripts/smoke.py` | Real hub and CLI passed against a synthetic loopback agent |
| Criterion protocol benchmarks | Both benchmarks executed successfully |
| `scripts/check-pins.py <nix-config>/flake.lock` | All three complete nixpkgs lock records match |
| `nix flake check --all-systems --no-build` | Packages and both Linux VM test derivations evaluated |
| `nix build .#packages.aarch64-darwin.default --no-link` | Passed, including all 48 nextest tests in the Nix build sandbox |
| Smoke test with Nix-packaged binaries | Passed against the synthetic loopback agent |
| `nix build .#packages.x86_64-linux.default --no-link` on a Linux host | Passed, including all 48 nextest tests in the Nix build sandbox |

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
acknowledgement, active-alert scope, Prometheus scrape freshness, and OpenAPI
operation names. The CLI tests check generated required and numeric parameters.

Criterion was initially run with ten samples, one second warmup and one second
measurement per benchmark. This verifies the harness and provides a preliminary
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
