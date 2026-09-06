# Validation

Validated locally on aarch64-darwin using the devenv toolchain (Rust 1.95.0).

| Check | Result |
| --- | --- |
| `cargo fmt --all --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| `cargo nextest run --workspace --locked` | 14 passed, none skipped |
| `cargo test --workspace --doc --locked` | Passed; no doctest examples yet |
| `cargo build --workspace --locked` | All three binaries built |
| `python3 scripts/smoke.py` | Real hub and CLI passed against a synthetic loopback agent |
| Criterion protocol benchmarks | Both benchmarks executed successfully |
| `scripts/check-pins.py <nix-config>/flake.lock` | All three complete nixpkgs lock records match |
| `nix flake check --all-systems --no-build` | Packages and both Linux VM test derivations evaluated |
| `nix build .#packages.aarch64-darwin.default --no-link` | Passed, including all 14 nextest tests in the Nix build sandbox |
| Smoke test with Nix-packaged binaries | Passed against the synthetic loopback agent |
| `nix build .#packages.x86_64-linux.default --no-link` on a Linux host | Passed, including all 14 nextest tests in the Nix build sandbox |

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

The macOS store cannot represent ncurses' case-distinct terminfo directories
faithfully on its case-insensitive volume. The VM fixture therefore uses the
classic scripted initrd so the closure avoids that damaged local ncurses path;
the actual VM execution and package build ran on Linux.
