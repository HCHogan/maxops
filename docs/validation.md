# Initial validation

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

The HTTP tests cover identity/capability/host scope, service filtering, stale and
oversized agent responses, agent identity mismatch, notification failure and
acknowledgement, active-alert scope, Prometheus scrape freshness, and OpenAPI
operation names. The CLI tests check generated required and numeric parameters.

Criterion was initially run with ten samples, one second warmup and one second
measurement per benchmark. This verifies the harness and provides a preliminary
local baseline, not a fleet performance result. Run `just bench` for the normal
sampling configuration.

The actual Linux/systemd agent and journal permissions still require the NixOS
VM test on a Linux builder with KVM. The VM derivation evaluating successfully
does not establish that those runtime checks pass. Nothing has been deployed
to the fleet or connected to a real notification destination.
