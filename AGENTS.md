# Working on maxops

Read README.md and docs/architecture.md. This is a standalone Rust workspace;
fleet-specific inventory, policy and deployment belong to the consuming repo.

- Use devenv for tooling. Keep flake.lock and devenv.lock nixpkgs pins aligned;
  run scripts/check-pins.py. Do not update nixpkgs incidentally.
- Use color-eyre for application errors and jiff for wall-clock timestamps.
  Use monotonic std/Tokio timers for deadlines.
- Correctness tests run with cargo nextest; Criterion is for benchmarks.
- Run `just check`, and validate affected Nix modules. A Nix eval is not a VM test.
- Operation names, request schemas and capabilities belong in maxops-proto's
  registry. Do not introduce a second dispatch table in a frontend.
- Never log tokens or journal contents, or add an unauthenticated data endpoint.
- Do not add mutations without the confirmation/execution design described in
  docs/architecture.md. This initial version is read-only.
- Configure consumers through native services.maxops-agent / services.maxops-hub
  options. Keep secrets in runtime files and use systemd LoadCredential.
