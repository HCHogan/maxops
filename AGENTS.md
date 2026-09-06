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
- Version 0.2 is read-only. Future execution follows docs/implementation-plan.md:
  preauthorized management clients, immutable jobs, durable outcomes and recovery.
  Keep maxops independent of any bot's identity, task system or database.
- Treat maxops as one of multiple fleet writers. Observe current remote and runtime
  state; do not assume internal locks cover manual rebuilds or let stale recovery
  overwrite a later external deployment.
- Configure consumers through native services.maxops-agent / services.maxops-hub
  options. Keep secrets in runtime files and use systemd LoadCredential.
