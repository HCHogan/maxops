{ pkgs, ... }: {
  packages = with pkgs; [
    rustc
    cargo
    rustfmt
    clippy
    rust-analyzer
    cargo-nextest
    just
    pkg-config
    openssl
    cmake
    nixfmt
    python3
    git
  ];

  env.RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";

  scripts.check.exec = ''
    set -eu
    cargo fmt --all --check
    cargo clippy --workspace --all-targets --locked -- -D warnings
    cargo nextest run --workspace --locked
    cargo test --workspace --doc --locked
  '';

  scripts.bench.exec = ''
    cargo bench -p maxops-proto --bench protocol --locked
    cargo bench -p maxops-store --bench store --locked
  '';
}
