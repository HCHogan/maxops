check:
    devenv shell check

test:
    devenv shell cargo nextest run --workspace --locked

bench:
    devenv shell bench

fmt:
    devenv shell cargo fmt --all

build:
    devenv shell cargo build --workspace --locked

eval:
    nix flake check --all-systems --no-build
