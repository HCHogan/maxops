{
  description = "Read-only fleet operations with explicit identity and host scope";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      eachSystem = nixpkgs.lib.genAttrs systems;
      packageFor =
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        pkgs.rustPlatform.buildRustPackage {
          pname = "maxops";
          version = "0.1.0";
          src = nixpkgs.lib.fileset.toSource {
            root = ./.;
            fileset = nixpkgs.lib.fileset.unions [
              ./Cargo.toml
              ./Cargo.lock
              ./LICENSE
              ./crates
              ./.config/nextest.toml
            ];
          };
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = [
            pkgs.pkg-config
            pkgs.cmake
          ];
          buildInputs = [ pkgs.openssl ];
          useNextest = true;
          __darwinAllowLocalNetworking = true;
          meta = {
            description = "Read-only fleet hub, Linux agent and CLI";
            license = nixpkgs.lib.licenses.mit;
            platforms = systems;
            mainProgram = "maxopsctl";
          };
        };
    in
    {
      packages = eachSystem (
        system:
        let
          package = packageFor system;
        in
        {
          default = package;
          maxops = package;
        }
      );
      checks = eachSystem (
        system:
        {
          package = self.packages.${system}.default;
        }
        // nixpkgs.lib.optionalAttrs (nixpkgs.lib.hasSuffix "-linux" system) {
          agent-vm = import ./nix/tests/agent.nix {
            pkgs = nixpkgs.legacyPackages.${system};
            inherit self;
          };
        }
      );
      nixosModules = {
        agent = import ./nix/modules/agent.nix self;
        hub = import ./nix/modules/hub.nix self;
        default = {
          imports = [
            self.nixosModules.agent
            self.nixosModules.hub
          ];
        };
      };
    };
}
