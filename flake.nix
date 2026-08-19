{
  description = "UpGrid";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    nixpkgs,
    rust-overlay,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        inherit (pkgs) lib;
        rustToolchain = pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.minimal);
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };
        source = pkgs.nix-gitignore.gitignoreSourcePure [./.gitignore] ./.;
        upgrid = rustPlatform.buildRustPackage {
          pname = "upgrid";
          version = "0.2.0";
          src = source;
          cargoBuildFlags = ["-p" "upgrid"];
          buildAndTestSubdir = ".";
          nativeBuildInputs = [pkgs.pkg-config];
          buildInputs = [pkgs.sqlite];
          cargoLock = {
            lockFile = ./Cargo.lock;
            outputHashes = {
              "openraft-0.10.0-alpha.33" = "sha256-IhUCyRg+MvR4UBvD1UphmzSdG7HBhcyHJw9L1eNuyKI=";
              "tarpc-0.36.0" = "sha256-RV0LUj0+5DZ2Qa1JsW+BSkA+mAJIvgWJEuPoNelfJ5I=";
            };
          };
          doCheck = false;
          meta.mainProgram = "upgrid";
        };
        runtimeRoot = pkgs.runCommand "upgrid-runtime-root" {} ''
          install -D -m 0755 ${upgrid}/bin/upgrid $out/usr/local/bin/upgrid
        '';
        container = pkgs.dockerTools.buildLayeredImage {
          name = "upgrid";
          tag = "nix";
          contents = [
            runtimeRoot
            pkgs.dockerTools.caCertificates
          ];
          extraCommands = ''
            mkdir -p etc var/lib/upgrid
            printf 'upgrid:x:10001:10001:UpGrid:/var/lib/upgrid:/sbin/nologin\n' > etc/passwd
            printf 'upgrid:x:10001:\n' > etc/group
          '';
          fakeRootCommands = ''
            chown 10001:10001 var/lib/upgrid
          '';
          config = {
            Entrypoint = ["/usr/local/bin/upgrid"];
            Env = [
              "UPGRID_BIND=0.0.0.0:8080"
              "UPGRID_DATA_DIR=/var/lib/upgrid"
            ];
            ExposedPorts = {
              "8080/tcp" = {};
              "11451/udp" = {};
            };
            User = "10001:10001";
            Volumes = {
              "/var/lib/upgrid" = {};
            };
          };
        };
      in {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs.cargo-feature
            pkgs.curl
            pkgs.jq
            pkgs.nodejs_22
            pkgs.openssl
            pkgs.pkg-config
            pkgs.pnpm
            pkgs.ripgrep
            pkgs.sqlite
            pkgs.lldb_21
            (pkgs.rust-bin.selectLatestNightlyWith (toolchain:
              toolchain.default.override {
                extensions = ["rust-src"];
              }))
          ];
        };

        packages =
          {
            default = upgrid;
            inherit upgrid;
          }
          // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
            inherit container;
          };
      }
    );
}
