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
    flake-utils.lib.eachSystem [
      "aarch64-darwin"
      "aarch64-linux"
      "x86_64-linux"
    ] (
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
          version = "0.3.0";
          src = source;
          cargoBuildFlags = ["-p" "upgrid"];
          buildAndTestSubdir = ".";
          nativeBuildInputs = [pkgs.pkg-config];
          buildInputs = [pkgs.sqlite];
          cargoLock.lockFile = ./Cargo.lock;
          doCheck = false;
          meta.mainProgram = "upgrid";
        };
        containerHealthcheck = pkgs.writeShellApplication {
          name = "upgrid-healthcheck";
          runtimeInputs = [pkgs.curl];
          text = ''
            bind="''${UPGRID_BIND:-0.0.0.0:8080}"
            port="''${bind##*:}"
            case "$bind" in
              "[::]:"*)
                host="[::1]"
                ;;
              "["*"]:"*)
                host="''${bind%:*}"
                ;;
              *)
                host="''${bind%:*}"
                if [[ "$host" == "0.0.0.0" ]]; then
                  host="127.0.0.1"
                fi
                ;;
            esac
            scheme="http"
            curl_options=(--fail --silent --show-error)
            if [[ -n "''${UPGRID_TLS_CERT:-}" ]]; then
              scheme="https"
              curl_options+=(--cacert "$UPGRID_TLS_CERT")
            fi
            if [[ -n "''${UPGRID_HEALTH_URL:-}" ]]; then
              url="$UPGRID_HEALTH_URL"
            else
              url="''${scheme}://''${host}:''${port}/healthz"
            fi
            exec curl "''${curl_options[@]}" "$url"
          '';
        };
        runtimeRoot = pkgs.runCommand "upgrid-runtime-root" {} ''
          install -D -m 0755 ${upgrid}/bin/upgrid $out/usr/local/bin/upgrid
        '';
        container = pkgs.dockerTools.buildLayeredImage {
          name = "upgrid";
          tag = "nix";
          contents = [
            runtimeRoot
            containerHealthcheck
            pkgs.dockerTools.caCertificates
            pkgs.curl
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
            Healthcheck = {
              Test = ["CMD" "/bin/upgrid-healthcheck"];
              Interval = 30 * 1000 * 1000 * 1000;
              Timeout = 5 * 1000 * 1000 * 1000;
              Retries = 3;
              StartPeriod = 5 * 1000 * 1000 * 1000;
            };
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
