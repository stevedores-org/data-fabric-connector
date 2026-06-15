{
  description = "dfc — data-fabric connector (workspace + OCI image)";

  nixConfig = {
    extra-substituters = [ "https://nix-cache.stevedores.org/" ];
    extra-trusted-public-keys = [
      "stevedores-1:ZEtb+wHYNR/LDmMDhF3/EpRZDNma8exY2b1TGZ6uS2A="
    ];
  };

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, crane, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rustfmt" "clippy" "rust-src" ];
        };

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        src = craneLib.cleanCargoSource ./.;

        cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
        pkgVersion = cargoToml.workspace.package.version;

        commonArgs = {
          inherit src;
          strictDeps = true;
          pname = "dfc-workspace";
          version = pkgVersion;
          buildInputs = pkgs.lib.optionals pkgs.stdenv.isDarwin [ pkgs.libiconv ];
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        dfc-server = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          pname = "dfc-server";
          cargoExtraArgs = "-p dfc-server --bin dfc-server";
        });

        workspaceClippy = craneLib.cargoClippy (commonArgs // {
          inherit cargoArtifacts;
          cargoClippyExtraArgs = "--workspace --all-targets -- -D warnings";
        });

        workspaceTests = craneLib.cargoTest (commonArgs // {
          inherit cargoArtifacts;
          cargoTestExtraArgs = "--workspace";
        });

        workspaceFmt = craneLib.cargoFmt { inherit src; };

        # OCI image — Linux-only. CI builds on a Linux runner; dockworker ships
        # the resulting docker-archive tarball to the registry via skopeo.
        image = pkgs.dockerTools.buildLayeredImage {
          name = "dfc";
          tag = pkgVersion;
          maxLayers = 50;
          contents = [
            dfc-server
            pkgs.cacert
          ];
          config = {
            Entrypoint = [ "${dfc-server}/bin/dfc-server" ];
            ExposedPorts = { "8080/tcp" = {}; };
            Env = [
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              "RUST_LOG=info"
              "DFC_HOST=0.0.0.0"
              "DFC_PORT=8080"
            ];
            Labels = {
              "org.opencontainers.image.title" = "dfc";
              "org.opencontainers.image.description" =
                "Stateless anti-corruption layer between AIVCS, HITL, and data-fabric";
              "org.opencontainers.image.source" =
                "https://github.com/stevedores-org/data-fabric-connector";
              "org.opencontainers.image.licenses" = "Apache-2.0";
              "stevedores.org/managed-by" = "dockworker";
              "stevedores.org/fqdn" = "dfc.aivcs.io";
            };
          };
        };

      in {
        checks = {
          inherit dfc-server workspaceClippy workspaceTests workspaceFmt;
        };

        packages = {
          default = dfc-server;
          inherit dfc-server;
        } // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          inherit image;
          dfc-image = image;
        };

        devShells.default = craneLib.devShell {
          checks = self.checks.${system};

          packages = with pkgs; [
            cargo-watch
            kubectl
            kustomize
            skopeo
          ];
        };
      });
}
