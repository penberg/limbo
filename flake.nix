{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, flake-utils, rust-overlay, crane, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        toolchain = break ((pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml).override {
          extensions = [ "rust-analyzer" "rust-src" ];
          targets = [ "wasm32-unknown-unknown" ];
        });

        lib = pkgs.lib;

        cargoArtifacts = craneLib.buildDepsOnly {
          src = ./.;
          pname = "limbo";
          stritcDeps = true;
          nativeBuildInputs = with pkgs; [ python3 ];
        };

        commonArgs = {
          inherit cargoArtifacts;
          pname = "limbo";
          src = ./.;
          nativeBuildInputs = with pkgs; [ python3 ];
          strictDeps = true;
        };

        craneLib = ((crane.mkLib pkgs).overrideToolchain toolchain);
      in
      rec {
        formatter = pkgs.nixpkgs-fmt;
        checks = {
          doc = craneLib.cargoDoc commonArgs;
          fmt = craneLib.cargoFmt commonArgs;
          clippy = craneLib.cargoClippy (commonArgs // {
            # TODO: maybe add `-- --deny warnings`
            cargoClippyExtraArgs = "--all-targets";
          });
        };
        packages.limbo = craneLib.buildPackage (commonArgs // {
          cargoExtraArgs = "--bin limbo";
        });
        packages.default = packages.limbo;
        devShells.default = with pkgs; mkShell {
          nativeBuildInputs = [
            clang
            sqlite
            gnumake
            tcl
            python3
            nodejs
            toolchain
          ] ++ lib.optionals pkgs.stdenv.isDarwin [
            apple-sdk
          ];
        };
      }
    );
}
