{
  inputs = {
    nixpkgs = {
      url = "github:nixos/nixpkgs";
    };
  };
  outputs = {nixpkgs, ...}: let
    systems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
    forAllSystems = nixpkgs.lib.genAttrs systems;
  in {
    devShells = forAllSystems (
      system: let
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        default = with pkgs;
          mkShell {
            buildInputs = [
              clang
              libiconv
              sqlite
              rustup
              gnumake
            ];
            shellHook = ''
              rustup default stable
            '';
          };
      }
    );
  };
}
