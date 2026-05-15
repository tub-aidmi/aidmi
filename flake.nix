{
  description = "AidMi development environment";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
  flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs {
        inherit system;
      };
    in
    {
      devShell = pkgs.mkShell {
        buildInputs = with pkgs; [
          nodejs_24
          git
          docker-compose
          podman-compose
          (python3.withPackages (ps: [ ps.uv ]))
        ];
      };
    });
}
