{
  description = "IPFIXCol2 Devel flake";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
  inputs.systems.url = "github:nix-systems/default";
  inputs.netmonpkgs.url = "github:jaroslavpesek/netmonpkgs";
  inputs.flake-utils = {
    url = "github:numtide/flake-utils";
    inputs.systems.follows = "systems";
  };

  outputs =
    { nixpkgs, flake-utils, netmonpkgs, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages.default = pkgs.callPackage ./package.nix {
          libfds = netmonpkgs.packages.${system}.libfds;
          nemea-framework = netmonpkgs.packages.${system}.nemea-framework;
        };

        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.bashInteractive
            pkgs.nixd
            pkgs.nixpkgs-fmt
            pkgs.cmake
            pkgs.pkg-config
            pkgs.gcc
            pkgs.docutils
            pkgs.libxml2
            pkgs.zlib
            pkgs.rdkafka
            pkgs.lz4
            pkgs.protobuf
            pkgs.xxHash
            netmonpkgs.packages.${system}.libfds
            netmonpkgs.packages.${system}.nemea-framework
          ];
        
        shellHook = ''
          echo "Welcome to IPFIXCol2 development environment."
        '';
        };
      }
    );
}