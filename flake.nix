{
  description = "Tracen development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix.url = "github:nix-community/fenix";
  };

  outputs = { nixpkgs, flake-utils, fenix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        fenixPkgs = fenix.packages.${system};
        rustToolchain = fenixPkgs.combine [
          fenixPkgs.stable.rustc
          fenixPkgs.stable.cargo
          fenixPkgs.stable.clippy
          fenixPkgs.stable.rustfmt
          fenixPkgs.stable.rust-src
        ];
      in {
        devShells.default = pkgs.mkShell {
          name = "tracen-shell";

          packages = with pkgs; [
            rustToolchain
            fenixPkgs.latest.rust-analyzer
            cargo-audit
            git
            just
            jq
            pkg-config
            ripgrep
          ];

          shellHook = ''
            export PATH=${rustToolchain}/bin:$PATH
            echo "Loaded tracen development shell"
          '';
        };
      });
}
