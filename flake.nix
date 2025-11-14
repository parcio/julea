{
  description = "A Flexible Storage Framework for HPC";

  # Flake inputs
  inputs.nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0"; # Stable Nixpkgs (use 0.1 for unstable)

  # Flake outputs
  outputs =
    { self, ... }@inputs:
    let
      # The systems supported for this flake's outputs
      supportedSystems = [
        "x86_64-linux" # 64-bit Intel/AMD Linux
        "aarch64-linux" # 64-bit ARM Linux
        # "x86_64-darwin" # 64-bit Intel macOS - lets not support mac for now
        # "aarch64-darwin" # 64-bit ARM macOS - same as above
      ];

      # Helper for providing system-specific attributes
      forEachSupportedSystem =
        f:
        inputs.nixpkgs.lib.genAttrs supportedSystems (
          system:
          f {
            inherit system;
            # Provides a system-specific, configured Nixpkgs
            pkgs = import inputs.nixpkgs {
              inherit system;
              # Enable using unfree packages
              config.allowUnfree = true;
              # Load the overlay.
              overlays = [ (import ./libfabric_overlay.nix) ];
            };
          }
        );
    in
    {
      # Development environments output by this flake

      # To activate the default environment:
      # nix develop
      # Or if you use direnv:
      # direnv allow
      devShells = forEachSupportedSystem (
        { pkgs, system }:
        {
          # Run `nix develop` to activate this environment or `direnv allow` if you have direnv installed
          default = pkgs.mkShell {
            # The Nix packages provided in the environment
            packages = with pkgs; [
              # Add the flake's formatter to your project's environment
              self.formatter.${system}

              # Build basics
              gcc
              ninja
              meson
              pkg-config

              # Other packages
              glib
              libbson
              libfabric

              hdf5
              fuse3
              lmdb
              sqlite
              leveldb
              mariadb
              rocksdb
            ];

            # Set any environment variables for your development environment
            env = { };

            # Add any shell logic you want executed when the environment is activated
            shellHook = "echo \"Hello where is ponsay\"";
          };
        }
      );

      # Nix formatter

      # This applies the formatter that follows RFC 166, which defines a standard format:
      # https://github.com/NixOS/rfcs/pull/166

      # To format all Nix files:
      # git ls-files -z '*.nix' | xargs -0 -r nix fmt
      # To check formatting:
      # git ls-files -z '*.nix' | xargs -0 -r nix develop --command nixfmt --check
      formatter = forEachSupportedSystem ({ pkgs, ... }: pkgs.nixfmt-rfc-style);
    };
}
