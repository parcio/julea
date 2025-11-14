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
        "x86_64-darwin" # 64-bit Intel macOS - lets not support mac for now
        "aarch64-darwin" # 64-bit ARM macOS - same as above
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
              # This uses the libfabric-overlay!
              libfabric

              hdf5
              fuse3
              lmdb
              sqlite

              gdbm
              leveldb
              # TODO: This is not the correct dependency.
              # We need a connector from here: https://downloads.mariadb.com/Connectors/c
              # As far as i can tell no nixpkgs exists as of now.
              # mariadb
              ceph

              # TODO: I think we need otf1 instead - but not nixpkgs exists
              # otf2
              rocksdb

            ];

            # Most hardening flags are enabled by default in nix.
            # The only one usually disabled is pie.
            # For more details check here:
            # https://ryantm.github.io/nixpkgs/stdenv/stdenv/#sec-hardening-in-nixpkgs
            hardeningEnable = ["pie"];

            # Set any environment variables for your development environment
            env = {
            };

            # Add any shell logic you want executed when the environment is activated
            # TODO: Is LD_LIBRARY_PATH ok like this? The spack version had a lot more stuff in it.
            shellHook = let buildDir = "${self}/bld"; in ''
            echo Welcome to the JULEA nix shell:

            export PATH="$PATH:${buildDir}"
            export LD_LIBRARY_PATH="${buildDir}"
            export PKG_CONFIG_PATH="$PKG_CONFIG_PATH:${buildDir}/meson-uninstalled/"
            export JULEA_BACKEND_PATH="${buildDir}"
            export HDF5_PLUGIN_PATH="${buildDir}"
            '';
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
