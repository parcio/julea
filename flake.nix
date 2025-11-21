{
  description = "A Flexible Storage Framework for HPC";

  # Flake inputs
  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0";
    flake-utils.url = "github:numtide/flake-utils";
  };

  # Flake outputs
  # Useful documentation: https://saylesss88.github.io/flakes/flake_outputs_4.2.html
  outputs = {
    self,
    nixpkgs,
    flake-utils,
  }: let
    # This is a function the returns all the needed dependencies given the system pkgs.
    dependencies = pkgs:
      with pkgs; [
        # Build basics
        # TODO: Currently the ci/cd pipelines tests for clang and gcc. Is gcc only ok?
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

        # TODO: Is this the correct way to include rados?
        ceph

        # TODO: I think we need otf1 instead - but no nixpkgs exists
        # otf2
        rocksdb
      ];

    # Function that gets pkgs and results in julea being build
    mkJulea = pkgs:
      pkgs.stdenv.mkDerivation {
        pname = "julea";
        # TODO: Do we have a versioning scheme?
        version = "1.0.0";
        src = self;

        mesonBuildType = "release";

        buildInputs = dependencies pkgs;
      };
  in
    flake-utils.lib.eachDefaultSystem (
      system: let
        # Import nixpkgs for the specific system with our config
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
          overlays = [(import ./libfabric_overlay.nix)];
        };
      in {
        # Development environments output by this flake
        devShells.default = pkgs.mkShell {
          # The Nix packages provided in the environment
          # Consists of the usual build dependencies and some dev dependencies.
          packages = (dependencies pkgs) ++ [
            # Needed to run coverage tests.
            pkgs.gcovr
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
          shellHook = ''
            echo "Welcome to the JULEA nix shell:"

            export BUILD_DIR="$PWD/bld"
            export PATH="$PATH:$BUILD_DIR"
            export LD_LIBRARY_PATH="$BUILD_DIR"
            export PKG_CONFIG_PATH="$PKG_CONFIG_PATH:$BUILD_DIR/meson-uninstalled/"
            export JULEA_BACKEND_PATH="$BUILD_DIR"
            export HDF5_PLUGIN_PATH="$BUILD_DIR"
          '';
        };

        # Can be called with `nix build`.
        packages.default = mkJulea pkgs;
      }
    );
}
