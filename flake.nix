{
   description = "A Flexible Storage Framework for HPC";

   # Flake inputs
   inputs = {
     nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0";
     flake-utils.url = "github:numtide/flake-utils";
   };

   # Flake outputs
   outputs = { self, nixpkgs, flake-utils }:
     flake-utils.lib.eachDefaultSystem (system:
       let
         # Import nixpkgs for the specific system with our config
         pkgs = import nixpkgs {
           inherit system;
           config.allowUnfree = true;
           overlays = [ (import ./libfabric_overlay.nix) ];
         };
       in
       {
         # Development environments output by this flake
         devShells.default = pkgs.mkShell {
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

             # TODO: Is this the correct way to include rados?
             ceph

             # TODO: I think we need otf1 instead - but not nixpkgs exists
             # otf2
             rocksdb
           ];

           # Most hardening flags are enabled by default in nix.
           # The only one usually disabled is pie.
           # For more details check here:
           # https://ryantm.github.io/nixpkgs/stdenv/stdenv/#sec-hardening-in-nixpkgs
           hardeningEnable = [ "pie" ];

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
       }
     );
 }
