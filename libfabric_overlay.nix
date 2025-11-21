# We currently install libfabric like this:	dependencies="${dependencies} libfabric#fabrics=sockets,tcp,udp,verbs,rxd,rxm"
# All these fabric flags gets parsed in the libfabric python file linked below.
# They define a list of fabrics and, if it was passed as an argument, append the --enable-{fabric} flag in line 249.
# https://github.com/spack/spack-packages/blob/develop/repos/spack_repo/builtin/packages/libfabric/package.py
# Sadly the current libfabric nix file:
# https://github.com/NixOS/nixpkgs/blob/nixpkgs-25.05-darwin/pkgs/by-name/li/libfabric/package.nix
# does not support these flags. It only exposes flags for psm2 and opx.
# In the future it would probably be best to submit a merge request to nixpkgs.
# For now i will attempt to add them myself here.

final : prev : {
    libfabric = prev.libfabric.overrideAttrs (old: {
        # We append our own flags to the existing ones. These should then get passed to the ./configure script.
        configureFlags = old.configureFlags
            ++ [
                "-enable-sockets"
                "-enable-tcp"
                "-enable-udp"
                "-enable-verbs"
                "-enable-rxd"
                "-enable-rxm"
            ];

        # verbs also requires rdma so we add that dependency
        buildInputs = old.buildInputs
        ++ [ prev.rdma-core ];
    });
}
