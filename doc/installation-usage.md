# Installation and Usage

By default, JULEA is built as a debug version that can be used for development and debugging purposes.
To build and install a release version, some additional arguments and variables are necessary.

First of all, JULEA's environment has to be loaded with the environment variable `JULEA_PREFIX`:

```console
$ export JULEA_PREFIX="${HOME}/julea-install"
$ . scripts/environment.sh
```

Afterwards, JULEA has to be built and installed in release mode:

```console
$ meson setup --prefix="${HOME}/julea-install" --buildtype=release bld-release
$ ninja -C bld-release
$ ninja -C bld-release install
```

Finally, you have to create a [configuration](configuration.md) if you do not already have one.

## Installation via Spack

Alternatively, you can install JULEA using Spack, which will install JULEA and all of its dependencies:

```console
$ git clone https://github.com/spack/spack.git
$ . spack/share/spack/setup-env.sh
$ spack install julea
$ spack load julea
```

The second and fourth steps will have to be repeated every time you want to use JULEA.
Since `spack load` will take care of setting up the environment, you do not need to use JULEA's environment script in this case.
