# Installation and Usage

By default, JULEA is built as a debug version that can be used for development and debugging purposes.
To build and install a release version, some additional arguments and variables are necessary.

First of all, you will have to make sure that JULEA's [dependencies](dependencies.md) have been installed.
Afterwards, JULEA's environment has to be loaded with the environment variable `JULEA_PREFIX`:

```console
$ export JULEA_PREFIX="${HOME}/julea-install"
$ . scripts/environment.sh
```

You can then build and install JULEA in release mode:

```console
$ meson setup --prefix="${HOME}/julea-install" --buildtype=release bld-release
$ ninja -C bld-release
$ ninja -C bld-release install
```

Finally, you have to create a [configuration](configuration.md) if you do not have one already.

## Benchmarks

JULEA provides a script that executes benchmarks to measure the performance of JULEA's different components.

```console
$ ./scripts/setup.sh start
$ ./scripts/benchmark.sh
$ ./scripts/setup.sh stop
```

By default, the individual benchmarks will run for approximately one second.
This can be changed using the `-d` or `--duration` option.
Additional options can be shown using `-h` or `--help`.

```console
$ ./scripts/benchmark.sh -d 60
```

## Using Specific Compilers

JULEA and its dependencies can also be built using specific compilers instead of the default ones.
The `install-dependencies.sh` script supports a `JULEA_SPACK_COMPILER` variable that can be used to set the compiler:

```console
$ export JULEA_SPACK_COMPILER=llvm
$ ./scripts/install-dependencies.sh
```

The compiler to use for JULEA can be configured using Meson by setting the `CC` variable:

```console
$ export CC=clang
$ meson setup --prefix="${HOME}/julea-install" bld
```

It is important to note that the `JULEA_SPACK_COMPILER` variable refers to the compiler name used within Spack, while the `CC` variable refers to the compiler binary that should be used.

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
