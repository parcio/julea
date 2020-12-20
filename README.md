# JULEA

[![Format](https://github.com/wr-hamburg/julea/workflows/Format/badge.svg)](https://github.com/wr-hamburg/julea/actions)
[![Build](https://github.com/wr-hamburg/julea/workflows/Build/badge.svg)](https://github.com/wr-hamburg/julea/actions)
[![Tests](https://github.com/wr-hamburg/julea/workflows/Tests/badge.svg)](https://github.com/wr-hamburg/julea/actions)

JULEA is a flexible storage framework that allows offering arbitrary I/O interfaces to applications.
To be able to rapidly prototype new approaches, it offers object, key-value and database backends.
Support for popular storage technologies such as POSIX, LevelDB and MongoDB is already included.

Additionally, JULEA allows dynamically adapting the I/O operations' semantics and can thus be adjusted to different use-cases.
It runs completely in user space, which eases development and debugging.
Its goal is to provide a solid foundation for storage research and teaching.

For more information, please refer to the [documentation](doc/README.md).

## Quick Start

To use JULEA, first clone the Git repository and enter the directory.

```console
$ git clone https://github.com/wr-hamburg/julea.git
$ cd julea
```

JULEA has two mandatory dependencies (GLib and libbson) and several optional ones that enable additional functionality.
The dependencies can either be installed using [your operating system's package manager](doc/dependencies.md#manual-installation) or with JULEA's `install-dependencies` script that installs them into the `dependencies` subdirectory using [Spack](https://spack.io/).
By default, the script will install a useful subset of dependencies;
if you want to install all of them, call it with the `full` argument.

```console
$ ./scripts/install-dependencies.sh
```

To allow the dependencies to be found, the JULEA environment has to be loaded.
This also ensures that JULEA's binaries and libraries are found later.
Make sure to load the script using `. ` instead of executing it with `./` because the environment changes will not persist otherwise.

```console
$ . scripts/environment.sh
```

JULEA now has to be configured using [Meson](https://mesonbuild.com/) and compiled using [Ninja](https://ninja-build.org/);
the different configuration and build options can be shown with `meson setup --help`.

```console
$ meson setup --prefix="${HOME}/julea-install" -Db_sanitize=address,undefined bld
$ ninja -C bld
```

Finally, a JULEA configuration has to be created.

```console
$ julea-config --user \
  --object-servers="$(hostname)" --kv-servers="$(hostname)" --db-servers="$(hostname)" \
  --object-backend=posix --object-component=server --object-path="/tmp/julea-$(id -u)/posix" \
  --kv-backend=lmdb --kv-component=server --kv-path="/tmp/julea-$(id -u)/lmdb" \
  --db-backend=sqlite --db-component=server --db-path="/tmp/julea-$(id -u)/sqlite"
```

You can check whether JULEA works by executing the integrated test suite.

```console
$ ./scripts/test.sh
```

To get an idea about how to use JULEA from your own application, check out the `example` directory.

```console
$ cd example
$ make run
```

The version is JULEA built using this guide is mainly meant for development and debugging.
If you want to deploy a release version of JULEA, please refer to the documentation about [installation and usage](doc/installation-usage.md).

## Citing JULEA

If you want to cite JULEA, please use the following paper:

- [JULEA: A Flexible Storage Framework for HPC](https://link.springer.com/chapter/10.1007/978-3-319-67630-2_51) (Michael Kuhn), In High Performance Computing, Lecture Notes in Computer Science (10524), (Editors: Julian Kunkel, Rio Yokota, Michela Taufer, John Shalf), Springer International Publishing, ISC High Performance 2017, Frankfurt, Germany, ISBN: 978-3-319-67629-6, 2017-11
