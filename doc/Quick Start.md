# Quick Start

To use JULEA, first clone the Git repository and enter the directory.

```
$ git clone https://github.com/wr-hamburg/julea.git
$ cd julea
```

JULEA has two mandatory dependencies (GLib and libbson) and several optional ones that enable additional functionality.
The dependencies can either be installed using your operating system's package manage or with JULEA's script that installs them into the `dependencies` subdirectory using [Spack](https://spack.io/).

```
$ ./scripts/install-dependencies.sh
```

After all dependencies have been installed, JULEA has to be configured and compiled using [Waf](https://waf.io/);
the different configuration and build options can be shown with `./waf --help`.
The `configure.sh` script is a wrapper around `./waf configure` that makes sure that the dependencies installed in the previous step are found by Waf.

```
$ ./configure.sh --debug --sanitize
$ ./waf
```

To allow the shell to find JULEA's binaries and to set some variables useful for debugging, the environment has to be loaded.
Alternatively, the binaries can be found in the `build` directory.

```
$ . ./scripts/environment.sh
```

Finally, a JULEA configuration has to be created.

```
$ julea-config --user \
  --object-servers="$(hostname)" --kv-servers="$(hostname)" \
  --object-backend=posix --object-component=server --object-path=/tmp/julea \
  --kv-backend=leveldb --kv-component=server --kv-path=/tmp/julea
```

You can check whether JULEA works by executing the integrated test suite.

```
$ ./scripts/test.sh
```
