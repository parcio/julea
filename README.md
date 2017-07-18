# JULEA

JULEA is a flexible storage framework that allows offering arbitrary client interfaces to applications.
To be able to rapidly prototype new approaches, it offers data and metadata backends that can either be client-side or server-side;
backends for popular storage technologies such as POSIX, LevelDB and MongoDB have already been implemented.
Additionally, JULEA allows dynamically adapting the I/O operations' semantics and can thus be adjusted to different use-cases.
It runs completely in user space, which eases development and debugging.
Its goal is to provide a solid foundation for storage research and teaching.

## Quick Start

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

```
$ ./waf configure --debug --sanitize
$ ./waf
```

To be able to use JULEA, its environment has to be loaded.

```
$ . ./scripts/environment.sh
```

Finally, a JULEA configuration has to be created.

```
$ julea-config --user \
  --data-servers="$(hostname)" --metadata-servers="$(hostname)" \
  --data-backend=posix --data-component=server --data-path=/tmp/julea \
  --metadata-backend=leveldb --metadata-component=server --metadata-path=/tmp/julea
```

You can check whether JULEA works by executing the integrated test suite.

```
$ ./scripts/test.sh
```
