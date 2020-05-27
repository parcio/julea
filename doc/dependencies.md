# Dependencies

JULEA's dependencies can either be installed using your operating system's package manage or with JULEA's `install-dependencies.sh` script.

## Automatic Installation

The `install-dependencies.sh` script installs all dependencies into the `dependencies` subdirectory using [Spack](https://spack.io/).

```console
$ ./scripts/install-dependencies.sh
```

By default, the script only installs a minimal set of packages that are necessary for JULEA's operation.
If you want to install additional dependencies for optional backends or clients, specify a mode (`minimal`, `standard` or `full`):

```console
$ ./scripts/install-dependencies.sh full
```

### Additional Dependencies

After JULEA's environment has been loaded, the `spack` command can also be used to install and load additional dependencies:

```console
$ . scripts/environment.sh
$ spack install py-gcovr
$ spack load py-gcovr
```

## Manual Installation

Alternatively, you can install the dependencies manually.
Most of them should be available within the repositories of Linux distributions.

### Required Dependencies

- Meson and Ninja
  - Debian: `apt install meson ninja-build`
  - Fedora: `dnf install meson ninja-build`
  - Arch Linux: `pacman -S meson ninja`

- GLib
  - Debian: `apt install libglib2.0-dev`
  - Fedora: `dnf install glib2-devel`
  - Arch Linux: `pacman -S glib2`

- libbson
  - Debian: `apt install libbson-dev`
  - Fedora: `dnf install libbson-devel`
  - Arch Linux: `pacman -S libbson`

### Optional Dependencies

- FUSE
  - Debian: `apt install libfuse-dev`
  - Fedora: `dnf install fuse-devel`
  - Arch Linux: `pacman -S fuse2`

- LevelDB
  - Debian: `apt install libleveldb-dev`
  - Fedora: `dnf install leveldb-devel`
  - Arch Linux: `pacman -S leveldb`

- libmongoc
  - Debian: `apt install libmongoc-dev`
  - Fedora: `dnf install mongo-c-driver-devel`
  - Arch Linux: `pacman -S libmongoc`

- librados
  - Debian: `apt install librados-dev`
  - Fedora: `dnf install librados-devel`
  - Arch Linux: `pacman -S ceph-libs`

- LMDB
  - Debian: `apt install liblmdb-dev`
  - Fedora: `dnf install lmdb-devel`
  - Arch Linux: `pacman -S lmdb`

- MariaDB
  - Debian: `apt install libmariadb-dev`
  - Fedora: `dnf install mariadb-connector-c-devel`
  - Arch Linux: `pacman -S mariadb-libs`

- RocksDB
  - Debian: `apt install librocksdb-dev`
  - Arch Linux: `pacman -S rocksdb`

- SQLite
  - Debian: `apt install libsqlite3-dev`
  - Fedora: `dnf install sqlite-devel`
  - Arch Linux: `pacman -S sqlite`
