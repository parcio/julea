# Dependencies

JULEA's dependencies can either be installed using your operating system's package manage or with JULEA's `install-dependencies.sh` script.

## Automatic Installation

The `install-dependencies.sh` script installs all dependencies into the `dependencies` subdirectory using [Spack](https://spack.io/).

```console
$ ./scripts/install-dependencies.sh
```

## Manual Installation

Alternatively, you can install the dependencies manually.
Most of them should be available within the repositories of Linux distributions.

### Required Dependencies

- GLib
  - Debian: `apt install libglib2.0-dev`
  - Fedora: `dnf install glib2-devel`
  - Arch Linux: `pacman -S glib2`

- libbson
  - Debian: `apt install libbson-dev`
  - Fedora: `dnf install libbson-devel`
  - Arch Linux: `pacman -S libbson`

### Optional Dependencies

- LevelDB
  - Debian: `apt install libleveldb-dev`
  - Fedora: `dnf install leveldb-devel`
  - Arch Linux: `pacman -S leveldb`

- LMDB
  - Debian: `apt install liblmdb-dev`
  - Fedora: `dnf install lmdb-devel`
  - Arch Linux: `pacman -S lmdb`

- libmongoc
  - Debian: `apt install libmongoc-dev`
  - Fedora: `dnf install mongo-c-driver-devel`
  - Arch Linux: `pacman -S libmongoc`

- SQLite
  - Debian: `apt install libsqlite3-dev`
  - Fedora: `dnf install sqlite-devel`
  - Arch Linux: `pacman -S sqlite`

- librados
  - Debian: `apt install librados-dev`
  - Fedora: `dnf install librados-devel`
  - Arch Linux: `pacman -S ceph-libs`

- FUSE
  - Debian: `apt install libfuse-dev`
  - Fedora: `dnf install fuse-devel`
  - Arch Linux: `pacman -S fuse2`

- MariaDB
  - Debian: `apt install libmariadb-dev`
  - Fedora: `dnf install mariadb-connector-c-devel`
  - Arch Linux: `pacman -S mariadb-libs`
