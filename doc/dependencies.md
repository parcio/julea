# Dependencies

## Automatic installation

With [[Spack|https://github.com/spack/spack]] it is possible, to load all needed dependencies automatically on a configured node. Just execute our script.

``` {.shell}
./scripts/install-dependencies.sh
```


## Manual installation

Otherwise, you could install them manually. Most of them should be available prepackaged in repositories of your favourite Linux distribution.


### Required dependencies

*   **GLib 2.0**  
    Debian: `apt install libglib2.0-dev`  
    Fedora: `dnf install glib2-devel`  
    Arch Linux: `pacman -S glib2`

*   **BSON**  
    Debian: `apt install libbson-dev`  
    Fedora: `dnf install libbson-devel`  
    Arch Linux: `pacman -S libbson`


### Optional dependencies

Depending on the used backend services of *JULEA* you have to install several extra packages. They are optional, but it is reasonable to use some of them.

* **LevelDB**  
    Debian: `apt install libleveldb-dev`  
    Fedora: `dnf install leveldb-devel`  
    Arch Linux: `pacman -S leveldb`

* **LMDB**  
    Debian: `apt install liblmdb-dev`  
    Fedora: `dnf install lmdb-devel`  
    Arch Linux: `pacman -S lmdb`

* **MongoDB C**  
    Debian: `apt install libmongoc-dev`  
    Fedora: `dnf install mongo-c-driver-devel`  
    Arch Linux: `pacman -S libmongoc`

* **SQLite 3**  
    Debian: `apt install libsqlite3-dev`  
    Fedora: `dnf install sqlite-devel`  
    Arch Linux: `pacman -S sqlite`

* **RADOS**  
    Debian: `apt install librados-dev`  
    Fedora: `dnf install librados-devel`  
    Arch Linux: `pacman -S ceph-libs`
