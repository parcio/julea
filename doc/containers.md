# Containers

For easier setup or testing JULEA, you can use the development container, which contains JULEA's required dependencies.

## Development Container

Setting up and using the container looks like the following:

```console
$ docker pull ghcr.io/julea/ubuntu-dev

$ git clone https://github.com/julea-io/julea.git

$ docker run -v $PWD/julea:/julea -it ghcr.io/julea/ubuntu-dev
```

Continue with the following commands inside the container:

```console
$ . scripts/environment.sh

$ meson setup --prefix="/julea/install" -Db_sanitize=address,undefined bld
$ ninja -C bld

$ julea-config --user \
  --object-servers="$(hostname)" --kv-servers="$(hostname)" --db-servers="$(hostname)" \
  --object-backend=posix --object-component=server --object-path="/tmp/julea-$(id -u)/posix" \
  --kv-backend=lmdb --kv-component=server --kv-path="/tmp/julea-$(id -u)/lmdb" \
  --db-backend=sqlite --db-component=server --db-path="/tmp/julea-$(id -u)/sqlite"

$ ./scripts/setup.sh start
$ ./scripts/test.sh
$ ./scripts/setup.sh stop
```

### Building Container

The Dockerfile can be found at `containers/ubuntu-22.04-dev`.
To build the container, use the following commands:

```console
$ cd julea/containers
$ docker build -f ubuntu-22.04-dev -t julea/ubuntu-dev:22.04 .
```
