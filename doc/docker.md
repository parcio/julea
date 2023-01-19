# Docker

For easier setup or testing JULEA you can use the JULEA-DEV image.
It contains the installed spack dependencies and therfore you can spare this time.

## JULEA-DEV

The image version of the newest commit can be found HERE(TODO).

Setting up and using the image with the `docker-cli`  could like the following:

### Container Setup

```sh
# pulling the docker
docker pull https://LINK.TODO

# cloning the repo
git clone https://github.com/julea-io/julea.git
cd julea

# starting the docker
# --net=host ... (optional) using theme network as host to make everything availbale,
# can be skipped if no outside communication is needed 
# -v $PWD:/julea ... mount git repo in docker, to build your current code (and store build artifacts persistent)
# -it ... run container interactive
# --name julea-dev ... give container a name for followup use
docker run --net=host -v $PWD:/julea -- -it JULEA-DEV

# inside docker now, follow the Quilck-Start Guide at the point

# loading spack enviroment
. scripts/enviroment.sh

# first time for the project only! configure building directory
meson setup --prefix="/julea-install" -Db_sanitize=address,undefined ../bld
ninja -C ../bld
julea-config --user \
  --object-servers="$(hostname)" --kv-servers="$(hostname)" --db-servers="$(hostname)" \
  --object-backend=posix --object-component=server --object-path="/tmp/julea-$(id -u)/posix" \
  --kv-backend=lmdb --kv-component=server --kv-path="/tmp/julea-$(id -u)/lmdb" \
  --db-backend=sqlite --db-component=server --db-path="/tmp/julea-$(id -u)/sqlite"

# now you can exit the container
exit
```

### Container Usage

```sh
# start prepared container
docker start --attach julea-dev

# inside docker normal development loop

# build bins
ninja -C bld

# test code
./scripts/setup.sh start
./scripts/test.sh
./scripts/setup.sh stop
```

### Building the Image

The Dockerfile can be found at `Docker/JuleaDev`.
To build the docker go in the project root and build the file with `docker build`

Example:
```sh
git clone https://github.com/julea_io/julea.git
cd julea
docker biuld . --file Docker/JuleaDev -t JULEA-DEV
```

