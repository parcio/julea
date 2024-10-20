ARG UBUNTU_VERSION="24.04"
ARG JULEA_SPACK_COMPILER="gcc"
ARG CC="gcc"


FROM ubuntu:${UBUNTU_VERSION} AS base
SHELL [ "/bin/bash", "-c" ]
ARG UBUNTU_VERSION="24.04"
ARG JULEA_SPACK_COMPILER="gcc"
ARG CC="gcc"
#####################################################
# Julea build using system dependencies             #
#####################################################
FROM base AS julea_system

# Some scripts require bash to work properly...
RUN DEBIAN_FRONTEND=noninteractive apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get --yes install \
    bash \
    python3 \
    python3-pip \ 
    python3-setuptools \ 
    python3-wheel \
    build-essential \
    ninja-build \
    pkgconf \
    libglib2.0-dev \
    libbson-dev \
    libfabric-dev \
    libgdbm-dev \
    liblmdb-dev \
    libsqlite3-dev \
    libleveldb-dev \
    libmongoc-dev \
    libmariadb-dev \
    librocksdb-dev \
    libfuse3-dev \
    libopen-trace-format-dev \
    librados-dev \
    python3-pip \
    python3-setuptools \
    python3-wheel \
    clang 


RUN echo UBUNTU_VERSION: $UBUNTU_VERSION
# Install meson from pip if the version is less than 22.04
RUN if awk "BEGIN {exit !($UBUNTU_VERSION >= 22.04)}" ; then echo "installing with apt..." && apt-get --yes install meson ; else pip install meson ; fi


WORKDIR /app
COPY . /app/

RUN pwd
RUN CC=${CC} . /app/scripts/environment.sh && meson setup --prefix="/app/julea-install" --buildtype=release --werror bld && \
    ninja -C bld && \
    ninja -C bld install

#####################################################
# Julea build using spack                           #
#####################################################
FROM base AS spack_dependencies
WORKDIR /app

RUN DEBIAN_FRONTEND=noninteractive apt-get -y update && apt-get -y upgrade
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install \ 
    git \
    python3 \
    build-essential \
    zstd \
    ca-certificates \
    git \
    gnupg \
    patchelf \
    unzip \
    clang 

COPY ./scripts/ /app/scripts/

RUN JULEA_SPACK_COMPILER=${JULEA_SPACK_COMPILER} JULEA_SPACK_DIR="/app/dependencies" /app/scripts/install-dependencies.sh


FROM base AS julea_spack
WORKDIR /app

COPY ./ /app/
COPY --from=spack_dependencies /app/dependencies /app/dependencies

RUN DEBIAN_FRONTEND=noninteractive apt-get -y update && apt-get -y install python3 build-essential clang

RUN CC=${CC} . /app/scripts/environment.sh && \
    meson setup --prefix="/app/julea-install" --buildtype=release --werror "-Dgdbm_prefix=$(spack location --install-dir gdbm)" bld && \
    ninja -C bld && \
    ninja -C bld install 


# Copy all created executables to /usr/bin
#RUN find ./bld -maxdepth 1 ! -name '*.*' -type f -exec cp {} /usr/bin ";"