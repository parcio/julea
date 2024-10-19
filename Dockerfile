ARG BASE_IMAGE=ubuntu:24.04
FROM $BASE_IMAGE
LABEL authors="Finn Hering <finn.hering@st.ovgu.de>"

WORKDIR /app
COPY . /app

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update && apt-get -y upgrade
RUN apt-get -y install \
    git \
    python3 \
    meson \
    ninja-build \
    libglib2.0-dev \
    libbson-dev \
    libfabric-dev \
    libfuse3-dev \
    libgdbm-dev \
    libleveldb-dev \
    libmongoc-dev \
    librados-dev \
    liblmdb-dev \
    libmariadb-dev \
    librocksdb-dev \
    libsqlite3-dev

RUN meson setup --prefix="${HOME}/julea-install" -Db_sanitize=address,undefined bld
RUN ninja -C bld

# Copy all created executables to /usr/bin
RUN find ./bld -maxdepth 1 ! -name '*.*' -type f -exec cp {} /usr/bin ";"