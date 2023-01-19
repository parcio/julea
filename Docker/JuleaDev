FROM ubuntu:20.04
SHELL ["/bin/bash", "-c"]

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC  apt-get install --yes --no-install-recommends ninja-build pkgconf git python-dev python3-dev build-essential python3-pip
RUN apt-get --yes install --reinstall ca-certificates
# RUN git clone --depth 1 -c feature.manyFiles=true https://github.com/spack/spack.git

WORKDIR /julea
ENV JULEA_SPACK_COMPILER=gcc@9.4.0
ENV CC=gcc
ENV CI=1
ENV SPACK_DIR=/dependencies
COPY scripts scripts
RUN ./scripts/install-dependencies.sh 
