FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
	build-essential \
	curl \
	unzip \
	git \
	python3

WORKDIR /julea
COPY . .

SHELL ["/bin/bash", "-c"]

RUN ./scripts/install-dependencies.sh
RUN . scripts/environment.sh && \
	meson setup --prefix="${HOME}/julea-install" -Db_sanitize=address,undefined bld && \
	ninja -C bld
