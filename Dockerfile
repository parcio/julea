FROM ghcr.io/vittel1/julea-deps-standard:0.0.1

WORKDIR /julea

SHELL ["/bin/bash", "-c"]

RUN . scripts/environment.sh && \
	meson setup --prefix="${HOME}/julea-install" -Db_sanitize=address,undefined bld && \
	ninja -C bld
