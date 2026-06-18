Bootstrap: library
From: ubuntu:20.04

%files
	. /julea

%post
	apt-get update && apt-get install -y \
	build-essential \
	curl \
	unzip \
	git \
	python3

	cd /julea
	./scripts/install-dependencies.sh
	. scripts/environment.sh
	meson setup --prefix="${HOME}/julea-install" -Db_sanitize=address,undefined bld
	ninja -C bld
