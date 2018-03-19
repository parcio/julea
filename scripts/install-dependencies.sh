#!/bin/sh

# JULEA - Flexible storage framework
# Copyright (C) 2017 Michael Kuhn
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

set -e

SELF_PATH="$(readlink --canonicalize-existing -- "$0")"
SELF_DIR="${SELF_PATH%/*}"
SELF_BASE="${SELF_PATH##*/}"

. "${SELF_DIR}/common"

spack_clone ()
{
	local spack_commit
	local spack_dir

	spack_commit='e9c66a76f00057f9900e8678765c474dc2a79823'
	spack_dir="$(get_directory "${SELF_DIR}/..")/dependencies"

	if test ! -d "${spack_dir}"
	then
		git clone https://github.com/LLNL/spack.git "${spack_dir}"
	fi

	cd "${spack_dir}"

	git fetch

	git reset --hard "${spack_commit}"
}

spack_install ()
{
	local spack_dir
	local spack_pkg

	spack_dir="$(get_directory "${SELF_DIR}/../dependencies")"
	spack_pkg="$1"

	test -n "${spack_dir}" || return 1
	test -d "${spack_dir}" || return 1
	test -n "${spack_pkg}" || return 1

	./bin/spack install "${spack_pkg}"
}

install_dependency ()
{
	local pkg_config
	local spack_pkg

	pkg_config="$1"
	spack_pkg="$2"

	test -n "${pkg_config}" || return 1
	test -n "${spack_pkg}" || return 1

	if ! pkg-config --exists "${pkg_config}"
	then
		spack_install "${spack_pkg}"
	fi
}

install_dependency_bin ()
{
	local bin
	local spack_pkg

	bin="$1"
	spack_pkg="$2"

	test -n "${bin}" || return 1
	test -n "${spack_pkg}" || return 1

	if ! command -v "${bin}" > /dev/null 2>&1
	then
		spack_install "${spack_pkg}"
	fi
}

spack_clone

# Required for Spack itself
spack_install environment-modules

# Mandatory dependencies
install_dependency glib-2.0 glib
install_dependency libbson-1.0 libbson

# Optional dependencies
install_dependency leveldb leveldb
install_dependency lmdb lmdb
install_dependency libmongoc-1.0 libmongoc
install_dependency sqlite3 sqlite

#install_dependency_bin otfconfig otf
