#!/bin/bash

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

. "${SELF_DIR}/scripts/common"

spack_init ()
{
	local spack_dir
	local spack_env

	spack_dir="$(get_directory "${SELF_DIR}/dependencies/spack")"
	spack_env="${spack_dir}/share/spack/setup-env.sh"

	test -n "${spack_dir}" || return 1
	test -d "${spack_dir}" || return 1
	test -f "${spack_env}" || return 1

	if test -f '/etc/profile.d/modules.sh'
	then
		. /etc/profile.d/modules.sh
	fi

	. "${spack_env}"
}

spack_load ()
{
	local spack_pkg

	spack_pkg="$1"

	test -n "${spack_pkg}" || return 1

	spack load --dependencies "${spack_pkg}"
}

if spack_init
then
	for pkg in glib libbson leveldb lmdb libmongoc otf
	do
		spack_load "${pkg}"
	done
fi

# Do not filter out paths contained in CPATH and LIBRARY_PATH,
# otherwise ./waf will not work without having them set.
PKG_CONFIG_ALLOW_SYSTEM_CFLAGS=1
PKG_CONFIG_ALLOW_SYSTEM_LIBS=1

export PKG_CONFIG_ALLOW_SYSTEM_CFLAGS
export PKG_CONFIG_ALLOW_SYSTEM_LIBS

./waf configure "$@"
